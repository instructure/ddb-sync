/*
 * ddb-sync
 * Copyright (C) 2018 Instructure Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package operations

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/instructure/ddb-sync/config"
	"github.com/instructure/ddb-sync/log"
	"github.com/instructure/ddb-sync/status"
	"github.com/instructure/ddb-sync/utils"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type BackfillRecord map[string]*dynamodb.AttributeValue

func (r *BackfillRecord) request() *dynamodb.WriteRequest {
	return &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: map[string]*dynamodb.AttributeValue(*r),
		},
	}
}

type BackfillOperation struct {
	OperationPlan     config.OperationPlan
	context           context.Context
	contextCancelFunc context.CancelFunc

	backfillBeginOnce sync.Once
	c                 chan BackfillRecord

	inputClient  *dynamodb.DynamoDB
	outputClient *dynamodb.DynamoDB

	scanning Phase
	writing  Phase

	readItemRateTracker    *RateTracker
	rcuRateTracker         *RateTracker
	wcuRateTracker         *RateTracker
	writtenItemRateTracker *RateTracker
}

func NewBackfillOperation(ctx context.Context, plan config.OperationPlan, cancelFunc context.CancelFunc) (*BackfillOperation, error) {
	inputSession, outputSession, err := plan.GetSessions()
	if err != nil {
		return nil, err
	}

	inputClient := dynamodb.New(inputSession)
	outputClient := dynamodb.New(outputSession)

	// Create operation w/instantiated clients
	return &BackfillOperation{
		OperationPlan:     plan,
		context:           ctx,
		contextCancelFunc: cancelFunc,

		c: make(chan BackfillRecord, recordChanBuffer),

		inputClient:  inputClient,
		outputClient: outputClient,

		readItemRateTracker:    NewRateTracker("Read Items", 9*time.Second),
		rcuRateTracker:         NewRateTracker("RCUs", 9*time.Second),
		wcuRateTracker:         NewRateTracker("WCUs", 9*time.Second),
		writtenItemRateTracker: NewRateTracker("Written Items", 9*time.Second),
	}, nil
}

func (o *BackfillOperation) Preflights(_ *dynamodb.DescribeTableOutput, _ *dynamodb.DescribeTableOutput) error {
	return nil
}

func (o *BackfillOperation) Run() error {
	o.readItemRateTracker.Start()
	o.rcuRateTracker.Start()
	o.wcuRateTracker.Start()
	o.writtenItemRateTracker.Start()

	defer o.readItemRateTracker.Stop()
	defer o.rcuRateTracker.Stop()
	defer o.wcuRateTracker.Stop()
	defer o.writtenItemRateTracker.Stop()

	collator := ErrorCollator{
		Cancel: o.contextCancelFunc,
	}
	collator.Register(o.scan)
	collator.Register(o.batchWrite)

	return collator.Run()
}

func (o *BackfillOperation) Status() string {
	if o.writing.Complete() {
		return completeMsg
	} else if o.errored() {
		return erroredMsg
	}
	return fmt.Sprintf("%d written", o.writtenItemRateTracker.Count())
}

func (o *BackfillOperation) Rate() string {
	if o.writing.Running() {
		return fmt.Sprintf("%s %s %s", o.rcuRateTracker.RatePerSecond(), status.BufferStatus(o.bufferFill(), o.bufferCapacity()), o.wcuRateTracker.RatePerSecond())
	}
	return ""
}

// Checkpoint prints a logging statement summarizing the current state.  Meant for periodic update requests.
func (o *BackfillOperation) Checkpoint() string {
	if o.writing.Running() {
		return fmt.Sprintf("%s: Backfill in progress: %d items written over %s", o.OperationPlan.Description(), o.writtenItemRateTracker.Count(), o.writtenItemRateTracker.Duration().String())
	}
	return ""
}

func (o *BackfillOperation) scan() error {
	defer close(o.c)
	o.scanning.Start()

	collator := ErrorCollator{
		Cancel: o.contextCancelFunc,
	}

	done := o.context.Done()

	scanHandler := func(output *dynamodb.ScanOutput, lastPage bool) bool {
		o.rcuRateTracker.Increment(int64(math.Ceil(*output.ConsumedCapacity.CapacityUnits)))

		for _, item := range output.Items {
			o.readItemRateTracker.Increment(1)

			select {
			case o.c <- BackfillRecord(item):
			case <-done:
				return false
			}
		}
		return true
	}

	if o.OperationPlan.Backfill.TotalSegments > 0 {
		if len(o.OperationPlan.Backfill.Segments) > 0 {
			// If segment indexes are provided, run those segments
			for _, segmentIndex := range o.OperationPlan.Backfill.Segments {
				collator.Register(o.scanner(segmentIndex, o.OperationPlan.Backfill.TotalSegments, scanHandler, done))
			}
		} else {
			// If not segment indexes are provided, run all segment indices
			for i := 0; i < o.OperationPlan.Backfill.TotalSegments; i++ {
				collator.Register(o.scanner(i, o.OperationPlan.Backfill.TotalSegments, scanHandler, done))
			}
		}
	} else {
		// If unspecified, run a single segment
		collator.Register(o.scanner(0, 1, scanHandler, done))
	}

	err := collator.Run()
	if err == nil {
		log.Printf("%s: Backfill: scan complete %d items read over %s", o.OperationPlan.Description(), o.readItemRateTracker.Count(), utils.FormatDuration(o.readItemRateTracker.Duration()))

		o.scanning.Finish()
		return nil
	}

	if err != context.Canceled {
		o.scanning.Error()
		return fmt.Errorf("%s: Backfill failed: (Scan) %v", o.OperationPlan.Description(), err)
	}

	return err
}

func (o *BackfillOperation) scanner(segIndex, segCount int, scanHandler func(*dynamodb.ScanOutput, bool) bool, done <-chan struct{}) func() error {
	return func() error {
		var input *dynamodb.ScanInput
		if segCount > 1 {
			input = &dynamodb.ScanInput{
				ReturnConsumedCapacity: aws.String("TOTAL"),
				Segment:                aws.Int64(int64(segIndex)),
				TotalSegments:          aws.Int64(int64(segCount)),
				TableName:              &o.OperationPlan.Input.TableName,
			}
		} else {
			input = &dynamodb.ScanInput{
				ReturnConsumedCapacity: aws.String("TOTAL"),
				TableName:              &o.OperationPlan.Input.TableName,
			}
		}

		err := o.inputClient.ScanPagesWithContext(o.context, input, scanHandler)

		select {
		case <-done:
			return o.context.Err()
		default:
			return err
		}
	}
}

func (o *BackfillOperation) batchWrite() error {
	collator := ErrorCollator{
		Cancel: o.contextCancelFunc,
	}

	fanOutWidth := runtime.NumCPU() * 1
	for i := 0; i < fanOutWidth; i++ {
		collator.Register(o.batchWriter)
	}

	err := collator.Run()
	if err == nil {
		log.Printf("%s: Backfill complete: %d items written over %s", o.OperationPlan.Description(), o.writtenItemRateTracker.Count(), o.writtenItemRateTracker.Duration().String())

		o.writing.Finish()
		return nil
	}

	if err != context.Canceled {
		o.writing.Error()
		return fmt.Errorf("%s: Backfill failed: (BatchWriteItem) %v", o.OperationPlan.Description(), err)
	}

	return err
}

func (o *BackfillOperation) signalBackfillStart() {
	o.writing.Start()
	log.Printf("[%s] ⇨ [%s]: Backfill started…", o.OperationPlan.Input.TableName, o.OperationPlan.Output.TableName)
}

func (o *BackfillOperation) batchWriter() error {
	batch := make([]*dynamodb.WriteRequest, 0, 25)

	done := o.context.Done()

channel:
	for {
		select {
		case record, ok := <-o.c:
			if !ok {
				break channel
			}

			o.backfillBeginOnce.Do(o.signalBackfillStart)

			batch = append(batch, record.request())
			if len(batch) == 25 {
				requestItems := map[string][]*dynamodb.WriteRequest{o.OperationPlan.Output.TableName: batch}
				batch = batch[:0]
				err := o.sendBatch(requestItems)
				if err != nil {
					return err
				}
			}

		case <-done:
			return o.context.Err()
		}
	}

	if len(batch) > 0 {
		requestItems := map[string][]*dynamodb.WriteRequest{o.OperationPlan.Output.TableName: batch}

		err := o.sendBatch(requestItems)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *BackfillOperation) sendBatch(batch map[string][]*dynamodb.WriteRequest) error {
	input := &dynamodb.BatchWriteItemInput{
		RequestItems:           batch,
		ReturnConsumedCapacity: aws.String("TOTAL"),
	}
	batchLength := len(batch[o.OperationPlan.Output.TableName])

	err := input.Validate()
	if err != nil {
		return err
	}
	result, err := o.outputClient.BatchWriteItemWithContext(o.context, input)
	if err != nil {
	}

	// self-reinvoking
	if len(result.UnprocessedItems) > 0 && len(result.UnprocessedItems[o.OperationPlan.Output.TableName]) > 0 {
		writeCount := batchLength - len(result.UnprocessedItems[o.OperationPlan.Output.TableName])
		o.writtenItemRateTracker.Increment(int64(writeCount))
		o.UpdateConsumedCapacity(result.ConsumedCapacity)
		return o.sendBatch(result.UnprocessedItems)
	}

	o.UpdateConsumedCapacity(result.ConsumedCapacity)
	o.writtenItemRateTracker.Increment(int64(batchLength))

	return nil
}

func (o *BackfillOperation) UpdateConsumedCapacity(capacities []*dynamodb.ConsumedCapacity) {
	var agg float64
	for _, cap := range capacities {
		agg = agg + *cap.CapacityUnits
	}

	o.wcuRateTracker.Increment(int64(math.Ceil(agg)))
}

func (o *BackfillOperation) bufferFill() int {
	return len(o.c)
}

func (o *BackfillOperation) bufferCapacity() int {
	return cap(o.c)
}

func (o *BackfillOperation) errored() bool {
	return o.scanning.Errored() || o.writing.Errored()
}
