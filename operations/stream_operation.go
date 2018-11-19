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
	"time"

	"github.com/instructure/ddb-sync/config"
	"github.com/instructure/ddb-sync/log"
	"github.com/instructure/ddb-sync/shard_tree"
	"github.com/instructure/ddb-sync/shard_watcher"
	"github.com/instructure/ddb-sync/status"
	"github.com/instructure/ddb-sync/utils"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

const (
	// In testing, this is sufficient to move to on from a blank shard
	// without backing off prematurely
	blankCountThreshold = 5

	backoffDuration = 3 * time.Second
)

type StreamOperation struct {
	OperationPlan     config.OperationPlan
	context           context.Context
	contextCancelFunc context.CancelFunc

	inputClient  *dynamodbstreams.DynamoDBStreams
	outputClient *dynamodb.DynamoDB

	writeLatency LatencyLock

	c         chan dynamodbstreams.Record
	streamARN string

	streamRead Phase
	writing    Phase

	watcher *shard_watcher.Watcher

	readItemRateTracker    *RateTracker
	wcuRateTracker         *RateTracker
	writtenItemRateTracker *RateTracker
}

func NewStreamOperation(ctx context.Context, plan config.OperationPlan, cancelFunc context.CancelFunc) (*StreamOperation, error) {
	inputSession, outputSession, err := plan.GetSessions()
	if err != nil {
		return nil, err
	}

	inputClient := dynamodbstreams.New(inputSession)
	outputClient := dynamodb.New(outputSession)

	watcherInput := &shard_watcher.RunInput{
		Context:           ctx,
		ContextCancelFunc: cancelFunc,

		InputTableName:       plan.Input.TableName,
		OperationDescription: plan.Description(),
		Client:               inputClient,
	}

	return &StreamOperation{
		OperationPlan:     plan,
		context:           ctx,
		contextCancelFunc: cancelFunc,

		c: make(chan dynamodbstreams.Record, recordChanBuffer),

		inputClient:  inputClient,
		outputClient: outputClient,

		watcher: shard_watcher.New(watcherInput),

		readItemRateTracker:    NewRateTracker("Items", 9*time.Second),
		wcuRateTracker:         NewRateTracker("WCUs", 9*time.Second),
		writtenItemRateTracker: NewRateTracker("Items", 9*time.Second),
	}, nil
}

func (o *StreamOperation) Preflights(in *dynamodb.DescribeTableOutput, _ *dynamodb.DescribeTableOutput) error {
	streamSpecification := in.Table.StreamSpecification
	if streamSpecification == nil {
		return fmt.Errorf("[%s] Fails pre-flight check: stream is not enabled", *in.Table.TableName)
	}
	if !*streamSpecification.StreamEnabled {
		return fmt.Errorf("[%s] Fails pre-flight check: stream is not enabled", *in.Table.TableName)
	}

	if !(*streamSpecification.StreamViewType == dynamodb.StreamViewTypeNewImage || *streamSpecification.StreamViewType == dynamodb.StreamViewTypeNewAndOldImages) {
		return fmt.Errorf("[%s] Fails pre-flight check: stream is not a correct type 'NEW_IMAGE' or 'NEW_AND_OLD_IMAGES'", *in.Table.TableName)
	}

	o.streamARN = *in.Table.LatestStreamArn

	return nil
}

func (o *StreamOperation) Run() error {
	o.readItemRateTracker.Start()
	o.wcuRateTracker.Start()
	o.writtenItemRateTracker.Start()

	defer o.readItemRateTracker.Stop()
	defer o.wcuRateTracker.Stop()
	defer o.writtenItemRateTracker.Stop()

	collator := ErrorCollator{
		Cancel: o.contextCancelFunc,
	}
	collator.Register(o.readStream)
	collator.Register(o.writeRecords)

	return collator.Run()
}

func (o *StreamOperation) Status() string {
	if !o.watcher.Started() {
		return pendingMsg
	}

	return fmt.Sprintf("%d written (%s latent)", o.writtenItemRateTracker.Count(), o.writeLatency.Status())
}

// Checkpoint is a periodic status output meant for historical tracking.  This will be called when an update is desired.
func (o *StreamOperation) Checkpoint() string {
	if o.writing.Running() {
		return fmt.Sprintf("%s: Streaming: %d items written over %s", o.OperationPlan.Description(), o.writtenItemRateTracker.Count(), utils.FormatDuration(o.writtenItemRateTracker.Duration()))
	}
	return ""
}

func (o *StreamOperation) Rate() string {
	if o.writing.Running() {
		return fmt.Sprintf("%s %s %s", o.readItemRateTracker.RatePerSecond(), status.BufferStatus(o.bufferFill(), o.bufferCapacity()), o.wcuRateTracker.RatePerSecond())
	}
	return ""
}

func (o *StreamOperation) readStream() error {
	defer close(o.c)
	log.Printf("%s: Streaming started…", o.OperationPlan.Description())
	o.streamRead.Start()

	o.watcher.StreamARN = o.streamARN
	o.watcher.ShardProcessor = o.processShard

	err := o.watcher.RunWorkers()
	if err == nil {
		log.Printf("%s: Stream closed: %d items written over %s", o.OperationPlan.Description(), o.writtenItemRateTracker.Count(), o.writtenItemRateTracker.Duration().String())
		o.streamRead.Finish()
	} else {
		o.streamRead.Error()
	}

	return err
}

func (o *StreamOperation) processShard(shard *shard_tree.Shard) error {
	shardIteratorOutput, err := o.inputClient.GetShardIteratorWithContext(
		o.context,
		&dynamodbstreams.GetShardIteratorInput{
			StreamArn:         &o.streamARN,
			ShardId:           &shard.Id,
			ShardIteratorType: aws.String("TRIM_HORIZON"),
		},
	)
	if err != nil {
		return err
	}

	iterator := shardIteratorOutput.ShardIterator
	done := o.context.Done()
	var blankCounter uint

	for iterator != nil && *iterator != "" {
		recordInput := &dynamodbstreams.GetRecordsInput{Limit: aws.Int64(1000), ShardIterator: iterator}
		recordOutput, err := o.inputClient.GetRecordsWithContext(o.context, recordInput)
		if err != nil {
			return err
		}

		if len(recordOutput.Records) > 0 {
			blankCounter++
		} else {
			blankCounter = 0
		}

		if blankCounter > blankCountThreshold {
			slpCh := time.After(backoffDuration)
			select {
			case <-slpCh:
			case <-done:
				return o.context.Err()
			}
		}

		for _, record := range recordOutput.Records {
			o.readItemRateTracker.Increment(1)
			select {
			case o.c <- *record:
			case <-done:
				return o.context.Err()
			}
		}

		iterator = recordOutput.NextShardIterator
	}
	return nil
}

func (o *StreamOperation) writeRecords() error {
	o.writing.Start()

	done := o.context.Done()
channel:
	for {
		var consumedCap *dynamodb.ConsumedCapacity
		var err error
		select {
		case record, ok := <-o.c:
			if !ok {
				break channel
			}
			o.writeLatency.Update(*record.Dynamodb.ApproximateCreationDateTime)
			if *record.EventName == "REMOVE" {
				input := &dynamodb.DeleteItemInput{
					Key:                    record.Dynamodb.Keys,
					ReturnConsumedCapacity: aws.String("TOTAL"),
					TableName:              aws.String(o.OperationPlan.Output.TableName),
				}
				var resp *dynamodb.DeleteItemOutput
				resp, err = o.outputClient.DeleteItemWithContext(o.context, input)
				consumedCap = resp.ConsumedCapacity
			} else {
				input := &dynamodb.PutItemInput{
					Item:                   record.Dynamodb.NewImage,
					ReturnConsumedCapacity: aws.String("TOTAL"),
					TableName:              aws.String(o.OperationPlan.Output.TableName),
				}
				var resp *dynamodb.PutItemOutput
				resp, err = o.outputClient.PutItemWithContext(o.context, input)
				consumedCap = resp.ConsumedCapacity

			}
		case <-done:
			return o.context.Err()
		}

		if err != nil {
			o.writing.Error()
			return fmt.Errorf("%s: Stream Failed (writeRecords): %v\n", o.OperationPlan.Description(), err)
		}

		o.markItemWritten(consumedCap)
	}

	o.writing.Finish()

	return nil
}

func (o *StreamOperation) bufferFill() int {
	return len(o.c)
}

func (o *StreamOperation) bufferCapacity() int {
	return cap(o.c)
}

func (o *StreamOperation) markItemWritten(cap *dynamodb.ConsumedCapacity) {
	o.writtenItemRateTracker.Increment(1)
	o.wcuRateTracker.Increment(int64(*cap.CapacityUnits))
}
