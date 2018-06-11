package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"gerrit.instructure.com/ddb-sync/config"
	"gerrit.instructure.com/ddb-sync/log"
	"gerrit.instructure.com/ddb-sync/status"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

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

	approximateItemCount      int64
	approximateTableSizeBytes int64
	scanCount                 int64

	wcuRateTracker   *RateTracker
	writeRateTracker *RateTracker
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

		c: make(chan BackfillRecord, 3500),

		inputClient:  inputClient,
		outputClient: outputClient,

		wcuRateTracker:   NewRateTracker(3 * time.Second),
		writeRateTracker: NewRateTracker(3 * time.Second),
	}, nil
}

type BackfillRecord struct {
	Item map[string]*dynamodb.AttributeValue
}

func (r *BackfillRecord) Request() *dynamodb.WriteRequest {
	return &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: r.Item,
		},
	}
}

func (o *BackfillOperation) Preflights(_ *dynamodb.DescribeTableOutput, _ *dynamodb.DescribeTableOutput) error {
	return nil
}

func (o *BackfillOperation) Run() error {
	o.wcuRateTracker.Start()
	o.writeRateTracker.Start()
	defer o.wcuRateTracker.Stop()
	defer o.writeRateTracker.Stop()

	collator := ErrorCollator{
		Cancel: o.contextCancelFunc,
	}
	collator.Register(o.scan)
	collator.Register(o.batchWrite)

	finalErr := collator.Run()

	return finalErr
}

func (o *BackfillOperation) Status(s *status.Status) {
	if o.writing.Complete() {
		s.Backfill = "-COMPLETE-"
	} else if o.errored() {
		s.Backfill = "-ERRORED-"
	} else {
		s.Rate = o.wcuRateTracker.RecordsPerSecond()

		buffer := float64(o.BufferFill()) / float64(o.BufferCapacity())
		writeCount := fmt.Sprintf("%d written", o.writeRateTracker.Count())

		s.Backfill = fmt.Sprintf("%s %s", s.BufferStatus(buffer), writeCount)
	}
}

func (o *BackfillOperation) scan() error {
	defer close(o.c)
	o.scanning.Start()

	input := &dynamodb.ScanInput{
		TableName: &o.OperationPlan.Input.TableName,
	}

	done := o.context.Done()

	scanHandler := func(output *dynamodb.ScanOutput, lastPage bool) bool {
		var lastReported time.Time
		var itemsReported int

		for i, item := range output.Items {
			if lastReported.Before(time.Now().Add(time.Second)) {
				lastReported = time.Now()

				atomic.AddInt64(&o.scanCount, int64(i-itemsReported))
				itemsReported = i
			}

			select {
			case o.c <- BackfillRecord{Item: item}:
			case <-done:
				return false
			}
		}

		atomic.AddInt64(&o.scanCount, int64(len(output.Items)-itemsReported))

		return true
	}

	err := o.inputClient.ScanPagesWithContext(o.context, input, scanHandler)
	if err != nil {
		o.scanning.Error()
		return fmt.Errorf("[%s] ⇨ [%s]: Backfill failed: (Scan) %v", o.OperationPlan.Input.TableName, o.OperationPlan.Output.TableName, err)
	}

	// check if the context has been canceled
	select {
	case <-done:
		return o.context.Err()

	default:
		o.scanning.Finish()
		return nil
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
		log.Printf("[%s] ⇨ [%s]: Backfill complete!", o.OperationPlan.Input.TableName, o.OperationPlan.Output.TableName)

		o.writing.Finish()
		return nil
	}

	if err != context.Canceled {
		o.writing.Error()
		return fmt.Errorf("[%s] ⇨ [%s]: Backfill failed: (BatchWriteItem) %v", o.OperationPlan.Input.TableName, o.OperationPlan.Output.TableName, err)
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

			batch = append(batch, record.Request())
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
		o.writeRateTracker.Increment(int64(writeCount))
		o.UpdateConsumedCapacity(result.ConsumedCapacity)
		return o.sendBatch(result.UnprocessedItems)
	}

	o.UpdateConsumedCapacity(result.ConsumedCapacity)
	o.writeRateTracker.Increment(int64(batchLength))

	return nil
}

func (o *BackfillOperation) UpdateConsumedCapacity(capacities []*dynamodb.ConsumedCapacity) {
	var agg float64
	for _, cap := range capacities {
		agg = agg + *cap.CapacityUnits
	}

	o.wcuRateTracker.Increment(int64(agg))
}

func (o *BackfillOperation) ApproximateItemCount() int64 {
	return atomic.LoadInt64(&o.approximateItemCount)
}

func (o *BackfillOperation) ApproximateTableSizeBytes() int64 {
	return atomic.LoadInt64(&o.approximateTableSizeBytes)
}

func (o *BackfillOperation) ScanCount() int64 {
	return atomic.LoadInt64(&o.scanCount)
}

func (o *BackfillOperation) BufferFill() int {
	return len(o.c)
}

func (o *BackfillOperation) BufferCapacity() int {
	return cap(o.c)
}

func (o *BackfillOperation) errored() bool {
	return o.scanning.Errored() || o.writing.Errored()
}
