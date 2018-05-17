package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"gerrit.instructure.com/ddb-sync/config"
	"gerrit.instructure.com/ddb-sync/dispatcher"
	"gerrit.instructure.com/ddb-sync/log"
	"gerrit.instructure.com/ddb-sync/shard_tree"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

type StreamOperation struct {
	OperationPlan     config.OperationPlan
	context           context.Context
	contextCancelFunc context.CancelFunc

	inputDescribeTableClient *dynamodb.DynamoDB
	inputClient              *dynamodbstreams.DynamoDBStreams
	outputClient             *dynamodb.DynamoDB

	writeStatusEnum int32

	receivedCount int64
	writeCount    int64

	checkLatency latencyLock
	writeLatency latencyLock

	c         chan dynamodbstreams.Record
	streamARN string

	dispatcher *dispatcher.Dispatcher
}

func NewStreamOperation(ctx context.Context, plan config.OperationPlan, cancelFunc context.CancelFunc) (*StreamOperation, error) {
	inputSession, outputSession, err := plan.GetSessions()
	if err != nil {
		return nil, err
	}

	inputClient := dynamodbstreams.New(inputSession)
	inputDescribeTableClient := dynamodb.New(inputSession)
	outputClient := dynamodb.New(outputSession)

	return &StreamOperation{
		OperationPlan:     plan,
		context:           ctx,
		contextCancelFunc: cancelFunc,

		c: make(chan dynamodbstreams.Record, 3500),

		inputClient:              inputClient,
		inputDescribeTableClient: inputDescribeTableClient,
		outputClient:             outputClient,
	}, nil
}

func (o *StreamOperation) Run() error {
	collator := ErrorCollator{
		Cancel: o.contextCancelFunc,
	}
	collator.Register(o.readStream)
	collator.Register(o.writeRecords)

	return collator.Run()
}

func (o *StreamOperation) Status() string {
	status := ""
	if o.Writing() {
		status += fmt.Sprintf("%s [%s] ⇨ [%s]:", o.dispatcher.Status(), o.OperationPlan.Input.TableName, o.OperationPlan.Output.TableName)

		status += fmt.Sprintf("  %d received ⤏ ", o.ReceivedCount())

		if o.BufferCapacity() > 0 {
			status += fmt.Sprintf(" (buffer:% 3d%%) ⤏ ", 100*o.BufferFill()/o.BufferCapacity())
		}
		status += fmt.Sprintf(" %d written", o.WriteCount())

		status += fmt.Sprintf(" latencies: record %s", o.writeLatency.Status())

		status += fmt.Sprintf(" - query %s", o.checkLatency.Status())
	}
	if o.WriteComplete() {
		status += fmt.Sprintf("[%s] ⇨ [%s] stream write complete (%d items)", o.OperationPlan.Input.TableName, o.OperationPlan.Output.TableName, o.WriteCount())
	}

	return status
}

func (o *StreamOperation) readStream() error {
	defer close(o.c)
	log.Printf("[%s] ⇨ [%s]: Streaming started…", o.OperationPlan.Input.TableName, o.OperationPlan.Output.TableName)

	err := o.lookupLatestStreamARN(o.OperationPlan.Input.TableName)
	if err != nil {
		return err
	}

	dispatcherInput := &dispatcher.DispatchInput{
		Context:           o.context,
		ContextCancelFunc: o.contextCancelFunc,

		InputTableName: o.OperationPlan.Input.TableName,
		StreamARN:      o.streamARN,
		Client:         o.inputClient,

		ShardProcessor: o.processShard,
	}

	o.dispatcher = dispatcher.New(dispatcherInput)

	err = o.dispatcher.RunWorkers()
	if err == nil {
		log.Printf("[%s] ⇨ [%s]: Stream closed…", o.OperationPlan.Input.TableName, o.OperationPlan.Output.TableName)
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

	recordInput := &dynamodbstreams.GetRecordsInput{Limit: aws.Int64(1000), ShardIterator: iterator}
	recordOutput, err := o.inputClient.GetRecordsWithContext(o.context, recordInput)

	if err != nil {
		return err
	}

	for recordOutput.NextShardIterator != nil && *recordOutput.NextShardIterator != "" {
		o.checkLatency.Update(time.Now())
		for _, record := range recordOutput.Records {
			atomic.AddInt64(&o.receivedCount, 1)
			select {
			case o.c <- *record:
			case <-o.context.Done():
				return o.context.Err()
			}
		}

		iterator := recordOutput.NextShardIterator
		recordInput := &dynamodbstreams.GetRecordsInput{Limit: aws.Int64(1000), ShardIterator: iterator}
		var err error
		recordOutput, err = o.inputClient.GetRecordsWithContext(o.context, recordInput)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *StreamOperation) lookupLatestStreamARN(tableName string) error {
	tableOutput, err := o.inputDescribeTableClient.DescribeTableWithContext(
		o.context,
		&dynamodb.DescribeTableInput{
			TableName: &o.OperationPlan.Input.TableName,
		},
	)

	if err != nil {
		return err
	}

	o.streamARN = *tableOutput.Table.LatestStreamArn
	return nil
}

func (o *StreamOperation) writeRecords() error {
	atomic.StoreInt32(&o.writeStatusEnum, 1)
	for record := range o.c {
		o.writeLatency.Update(*record.Dynamodb.ApproximateCreationDateTime)
		if *record.EventName == "REMOVE" {
			input := &dynamodb.DeleteItemInput{
				Key:       record.Dynamodb.Keys,
				TableName: aws.String(o.OperationPlan.Output.TableName),
			}
			_, err := o.outputClient.DeleteItemWithContext(o.context, input)
			if err != nil {
				return err
			}
		} else {
			input := &dynamodb.PutItemInput{
				Item:      record.Dynamodb.NewImage,
				TableName: aws.String(o.OperationPlan.Output.TableName),
			}
			_, err := o.outputClient.PutItemWithContext(o.context, input)
			if err != nil {
				return err
			}
		}

		o.markItemReceived(record)
	}

	atomic.StoreInt32(&o.writeStatusEnum, 2)

	return nil
}

func (o *StreamOperation) BufferFill() int {
	return len(o.c)
}

func (o *StreamOperation) BufferCapacity() int {
	return cap(o.c)
}

func (o *StreamOperation) ReceivedCount() int64 {
	return atomic.LoadInt64(&o.receivedCount)
}

func (o *StreamOperation) Writing() bool {
	return atomic.LoadInt32(&o.writeStatusEnum) == 1
}

func (o *StreamOperation) WriteComplete() bool {
	return atomic.LoadInt32(&o.writeStatusEnum) == 2
}

func (o *StreamOperation) WriteCount() int64 {
	return atomic.LoadInt64(&o.writeCount)
}

func (o *StreamOperation) markItemReceived(record dynamodbstreams.Record) {
	atomic.AddInt64(&o.writeCount, 1)
}
