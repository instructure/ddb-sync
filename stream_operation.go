package main

import (
	"context"
	"errors"
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

	readStatusEnum  int32
	writeStatusEnum int32

	receivedCount int64
	writeCount    int64

	latency latencyLock

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
	return o.dispatcher.Status()
}

func (o *StreamOperation) readStream() error {
	defer close(o.c)
	atomic.StoreInt32(&o.readStatusEnum, 1)
	log.Printf("[INFO] reading stream for %s", o.OperationPlan.Input.TableName)

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
		log.Printf("[INFO] %s stream closed, scan complete!", o.OperationPlan.Input.TableName)
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
		if len(recordOutput.Records) == 0 {
			o.latency.Update(time.Now())
		}

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
	for range o.c {
		// TODO: BATCH AND WRITE ALL RECORDS (probably with a select & timer)
	}

	return errors.New("NOT IMPLEMENTED")
}
