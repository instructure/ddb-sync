package main

import (
	"context"
	"errors"

	"gerrit.instructure.com/ddb-sync/config"

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

	readStatusEnum int32

	receivedCount int64
	writeCount    int64

	c chan dynamodbstreams.Record
}

func NewStreamOperation(ctx context.Context, plan config.OperationPlan, cancelFunc context.CancelFunc) (*StreamOperation, error) {
	return &StreamOperation{
		OperationPlan:     plan,
		context:           ctx,
		contextCancelFunc: cancelFunc,

		c: make(chan dynamodbstreams.Record),
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
	// TODO: RETURN THE CURRENT STATUS
	return "NOT IMPLEMENTED"
}

func (o *StreamOperation) readStream() error {
	defer close(o.c)

	// TODO: READ ALL SHARDS IN THE STREAM
	return errors.New("NOT IMPLEMENTED")
}

func (o *StreamOperation) writeRecords() error {
	for range o.c {
		// TODO: BATCH AND WRITE ALL RECORDS (probably with a select & timer)
	}

	return errors.New("NOT IMPLEMENTED")
}
