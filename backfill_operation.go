package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"gerrit.instructure.com/ddb-sync/plan"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type BackfillOperation struct {
	Plan    plan.Plan
	context context.Context

	c chan BackfillRecord

	inputClient *dynamodb.DynamoDB

	describeStatusEnum int32

	approximateItemCount      int64
	approximateTableSizeBytes int64
}

func NewBackfillOperation(ctx context.Context, plan plan.Plan) (*BackfillOperation, error) {
	// Base config & session (used for STS calls)
	baseConfig := aws.NewConfig().WithRegion(plan.Input.Region).WithMaxRetries(15)
	baseSession, err := session.NewSession(baseConfig)
	if err != nil {
		return nil, err
	}

	// Input config, session, & client (used for input-side DynamoDB calls)
	inputConfig := baseConfig.Copy()
	if plan.Input.RoleARN != "" {
		inputConfig.WithCredentials(stscreds.NewCredentials(baseSession, plan.Input.RoleARN))
	}
	inputSession, err := session.NewSession(inputConfig)
	if err != nil {
		return nil, err
	}
	inputClient := dynamodb.New(inputSession)

	// Create operation w/instantiated clients
	return &BackfillOperation{
		Plan:    plan,
		context: ctx,

		c: make(chan BackfillRecord),

		inputClient: inputClient,
	}, nil
}

type BackfillRecord struct{} // TODO: REPLACE W/REAL RECORD

func (o *BackfillOperation) Run() error {
	collator := ErrorCollator{}
	collator.Register(o.describe)
	collator.Register(o.scan)       // TODO: FANOUT?
	collator.Register(o.batchWrite) // TODO: FANOUT?

	return collator.Run()
}

func (o *BackfillOperation) Status() string {
	status := o.Plan.Input.TableName
	if o.Described() {
		status += fmt.Sprintf(" (%d items; %d bytes)", o.ApproximateItemCount(), o.ApproximateTableSizeBytes())
	}
	return status
}

func (o *BackfillOperation) describe() error {
	output, err := o.inputClient.DescribeTableWithContext(o.context, &dynamodb.DescribeTableInput{TableName: aws.String(o.Plan.Input.TableName)})
	if err != nil {
		return fmt.Errorf("[DESCRIBE] [%s] failed: %v", o.Plan.Input.TableName, err)
	}

	atomic.StoreInt64(&o.approximateItemCount, *output.Table.ItemCount)
	atomic.StoreInt64(&o.approximateTableSizeBytes, *output.Table.TableSizeBytes)
	atomic.StoreInt32(&o.describeStatusEnum, 1)
	return nil
}

func (o *BackfillOperation) scan() error {
	defer close(o.c)

	// TODO: SCAN ALL RECORDS IN THE TABLE

	return errors.New("NOT IMPLEMENTED")
}

func (o *BackfillOperation) batchWrite() error {
	for _ = range o.c {
		// TODO: BATCH AND WRITE ALL RECORDS (probably with a select & timer)
	}

	return errors.New("NOT IMPLEMENTED")
}

func (o *BackfillOperation) Described() bool {
	return atomic.LoadInt32(&o.describeStatusEnum) == 1
}

func (o *BackfillOperation) ApproximateItemCount() int64 {
	return atomic.LoadInt64(&o.approximateItemCount)
}

func (o *BackfillOperation) ApproximateTableSizeBytes() int64 {
	return atomic.LoadInt64(&o.approximateTableSizeBytes)
}
