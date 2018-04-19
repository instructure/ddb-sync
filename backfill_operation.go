package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"gerrit.instructure.com/ddb-sync/log"
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
	scanStatusEnum     int32

	approximateItemCount      int64
	approximateTableSizeBytes int64
	batchScanCount            int64
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

type BackfillRecord struct {
	Item map[string]*dynamodb.AttributeValue
}

func (o *BackfillOperation) Run() error {
	collator := ErrorCollator{}
	collator.Register(o.describe)
	collator.Register(o.scan)       // TODO: FANOUT?
	collator.Register(o.batchWrite) // TODO: FANOUT?

	return collator.Run()
}

func (o *BackfillOperation) Status() string {
	inputDescription := ""
	if o.Described() {
		inputDescription = fmt.Sprintf(" ~%d items (~%d bytes)", o.ApproximateItemCount(), o.ApproximateTableSizeBytes())
	}

	status := fmt.Sprintf("Backfilling [%s]%s ⇨ [%s]:  ", o.Plan.Input.TableName, inputDescription, o.Plan.Output.TableName)

	if o.Scanning() || o.ScanComplete() {
		status += fmt.Sprintf("%d read", o.BatchScanCount())
		if o.BufferCapacity() > 0 {
			status += fmt.Sprintf(" ⤏ (buffer:% 3d%%)", 100*o.BufferFill()/o.BufferCapacity())
		}
	} else {
		status += "initializing…"
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
	atomic.StoreInt32(&o.scanStatusEnum, 1)

	input := &dynamodb.ScanInput{
		TableName: &o.Plan.Input.TableName,
	}

	scanHandler := func(output *dynamodb.ScanOutput, lastPage bool) bool {
		var lastReported time.Time
		var itemsReported int

		for i, item := range output.Items {
			if lastReported.Before(time.Now().Add(time.Second)) {
				lastReported = time.Now()

				atomic.AddInt64(&o.batchScanCount, int64(i-itemsReported))
				itemsReported = i
			}

			o.c <- BackfillRecord{Item: item}
		}

		atomic.AddInt64(&o.batchScanCount, int64(len(output.Items)-itemsReported))

		return true
	}

	err := o.inputClient.ScanPagesWithContext(o.context, input, scanHandler)
	if err != nil {
		return fmt.Errorf("[SCAN] [%s] failed: %v", o.Plan.Input.TableName, err)
	}

	atomic.StoreInt32(&o.scanStatusEnum, 2)
	log.Printf("[INFO] %s scan complete!", o.Plan.Input.TableName)
	return nil
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

func (o *BackfillOperation) BatchScanCount() int64 {
	return atomic.LoadInt64(&o.batchScanCount)
}

func (o *BackfillOperation) Scanning() bool {
	return atomic.LoadInt32(&o.scanStatusEnum) == 1
}

func (o *BackfillOperation) ScanComplete() bool {
	return atomic.LoadInt32(&o.scanStatusEnum) == 2
}

func (o *BackfillOperation) BufferFill() int {
	return len(o.c)
}

func (o *BackfillOperation) BufferCapacity() int {
	return cap(o.c)
}
