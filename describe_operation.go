package main

import (
	"context"
	"fmt"
	"sync/atomic"

	"gerrit.instructure.com/ddb-sync/config"
	"gerrit.instructure.com/ddb-sync/log"
	"gerrit.instructure.com/ddb-sync/status"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/tears-of-noobs/bytefmt"
)

type DescribeOperation struct {
	OperationPlan     config.OperationPlan
	context           context.Context
	contextCancelFunc context.CancelFunc

	inputClient *dynamodb.DynamoDB

	describing Phase

	approximateItemCount      int64
	approximateTableSizeBytes int64
}

func NewDescribeOperation(ctx context.Context, plan config.OperationPlan, cancelFunc context.CancelFunc) (*DescribeOperation, error) {
	inputSession, _, err := plan.GetSessions()
	if err != nil {
		return nil, err
	}

	inputClient := dynamodb.New(inputSession)

	// Create operation w/instantiated clients
	return &DescribeOperation{
		OperationPlan:     plan,
		context:           ctx,
		contextCancelFunc: cancelFunc,

		inputClient: inputClient,
	}, nil
}

func (o *DescribeOperation) Preflights(_ *dynamodb.DescribeTableOutput, _ *dynamodb.DescribeTableOutput) error {
	return nil
}

func (o *DescribeOperation) Run() error {
	collator := ErrorCollator{
		Cancel: o.contextCancelFunc,
	}
	collator.Register(o.describe)

	return collator.Run()
}

func (o *DescribeOperation) Status(s *status.Status) {
	if o.describing.Errored() {
		s.Description = "-ERRORED-"
	}
	// // TODO: Approximate these numbers
	s.Description = fmt.Sprintf("%s items (~%s)", o.ApproximateItemCount(), o.ApproximateTableSize())
}

func (o *DescribeOperation) describe() error {
	o.describing.Start()
	output, err := o.inputClient.DescribeTableWithContext(o.context, &dynamodb.DescribeTableInput{TableName: aws.String(o.OperationPlan.Input.TableName)})
	if err != nil {
		o.describing.Error()
		return fmt.Errorf("[%s] ⇨ [%s]: Backfill failed: (DescribeTable) %v", o.OperationPlan.Input.TableName, o.OperationPlan.Output.TableName, err)
	}

	atomic.StoreInt64(&o.approximateItemCount, *output.Table.ItemCount)
	atomic.StoreInt64(&o.approximateTableSizeBytes, *output.Table.TableSizeBytes)
	o.describing.Finish()
	return nil
}

func (o *DescribeOperation) ApproximateItemCount() string {
	return log.Approximate(int(atomic.LoadInt64(&o.approximateItemCount)))
}

func (o *DescribeOperation) ApproximateTableSize() string {
	bytes := atomic.LoadInt64(&o.approximateTableSizeBytes)
	return bytefmt.FormatBytes(float64(bytes), 0, true)
}
