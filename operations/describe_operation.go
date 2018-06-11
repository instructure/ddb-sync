package operations

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"gerrit.instructure.com/ddb-sync/config"
	"gerrit.instructure.com/ddb-sync/log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/tears-of-noobs/bytefmt"
)

const tickInterval = 15 * time.Minute

type DescribeOperation struct {
	OperationPlan     config.OperationPlan
	context           context.Context
	contextCancelFunc context.CancelFunc

	inputClient *dynamodb.DynamoDB

	describing Phase

	approximateItemCount      int64
	approximateTableSizeBytes int64

	ticker *time.Ticker
}

func NewDescribeOperation(ctx context.Context, plan config.OperationPlan, cancelFunc context.CancelFunc) (*DescribeOperation, error) {
	inputSession, _, err := plan.GetSessions()
	if err != nil {
		return nil, err
	}

	inputClient := dynamodb.New(inputSession)

	// Create a describe operation w/instantiated clients
	return &DescribeOperation{
		OperationPlan:     plan,
		context:           ctx,
		contextCancelFunc: cancelFunc,

		inputClient: inputClient,

		ticker: time.NewTicker(tickInterval),
	}, nil
}

func (o *DescribeOperation) Start() {
	o.describing.Start()
	o.describe()

	for range o.ticker.C {
		o.describe()
	}
}

func (o *DescribeOperation) Stop() {
	o.describing.Finish()
	o.ticker.Stop()
}

func (o *DescribeOperation) Status() string {
	if o.describing.Errored() {
		return "-ERRORED-"
	}
	return fmt.Sprintf("%s items (~%s)", o.ApproximateItemCount(), o.ApproximateTableSize())
}

func (o *DescribeOperation) describe() {
	log.Printf("%s Describing…\n", o.OperationPlan.Description())
	output, _ := o.inputClient.DescribeTableWithContext(o.context, &dynamodb.DescribeTableInput{TableName: aws.String(o.OperationPlan.Input.TableName)})

	atomic.StoreInt64(&o.approximateItemCount, *output.Table.ItemCount)
	atomic.StoreInt64(&o.approximateTableSizeBytes, *output.Table.TableSizeBytes)
}

func (o *DescribeOperation) ApproximateItemCount() string {
	return log.Approximate(int(atomic.LoadInt64(&o.approximateItemCount)))
}

func (o *DescribeOperation) ApproximateTableSize() string {
	bytes := atomic.LoadInt64(&o.approximateTableSizeBytes)
	return bytefmt.FormatBytes(float64(bytes), 0, true)
}
