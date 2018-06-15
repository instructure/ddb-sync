package operations

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"gerrit.instructure.com/ddb-sync/config"
	"gerrit.instructure.com/ddb-sync/status"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// preflightRetries is set to less than defaults because it causes preflights when
// unauthenticated to take a very long time to fail
const preflightRetries = 7

var ErrOperationFailed = errors.New("Operation failed")

type Operation interface {
	Preflights(*dynamodb.DescribeTableOutput, *dynamodb.DescribeTableOutput) error
	Run() error
	Rate() string
	Status() string
}

type OperatorPhase int

const (
	NotStartedPhase OperatorPhase = iota
	BackfillPhase
	StreamPhase
	NoopPhase
	CompletedPhase
)

type Operator struct {
	OperationPlan config.OperationPlan

	context           context.Context
	contextCancelFunc context.CancelFunc

	operationLock  sync.Mutex
	operationPhase OperatorPhase

	describe *DescribeOperation

	backfill Operation
	stream   Operation
}

func NewOperator(ctx context.Context, plan config.OperationPlan, cancelFunc context.CancelFunc) (*Operator, error) {
	var err error

	o := &Operator{
		OperationPlan:     plan,
		context:           ctx,
		contextCancelFunc: cancelFunc,
	}

	o.describe, err = NewDescribeOperation(ctx, plan, cancelFunc)
	if err != nil {
		return nil, err
	}

	if !o.OperationPlan.Backfill.Disabled {
		o.backfill, err = NewBackfillOperation(ctx, plan, cancelFunc)
		if err != nil {
			return nil, err
		}
	}

	if !o.OperationPlan.Stream.Disabled {
		o.stream, err = NewStreamOperation(ctx, plan, cancelFunc)
		if err != nil {
			return nil, err
		}
	}

	return o, nil
}

func (o *Operator) Preflights() error {
	inputSession, outputSession, err := o.OperationPlan.GetSessions()
	if err != nil {
		return err
	}

	inputClient := dynamodb.New(inputSession.Copy(aws.NewConfig().WithMaxRetries(preflightRetries)))
	outputClient := dynamodb.New(outputSession.Copy(aws.NewConfig().WithMaxRetries(preflightRetries)))

	inDescr, err := o.getTableDescription(inputClient, o.OperationPlan.Input.TableName)
	if err != nil {
		return err
	}

	outDescr, err := o.getTableDescription(outputClient, o.OperationPlan.Output.TableName)
	if err != nil {
		return err
	}

	if o.backfill != nil {
		err := o.backfill.Preflights(inDescr, outDescr)
		if err != nil {
			return err
		}
	}

	if o.stream != nil {
		err := o.stream.Preflights(inDescr, outDescr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Operator) Run() error {
	go o.describe.Start()
	defer o.describe.Stop()

	if o.backfill != nil {
		o.operationLock.Lock()
		o.operationPhase = BackfillPhase
		o.operationLock.Unlock()

		err := o.backfill.Run()
		if err != nil {
			return err
		}
	}

	if o.stream != nil {
		o.operationLock.Lock()
		o.operationPhase = StreamPhase
		o.operationLock.Unlock()

		err := o.stream.Run()
		if err != nil {
			return err
		}
	}

	if o.OperationPlan.Backfill.Disabled && o.OperationPlan.Stream.Disabled {
		o.operationPhase = NoopPhase
	}

	return nil
}

func (o *Operator) Status() *status.Status {
	o.operationLock.Lock()
	defer o.operationLock.Unlock()
	status := status.New(o.OperationPlan)

	status.Description = o.describe.Status()

	if o.backfill != nil {
		status.Backfill = o.backfill.Status()
	}

	if o.stream != nil {
		status.Stream = o.stream.Status()
	}

	switch o.operationPhase {
	case NotStartedPhase:
		status.WaitingStatus()
	case BackfillPhase:
		status.Rate = o.backfill.Rate()
	case StreamPhase:
		status.Rate = o.stream.Rate()
	case NoopPhase:
		status.NoopStatus()
	case CompletedPhase:
	default:
		status.ErrorStatus()
	}
	return status
}

func (o *Operator) getTableDescription(client *dynamodb.DynamoDB, tableName string) (*dynamodb.DescribeTableOutput, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	description, err := client.DescribeTableWithContext(o.context, input)

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ResourceNotFoundException" {
				return nil, fmt.Errorf("[%s] Failed pre-flight check: table does not exist", tableName)
			}
			return nil, fmt.Errorf("[%s] describe table operation failed with %v", tableName, err)
		}
		return nil, err
	}

	if *description.Table.TableStatus != "ACTIVE" {
		return nil, fmt.Errorf("[%s] Fails pre-flight check: table status is not active", tableName)
	}
	return description, nil
}
