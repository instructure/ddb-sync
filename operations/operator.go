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

var ErrOperationFailed = errors.New("Operation failed")

type operatorPhase int

const (
	NotStartedPhase operatorPhase = iota
	BackfillPhase
	StreamPhase
	NoopPhase
	CompletedPhase
)

type Operation interface {
	Checkpoint() string
	Preflights(*dynamodb.DescribeTableOutput, *dynamodb.DescribeTableOutput) error
	Rate() string
	Run() error
	Status() string
}

type Operator struct {
	OperationPlan config.OperationPlan

	context           context.Context
	contextCancelFunc context.CancelFunc

	mOperatorPhase sync.Mutex
	operatorPhase  operatorPhase

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

	inputClient := dynamodb.New(inputSession)
	outputClient := dynamodb.New(outputSession)

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
		o.mOperatorPhase.Lock()
		o.operatorPhase = BackfillPhase
		o.mOperatorPhase.Unlock()

		err := o.backfill.Run()
		if err != nil {
			return err
		}
	}

	if o.stream != nil {
		o.mOperatorPhase.Lock()
		o.operatorPhase = StreamPhase
		o.mOperatorPhase.Unlock()

		err := o.stream.Run()
		if err != nil {
			return err
		}
	}

	o.mOperatorPhase.Lock()
	if o.backfill == nil && o.stream == nil {
		o.operatorPhase = NoopPhase
	} else {
		o.operatorPhase = CompletedPhase
	}
	o.mOperatorPhase.Unlock()

	return nil
}

func (o *Operator) Checkpoint() string {
	o.mOperatorPhase.Lock()
	defer o.mOperatorPhase.Unlock()

	switch o.operatorPhase {
	case NotStartedPhase:
		return fmt.Sprintf("%s Waiting", o.OperationPlan.Description())
	case BackfillPhase:
		return o.backfill.Checkpoint()
	case StreamPhase:
		return o.stream.Checkpoint()
	case CompletedPhase:
		return fmt.Sprintf("%s Completed", o.OperationPlan.Description())
	}
	return ""
}

func (o *Operator) Status() *status.Status {
	status := status.New(o.OperationPlan)

	status.Description = o.describe.Status()

	if o.backfill != nil {
		status.Backfill = o.backfill.Status()
	}

	if o.stream != nil {
		status.Stream = o.stream.Status()
	}

	o.mOperatorPhase.Lock()
	defer o.mOperatorPhase.Unlock()

	switch o.operatorPhase {
	case NotStartedPhase:
		status.SetWaiting()
	case BackfillPhase:
		status.Rate = o.backfill.Rate()
	case StreamPhase:
		status.Rate = o.stream.Rate()
	case NoopPhase:
		status.SetNoop()
	case CompletedPhase:
	default:
		status.SetError()
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
			return nil, fmt.Errorf("[%s] Describe table operation failed with %v", tableName, err)
		}
		return nil, err
	}

	if *description.Table.TableStatus != "ACTIVE" {
		return nil, fmt.Errorf("[%s] Fails pre-flight check: table status is not active", tableName)
	}
	return description, nil
}
