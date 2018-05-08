package main

import (
	"context"
	"fmt"
	"sync"

	"gerrit.instructure.com/ddb-sync/config"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// preflightRetries is set to less than defaults because it causes preflights when
// unauthenticated to take a very long time to fail
const preflightRetries = 7

type Operation interface {
	Preflights(*dynamodb.DescribeTableOutput, *dynamodb.DescribeTableOutput) error
	Run() error
	Status() string
}

type OperatorPhase int

const (
	NotStartedPhase OperatorPhase = iota
	BackfillPhase
	StreamPhase
	NoopPhase
)

type Operator struct {
	OperationPlan config.OperationPlan

	context           context.Context
	contextCancelFunc context.CancelFunc

	operationLock  sync.Mutex
	operationPhase OperatorPhase

	describe Operation
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

	err = o.describe.Preflights(inDescr, outDescr)
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
	err := o.describe.Run()
	if err != nil {
		return err
	}

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

func (o *Operator) Status() string {
	o.operationLock.Lock()
	defer o.operationLock.Unlock()

	switch o.operationPhase {
	case NotStartedPhase:
		return "Waiting…"
	case BackfillPhase:
		return o.backfill.Status()
	case StreamPhase:
		return o.stream.Status()
	case NoopPhase:
		return fmt.Sprintf("Nothing to do: [%s] ⇨ [%s]:  ", o.OperationPlan.Input.TableName, o.OperationPlan.Output.TableName)
	default:
		return "INTERNAL ERROR: Unknown operation status"
	}
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
