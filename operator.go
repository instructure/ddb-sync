package main

import (
	"context"
	"fmt"
	"sync"

	"gerrit.instructure.com/ddb-sync/config"
)

type Operation interface {
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

func (o *Operator) Run() error {
	err := o.describe.Run()
	if err != nil {
		return err
	}

	if !o.OperationPlan.Backfill.Disabled {
		o.operationLock.Lock()
		o.operationPhase = BackfillPhase
		o.operationLock.Unlock()

		err := o.backfill.Run()
		if err != nil {
			return err
		}
	}

	if !o.OperationPlan.Stream.Disabled {
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
