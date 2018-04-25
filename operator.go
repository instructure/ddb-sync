package main

import (
	"sync"

	"gerrit.instructure.com/ddb-sync/plan"
)

type Operation interface {
	Run() error
	Stop()

	Status() string
}

type OperatorPhase int

const (
	NotStartedPhase OperatorPhase = iota
	BackfillPhase
	StreamPhase
)

type Operator struct {
	Plan plan.Plan

	operationLock  sync.Mutex
	operationPhase OperatorPhase
	backfill       Operation
	stream         Operation
}

func NewOperator(plan plan.Plan) (*Operator, error) {
	var err error

	o := &Operator{
		Plan: plan,
	}

	if !o.Plan.Backfill.Disabled {
		o.backfill, err = NewBackfillOperation(plan)
		if err != nil {
			return nil, err
		}
	}

	if !o.Plan.Stream.Disabled {
		o.stream, err = NewStreamOperation(plan)
		if err != nil {
			return nil, err
		}
	}

	return o, nil
}

func (o *Operator) Run() error {
	if !o.Plan.Backfill.Disabled {
		o.operationLock.Lock()
		o.operationPhase = BackfillPhase
		o.operationLock.Unlock()

		err := o.backfill.Run()
		if err != nil {
			return err
		}
	}

	if !o.Plan.Stream.Disabled {
		o.operationLock.Lock()
		o.operationPhase = StreamPhase
		o.operationLock.Unlock()

		err := o.stream.Run()
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *Operator) Stop() {
	o.operationLock.Lock()
	defer o.operationLock.Unlock()

	switch o.operationPhase {
	case BackfillPhase:
		o.backfill.Stop()
	case StreamPhase:
		o.stream.Stop()
	}
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
	default:
		return "INTERNAL ERROR: Unknown operation status"
	}
}
