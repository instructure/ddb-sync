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

type Operator struct {
	Plan plan.Plan

	operationLock sync.Mutex
	operation     Operation
}

func NewOperator(plan plan.Plan) *Operator {
	return &Operator{
		Plan: plan,
	}
}

func (o *Operator) Run() error {
	if !o.Plan.Backfill.Disabled {
		o.operationLock.Lock()
		o.operation = &BackfillOperation{}
		o.operationLock.Unlock()

		err := o.operation.Run()
		if err != nil {
			return err
		}
	}

	if !o.Plan.Stream.Disabled {
		o.operationLock.Lock()
		o.operation = &StreamOperation{}
		o.operationLock.Unlock()

		err := o.operation.Run()
		if err != nil {
			return err
		}
	}

	return nil
}

func (o *Operator) Stop() {
	o.operationLock.Lock()
	defer o.operationLock.Unlock()

	if o.operation != nil {
		o.operation.Stop()
	}
}

func (o *Operator) Status() string {
	o.operationLock.Lock()
	defer o.operationLock.Unlock()

	if o.operation != nil {
		return o.operation.Status()
	} else {
		return "INTERNAL ERROR: Operation missing"
	}
}
