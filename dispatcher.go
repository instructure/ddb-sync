package main

import (
	"context"
	"errors"
	"sync"

	"gerrit.instructure.com/ddb-sync/plan"
)

var (
	ErrOperationFailed = errors.New("Operation failed")
)

type Dispatcher struct {
	Operators   []*Operator
	operatorsWG sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	errLock sync.Mutex
	err     error
}

func NewDispatcher(plans []plan.Plan) (*Dispatcher, error) {
	var operators []*Operator
	ctx, cancel := context.WithCancel(context.Background())
	for _, plan := range plans {
		plan = plan.WithDefaults()
		err := plan.Validate()
		if err != nil {
			cancel()
			return nil, err
		}

		operator, err := NewOperator(ctx, plan)
		if err != nil {
			cancel()
			return nil, err
		}
		operators = append(operators, operator)
	}

	return &Dispatcher{
		Operators: operators,
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

func (d *Dispatcher) Start() {
	collator := ErrorCollator{}

	d.operatorsWG.Add(len(d.Operators))
	for i := range d.Operators {
		operator := d.Operators[i]
		collator.Register(func() error {
			defer d.operatorsWG.Done()
			return operator.Run()
		})
	}

	go func() {
		d.errLock.Lock()
		defer d.errLock.Unlock()

		d.err = collator.Run()
	}()
}

func (d *Dispatcher) Statuses() []string {
	var statuses []string
	for _, operator := range d.Operators {
		statuses = append(statuses, operator.Status())
	}
	return statuses
}

func (d *Dispatcher) Cancel() {
	d.cancel()
}

func (d *Dispatcher) Wait() error {
	d.operatorsWG.Wait()

	d.errLock.Lock()
	defer d.errLock.Unlock()
	return d.err
}
