package main

import (
	"context"
	"errors"
	"sync"

	"gerrit.instructure.com/ddb-sync/config"
	"gerrit.instructure.com/ddb-sync/log"
	"gerrit.instructure.com/ddb-sync/status"
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

func NewDispatcher(plans []config.OperationPlan) (*Dispatcher, error) {
	var operators []*Operator
	ctx, cancel := context.WithCancel(context.Background())

	var finalErr error
	for _, plan := range plans {
		plan = plan.WithDefaults()
		err := plan.Validate()
		if err != nil {
			log.Printf("[ERROR] %v\n", err)
			finalErr = err
		}

		operator, err := NewOperator(ctx, plan, cancel)
		if err != nil {
			log.Printf("[ERROR] %v\n", err)
			finalErr = err
		}
		operators = append(operators, operator)
	}

	return &Dispatcher{
		Operators: operators,
		ctx:       ctx,
		cancel:    cancel,
	}, finalErr
}

func (d *Dispatcher) Preflights() error {
	var finalErr error
	for _, operator := range d.Operators {
		err := operator.Preflights()
		if err != nil {
			log.Printf("[ERROR] %v\n", err)
			finalErr = err
		}
	}

	return finalErr
}

func (d *Dispatcher) Start() {
	collator := ErrorCollator{
		Cancel: d.cancel,
	}

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

func (d *Dispatcher) Statuses() *status.Set {
	var statuses []*status.Status
	for _, operator := range d.Operators {
		statuses = append(statuses, operator.Status())
	}
	return status.NewSet(statuses)
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
