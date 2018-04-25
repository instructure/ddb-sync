package main

import (
	"context"
	"sync"

	"gerrit.instructure.com/ddb-sync/plan"
)

type Dispatcher struct {
	Operators   []*Operator
	operatorsWG sync.WaitGroup

	ctx      context.Context
	cancel   context.CancelFunc
	errsLock sync.Mutex
	errs     []error
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
	d.operatorsWG.Add(len(d.Operators))
	for i := range d.Operators {
		operator := d.Operators[i]
		go func() {
			defer d.operatorsWG.Done()

			// Run the operator
			err := operator.Run()
			if err != nil {
				d.errsLock.Lock()
				defer d.errsLock.Unlock()

				d.errs = append(d.errs, err)
			}
		}()
	}
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

func (d *Dispatcher) Wait() []error {
	d.operatorsWG.Wait()

	d.errsLock.Lock()
	defer d.errsLock.Unlock()
	return d.errs
}
