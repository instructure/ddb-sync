/*
 * ddb-sync
 * Copyright (C) 2018 Instructure Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package main

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/instructure/ddb-sync/config"
	"github.com/instructure/ddb-sync/log"
	"github.com/instructure/ddb-sync/operations"
	"github.com/instructure/ddb-sync/status"
)

const (
	checkpointHeader = "================= Progress Update =================="
	checkpointFooter = "===================================================="
)

type Dispatcher struct {
	Operators   []*operations.Operator
	operatorsWG sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

func NewDispatcher(plans []config.OperationPlan) (*Dispatcher, error) {
	var operators []*operations.Operator
	ctx, cancel := context.WithCancel(context.Background())

	var finalErr error
	for _, plan := range plans {
		plan = plan.WithDefaults()
		err := plan.Validate()
		if err != nil {
			fmt.Printf("[ERROR] %v\n", err)
			finalErr = err
			continue
		}

		operator, err := operations.NewOperator(ctx, plan, cancel)
		if err != nil {
			fmt.Printf("[ERROR] %v\n", err)
			finalErr = err
			continue
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
	err := quickCheckForActiveCredentials(d.ctx)
	if err != nil {
		fmt.Println("[ERROR] No valid credentials found")
		return err
	}

	var finalErr error

	for _, operator := range d.Operators {
		err := operator.Preflights()
		if err != nil {
			fmt.Printf("[ERROR] %v\n", err)
			finalErr = err
		}
	}

	return finalErr
}

func (d *Dispatcher) Run() error {
	collator := operations.ErrorCollator{
		Cancel: d.cancel,
	}

	for _, operator := range d.Operators {
		collator.Register(operator.Run)
	}

	return collator.Run()
}

func (d *Dispatcher) Checkpoint() {
	checkpoints := []string{"", checkpointHeader}
	for _, operator := range d.Operators {
		adtl := operator.Checkpoint()
		if len(adtl) > 0 {
			checkpoints = append(checkpoints, operator.Checkpoint())
		}
	}
	checkpoints = append(checkpoints, checkpointFooter)
	log.Printf(strings.Join(checkpoints, "\n"))
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
