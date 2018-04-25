package main

import (
	"context"
	"errors"

	"gerrit.instructure.com/ddb-sync/plan"
)

type BackfillOperation struct {
	Plan    plan.Plan
	context context.Context

	c chan BackfillRecord
}

func NewBackfillOperation(ctx context.Context, plan plan.Plan) (*BackfillOperation, error) {
	return &BackfillOperation{
		Plan:    plan,
		context: ctx,

		c: make(chan BackfillRecord),
	}, nil
}

type BackfillRecord struct{} // TODO: REPLACE W/REAL RECORD

func (o *BackfillOperation) Run() error {
	collator := ErrorCollator{}
	collator.Register(o.scan)       // TODO: FANOUT?
	collator.Register(o.batchWrite) // TODO: FANOUT?

	return collator.Run()
}

func (o *BackfillOperation) Status() string {
	// TODO: RETURN THE CURRENT STATUS
	return "NOT IMPLEMENTED"
}

func (o *BackfillOperation) scan() error {
	defer close(o.c)

	// TODO: SCAN ALL RECORDS IN THE TABLE

	return errors.New("NOT IMPLEMENTED")
}

func (o *BackfillOperation) batchWrite() error {
	for _ = range o.c {
		// TODO: BATCH AND WRITE ALL RECORDS (probably with a select & timer)
	}

	return errors.New("NOT IMPLEMENTED")
}
