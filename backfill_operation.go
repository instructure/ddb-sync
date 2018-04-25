package main

import (
	"context"

	"gerrit.instructure.com/ddb-sync/plan"
)

type BackfillOperation struct {
	Plan    plan.Plan
	context context.Context
}

func NewBackfillOperation(ctx context.Context, plan plan.Plan) (*BackfillOperation, error) {
	return &BackfillOperation{
		Plan: plan,

		context: ctx,
	}, nil
}

type BackfillRecord struct{} // TODO: REPLACE W/REAL RECORD

func (o *BackfillOperation) Run() error {
	c := make(chan BackfillRecord)

	go o.scan(c)    // TODO: FANOUT?
	o.batchWrite(c) // TODO: FANOUT?

	return errors.New("NOT IMPLEMENTED")
}

func (o *BackfillOperation) Status() string {
	// TODO: RETURN THE CURRENT STATUS
	return "NOT IMPLEMENTED"
}

func (o *BackfillOperation) scan(c chan BackfillRecord) {
	defer close(c)

	// TODO: SCAN ALL RECORDS IN THE TABLE
}

func (o *BackfillOperation) batchWrite(c chan BackfillRecord) {
	for _ = range c {
		// TODO: BATCH AND WRITE ALL RECORDS (probably with a select & timer)
	}
}
