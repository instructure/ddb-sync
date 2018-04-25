package main

import (
	"context"
	"errors"

	"gerrit.instructure.com/ddb-sync/plan"
)

type StreamOperation struct {
	Plan    plan.Plan
	context context.Context

	c chan StreamRecord
}

func NewStreamOperation(ctx context.Context, plan plan.Plan) (*StreamOperation, error) {
	return &StreamOperation{
		Plan:    plan,
		context: ctx,

		c: make(chan StreamRecord),
	}, nil
}

type StreamRecord struct{} // TODO: REPLACE W/REAL RECORD

func (o *StreamOperation) Run() error {
	collator := ErrorCollator{}
	collator.Register(o.readStream) // TODO: FANOUT?
	collator.Register(o.batchWrite) // TODO: FANOUT?

	return collator.Run()
}

func (o *StreamOperation) Status() string {
	// TODO: RETURN THE CURRENT STATUS
	return "NOT IMPLEMENTED"
}

func (o *StreamOperation) readStream() error {
	defer close(o.c)

	// TODO: READ ALL SHARDS IN THE STREAM

	return errors.New("NOT IMPLEMENTED")
}

func (o *StreamOperation) batchWrite() error {
	for _ = range o.c {
		// TODO: BATCH AND WRITE ALL RECORDS (probably with a select & timer)
	}

	return errors.New("NOT IMPLEMENTED")
}
