package main

import (
	"errors"
)

type StreamOperation struct {
}

type StreamRecord struct{} // TODO: REPLACE W/REAL RECORD

func (o *StreamOperation) Run() error {
	c := make(chan StreamRecord)

	go o.readStream(c) // TODO: FANOUT?
	o.batchWrite(c)    // TODO: FANOUT?

	return errors.New("NOT IMPLEMENTED")
}

func (o *StreamOperation) Stop() {
	// TODO: STOP THE STREAM OPERATION
}

func (o *StreamOperation) Status() string {
	// TODO: RETURN THE CURRENT STATUS
	return "NOT IMPLEMENTED"
}

func (o *StreamOperation) readStream(c chan StreamRecord) {
	defer close(c)

	// TODO: READ ALL SHARDS IN THE STREAM
}

func (o *StreamOperation) batchWrite(c chan StreamRecord) {
	for _ = range c {
		// TODO: BATCH AND WRITE ALL RECORDS (probably with a select & timer)
	}
}
