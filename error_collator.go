package main

import (
	"context"

	"gerrit.instructure.com/ddb-sync/log"
)

type ErrorCollator struct {
	Funcs []func() error
}

func (c *ErrorCollator) Register(f func() error) {
	c.Funcs = append(c.Funcs, f)
}

func (c *ErrorCollator) Run() error {
	errs := make(chan error)
	for i := range c.Funcs {
		f := c.Funcs[i]
		go func() {
			errs <- f()
		}()
	}

	var finalError error
	for range c.Funcs {
		switch err := <-errs; err {
		case nil:
			// :D
		case context.Canceled:
			if finalError == nil {
				finalError = context.Canceled
			}
		default:
			if err != ErrOperationFailed {
				log.Printf("[ERROR] %v\n", err)
			}
			finalError = ErrOperationFailed
		}
	}

	return finalError
}
