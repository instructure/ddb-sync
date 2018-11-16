package operations

import (
	"context"

	"github.com/instructure/ddb-sync/log"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

type ErrorCollator struct {
	Funcs  []func() error
	Cancel func()
}

func (c *ErrorCollator) Register(f func() error) {
	c.Funcs = append(c.Funcs, f)
}

func (c *ErrorCollator) Run() error {
	errs := make(chan error)
	defer close(errs)

	for _, f := range c.Funcs {
		go func(f func() error) {
			errs <- f()
		}(f)
	}

	var finalError error
	for range c.Funcs {
		err := <-errs
		err = RequestCanceledCheck(err)

		switch err {
		case nil:
			// :D
		case context.Canceled:
			if finalError == nil {
				finalError = context.Canceled
			}
		default:
			if c.Cancel != nil {
				c.Cancel()
			}

			if err != ErrOperationFailed {
				log.Printf("[ERROR] %v\n", err)
			}
			finalError = ErrOperationFailed
		}
	}

	return finalError
}

func RequestCanceledCheck(err error) error {
	if awsErr, ok := err.(awserr.Error); ok {
		if awsErr.Code() == "RequestCanceled" {
			return context.Canceled
		}
	}

	return err
}
