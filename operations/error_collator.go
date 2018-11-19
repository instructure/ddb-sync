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
