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
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
)

// QuickCheckForActiveCredentials is a fast and dirty check, if there aren't
// active credentials and this didn't figure it out, that is still captured by
// subsequent preflight checks that take longer. We are optimizing for
// "fail fast" conditions, like not having any credentials at all.
func quickCheckForActiveCredentials(ctx context.Context) error {
	client := &http.Client{Timeout: 1 * time.Second}
	session, err := session.NewSession(
		aws.NewConfig().
			WithMaxRetries(1).
			WithHTTPClient(client),
	)
	if err != nil {
		return err
	}

	svc := sts.New(session)
	result, err := svc.GetCallerIdentityWithContext(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return err
	}

	if *result.Account == "" {
		return fmt.Errorf("No active AWS credentials")
	}
	return nil
}
