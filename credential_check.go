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
