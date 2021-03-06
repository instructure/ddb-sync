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

package config

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	yaml "gopkg.in/yaml.v2"
)

const MaxRetries int = 15

var (
	ErrInputRegionRequired    = errors.New("Input region is required")
	ErrInputTableNameRequired = errors.New("Input table name is required")

	ErrOutputRegionRequired    = errors.New("Output region is required")
	ErrOutputTableNameRequired = errors.New("Output table name is required")

	ErrInputAndOutputTablesCannotMatch = errors.New("Input and output tables cannot match")

	ErrBackfillSegmentConfiguration       = errors.New("Backfill segment configuration is invalid")
	ErrBackfillTotalSegmentsConfiguration = errors.New("Backfill total segments configuration is invalid")
	ErrStreamCannotRunWithSegmentedScan   = errors.New("Stream must be disabled if scan segment target is specified")
)

type PlanConfig struct {
	Plan []OperationPlan `yaml:"plan"`
}

type Input struct {
	Region    string `yaml:"region"`
	TableName string `yaml:"table"`

	RoleARN string `yaml:"role_arn"`
}

type Output struct {
	Region    string `yaml:"region"` // defaults to the Input region
	TableName string `yaml:"table"`  // defaults to the Input table name

	RoleARN string `yaml:"role_arn"`
}

type Backfill struct {
	Disabled      bool  `yaml:"disabled"`
	Segments      []int `yaml:"segments"`
	TotalSegments int   `yaml:"total_segments"`
}

type Stream struct {
	Disabled bool `yaml:"disabled"`
}

type OperationPlan struct {
	Input Input `yaml:"input"`

	Output Output `yaml:"output"`

	Backfill Backfill `yaml:"backfill"`

	Stream Stream `yaml:"stream"`
}

func (p OperationPlan) WithDefaults() OperationPlan {
	newPlan := p

	if newPlan.Output.TableName == "" {
		newPlan.Output.TableName = newPlan.Input.TableName
	}

	if newPlan.Output.Region == "" {
		newPlan.Output.Region = newPlan.Input.Region
	}

	return newPlan
}

// Description returns a description of the operation input/output
// "InputTableName => OutputTableName:"
func (p OperationPlan) Description() string {
	return fmt.Sprintf("[%s] ⇨ [%s]", p.Input.TableName, p.Output.TableName)
}

func (p OperationPlan) Validate() error {
	defaultRegion := ""
	defaultConfig := defaults.Get().Config
	if defaultConfig.Region != nil {
		defaultRegion = *defaultConfig.Region
	}

	if p.Input.Region == "" && defaultRegion == "" {
		return ErrInputRegionRequired
	} else if p.Input.TableName == "" {
		return ErrInputTableNameRequired
	}

	if p.Output.Region == "" && defaultRegion == "" {
		return ErrOutputRegionRequired
	} else if p.Output.TableName == "" {
		return ErrOutputTableNameRequired
	}

	err := p.validateBackfillSegments()
	if err != nil {
		return err
	}

	if p.Input.Region != p.Output.Region || p.Input.TableName != p.Output.TableName || p.Input.RoleARN != p.Output.RoleARN {
		return nil
	}
	return ErrInputAndOutputTablesCannotMatch
}

func (p OperationPlan) GetSessions() (*session.Session, *session.Session, error) {
	// Base config & session (used for STS calls)
	baseConfig := aws.NewConfig().WithRegion(p.Input.Region).WithMaxRetries(MaxRetries)
	baseSession, err := session.NewSession(baseConfig)
	if err != nil {
		return nil, nil, err
	}

	// Input config, session, & client (used for input-side DynamoDB calls)
	inputConfig := baseConfig.Copy()
	if p.Input.RoleARN != "" {
		inputConfig.WithCredentials(stscreds.NewCredentials(baseSession, p.Input.RoleARN))
	}
	inputSession, err := session.NewSession(inputConfig)
	if err != nil {
		return nil, nil, err
	}

	// Output config, session, & client (used for output-side DynamoDB calls)
	outputConfig := baseConfig.Copy().WithRegion(p.Output.Region)
	if p.Output.RoleARN != "" {
		outputConfig.WithCredentials(stscreds.NewCredentials(baseSession, p.Output.RoleARN))
	}
	outputSession, err := session.NewSession(outputConfig)
	if err != nil {
		return nil, nil, err
	}
	return inputSession, outputSession, nil
}

func ParseConfigFile(filePath string) ([]OperationPlan, error) {
	var f io.Reader
	var err error

	if filePath == "-" {
		f = os.Stdin
	} else {
		fp, err := os.Open(filePath)
		if err != nil {
			return nil, fmt.Errorf("Failed to open configuration file: %v", err)
		}
		defer fp.Close()
		f = fp
	}

	var config PlanConfig

	decoder := yaml.NewDecoder(f)
	decoder.SetStrict(true)
	err = decoder.Decode(&config)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse configuration file: %v", err)
	}
	return config.Plan, nil
}

func (p OperationPlan) validateBackfillSegments() error {
	if p.Backfill.Disabled == true {
		return nil
	}

	// That both backfill segment and backfill total segment were configured is already validated
	if p.Backfill.TotalSegments != 0 {
		if p.Backfill.TotalSegments < 1 {
			return ErrBackfillTotalSegmentsConfiguration
		}

		// Must be specified to require configuration
		if len(p.Backfill.Segments) > 0 {
			min, max := segmentBounds(p.Backfill.Segments)
			if min < 0 {
				return ErrBackfillSegmentConfiguration
			}

			if max >= p.Backfill.TotalSegments {
				// Cannot be greater than or equal to TotalSegments
				return ErrBackfillSegmentConfiguration
			}

			// Stream must be disabled as guarding ordering across distributed segment scans is difficult to message
			if p.Stream.Disabled != true {
				return ErrStreamCannotRunWithSegmentedScan
			}
		}
	}

	return nil
}
func segmentBounds(vals []int) (int, int) {
	// Generate some bounds
	min := 0
	max := 0
	for _, v := range vals {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}
