package config

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go/aws/defaults"
	"gopkg.in/yaml.v2"
)

var (
	ErrInputRegionRequired    = errors.New("Input region is required")
	ErrInputTableNameRequired = errors.New("Input table name is required")

	ErrOutputRegionRequired    = errors.New("Output region is required")
	ErrOutputTableNameRequired = errors.New("Output table name is required")

	ErrInputAndOutputTablesCannotMatch = errors.New("Input and output tables cannot match")
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
	Disabled bool `yaml:"disabled"`
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

	if p.Input.Region != p.Output.Region || p.Input.TableName != p.Output.TableName || p.Input.RoleARN != p.Output.RoleARN {
		return nil
	} else {
		return ErrInputAndOutputTablesCannotMatch
	}
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
