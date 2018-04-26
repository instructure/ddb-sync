package plan

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws/defaults"
)

var (
	ErrInputRegionRequired    = errors.New("Input region is required")
	ErrInputTableNameRequired = errors.New("Input table name is required")

	ErrOutputRegionRequired    = errors.New("Output region is required")
	ErrOutputTableNameRequired = errors.New("Output table name is required")

	ErrInputAndOutputTablesCannotMatch = errors.New("Input and output tables cannot match")
)

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

type Plan struct {
	Input Input `yaml:"input"`

	Output Output `yaml:"output"`

	Backfill Backfill `yaml:"backfill"`

	Stream Stream `yaml:"stream"`
}

func (p Plan) WithDefaults() Plan {
	newPlan := p

	if newPlan.Output.TableName == "" {
		newPlan.Output.TableName = newPlan.Input.TableName
	}

	if newPlan.Output.Region == "" {
		newPlan.Output.Region = newPlan.Input.Region
	}

	return newPlan
}

func (p Plan) Validate() error {
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
