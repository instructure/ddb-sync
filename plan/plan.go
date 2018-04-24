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
	Region    string
	TableName string

	RoleARN string
}

type Output struct {
	Region    string // defaults to the Input region
	TableName string // defaults to the Input table name

	RoleARN string
}

type Backfill struct {
	Disabled bool
}

type Stream struct {
	Disabled bool
}

type Plan struct {
	Input Input

	Output Output

	Backfill Backfill

	Stream Stream
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
