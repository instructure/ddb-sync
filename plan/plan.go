package plan

import (
	"errors"
)

var (
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
	if p.Input.Region != p.Output.Region || p.Input.TableName != p.Output.TableName || p.Input.RoleARN != p.Output.RoleARN {
		return nil
	} else {
		return ErrInputAndOutputTablesCannotMatch
	}
}
