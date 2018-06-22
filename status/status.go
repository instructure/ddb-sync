package status

import (
	"fmt"
	"math"

	"gerrit.instructure.com/ddb-sync/config"
)

// BufferStates are single character "fullness" indicators of the buffer
var BufferStates = []string{
	"○",
	"◔",
	"◑",
	"◕",
	"●",
}

type Status struct {
	Plan config.OperationPlan

	Description string
	Backfill    string
	Stream      string
	Rate        string

	output []string
}

func New(plan config.OperationPlan) *Status {
	return &Status{
		Plan:        plan,
		Description: "  --  ",
		Backfill:    "  --  ",
		Stream:      "  --  ",
	}
}

func BufferStatus(percentage float64) string {
	// We need to grab the closest bar char
	if percentage > 1.0 {
		percentage = 1.0
	} else if percentage < 0.0 {
		percentage = 0.0
	}
	selection := int(math.Ceil(float64(len(BufferStates)-1) * percentage))
	return fmt.Sprintf("⇨ %s ⇨", BufferStates[selection])
}

func (s *Status) Display() []string {
	// Clear the last output set
	s.output = []string{}

	s.addContent(s.formatTableDescription())
	s.addContent(s.Description)
	s.addContent(s.Backfill)
	s.addContent(s.Stream)
	s.addContent(s.Rate)
	return s.output
}

func (s *Status) formatTableDescription() string {
	return fmt.Sprintf("⇨ [%s]", s.Plan.Output.TableName)
}

func (s *Status) addContent(str string) {
	s.output = append(s.output, str)
}

// SetWaiting update description to indicate waiting status
func (s *Status) SetWaiting() {
	s.Description = "Waiting..."
}

// SetNoop update description to indicate Noop
func (s *Status) SetNoop() {
	s.Description = "Nothing to do"
}

// SetError update description to indicate error status
func (s *Status) SetError() {
	s.Description = "Error!"
}
