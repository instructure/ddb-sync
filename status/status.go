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

package status

import (
	"fmt"

	"github.com/instructure/ddb-sync/config"
)

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
