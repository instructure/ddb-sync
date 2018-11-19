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
	"os"
	"strings"
	"time"

	"github.com/instructure/ddb-sync/tty_table"

	"golang.org/x/crypto/ssh/terminal"
)

const (
	statusDelimiter       = " Current Status "
	halfMaxDelimiterWidth = 60
)

var renderer = tty_table.Renderer{
	ColumnSeparator: "  ",
	MaxWidth:        halfMaxDelimiterWidth * 2,
}

type Set struct {
	Statuses      []*Status
	ViewportWidth int
}

func NewBlankSet() *Set {
	return &Set{}
}

func NewSet(statuses []*Status) *Set {
	return &Set{
		Statuses: statuses,
	}
}

func (s *Set) UpdateViewport() {
	if s != nil {
		s.ViewportWidth, _, _ = terminal.GetSize(int(os.Stdin.Fd()))
	}
}

// Delimiter returns " Current Status " centered in a viewport wide list of ---- markers
func (s *Set) Delimiter() string {
	if s == nil {
		return ""
	}

	div := (s.ViewportWidth-len(statusDelimiter))/2 - 1
	if div > halfMaxDelimiterWidth {
		div = halfMaxDelimiterWidth
	}

	return strings.Repeat("-", div) + statusDelimiter + strings.Repeat("-", div)
}

func (s *Set) Header() []string {
	return []string{"TABLE", "DETAILS", "BACKFILL", "STREAM", "RATES & BUFFER"}
}

func (s *Set) Display() []string {
	s.UpdateViewport()
	renderer.MaxWidth = s.ViewportWidth

	table := tty_table.Table{
		Headers: s.Header(),
		Cells:   s.statusRows(),
	}

	return append([]string{"", s.Delimiter()}, renderer.Render(&table)...)
}

func (s *Set) ToFile() []string {
	renderer.MaxWidth = 200
	table := tty_table.Table{
		Headers: s.Header(),
		Cells:   s.statusRows(),
	}

	return append([]string{"", time.Now().Format("2006-01-02 15:04:05")}, renderer.Render(&table)...)
}

func (s *Set) statusRows() [][]string {
	output := [][]string{}
	if s == nil {
		return output
	}

	for _, status := range s.Statuses {
		output = append(output, status.Display())
	}
	return output
}
