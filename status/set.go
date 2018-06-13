package status

import (
	"os"
	"strings"

	"gerrit.instructure.com/ddb-sync/tty_table"

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
	return []string{"Table", "Describe", "Backfill", "Stream", "WCU Rate"}
}

func (s *Set) Display() []string {
	s.UpdateViewport()
	renderer.MaxWidth = s.ViewportWidth

	table := tty_table.Table{
		Headers: s.Header(),
		Cells:   s.statusRows(),
	}

	return append([]string{s.Delimiter()}, renderer.Render(&table)...)
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
