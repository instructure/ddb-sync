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

package tty_table_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/instructure/ddb-sync/tty_table"
)

type RenderTestCase struct {
	Title          string
	Table          tty_table.Table
	Renderer       *tty_table.Renderer
	ExpectedRender []string
}

var (
	DefaultRenderer = &tty_table.Renderer{
		ColumnSeparator: "|",
	}
)

func TestRenderer(t *testing.T) {
	testCases := []RenderTestCase{
		{
			Title: "Default column separator",
			Table: tty_table.Table{
				Headers: []string{"A", "SET", "OF", "HEADERS"},
				Cells: [][]string{
					{"THIS", "IS", "A", "ROW"},
					{"EVEN", "THE", "BEST", "FALL"},
				},
			},
			Renderer: &tty_table.Renderer{},
			ExpectedRender: []string{
				"A    SET OF   HEADERS",
				"THIS IS  A    ROW",
				"EVEN THE BEST FALL",
			},
		},
		{
			Title: "Varying lengths between headers and columns",
			Table: tty_table.Table{
				Headers: []string{"A", "SET", "OF", "HEADERS"},
				Cells: [][]string{
					{"THIS", "IS", "A", "ROW"},
					{"EVEN", "THE", "BEST", "FALL"},
				},
			},
			ExpectedRender: []string{
				"A   |SET|OF  |HEADERS",
				"THIS|IS |A   |ROW",
				"EVEN|THE|BEST|FALL",
			},
		},
		{
			Title: "Headers with spaces",
			Table: tty_table.Table{
				Headers: []string{"ONE OR THREE", "COLUMNS WITH", "SPACES IN THEM"},
				Cells:   [][]string{{"ONE", "TWO", "THREE"}},
			},
			ExpectedRender: []string{
				"ONE OR THREE|COLUMNS WITH|SPACES IN THEM",
				"ONE         |TWO         |THREE",
			},
		},
		{
			Title: "Cells with spaces",
			Table: tty_table.Table{
				Headers: []string{"ONE", "TWO", "THREE"},
				Cells:   [][]string{{"ONE OR THREE", "CELLS WITH", "SPACES IN THEM"}},
			},
			ExpectedRender: []string{
				"ONE         |TWO       |THREE",
				"ONE OR THREE|CELLS WITH|SPACES IN THEM",
			},
		},
		{
			Title: "Fewer headers than cells (final header should not be padded)",
			Table: tty_table.Table{
				Headers: []string{"ONE", "TWO"},
				Cells: [][]string{
					{"ONE", "THE", "THREE"},
					{"ONE", "OTHER", "THREE"},
					{"ONE", "THINGS", "THREE"},
				},
			},
			ExpectedRender: []string{
				"ONE|TWO",
				"ONE|THE   |THREE",
				"ONE|OTHER |THREE",
				"ONE|THINGS|THREE",
			},
		},
		{
			Title: "More headers than cells (final cells should not be padded)",
			Table: tty_table.Table{
				Headers: []string{"ONE", "TWO", "LOOOOOOOOOOONG"},
				Cells: [][]string{
					{"ONE", "THE"},
					{"ONE", "OTHER"},
					{"ONE", "THINGS"},
				},
			},
			ExpectedRender: []string{
				"ONE|TWO   |LOOOOOOOOOOONG",
				"ONE|THE",
				"ONE|OTHER",
				"ONE|THINGS",
			},
		},
		{
			Title: "Final header is not padded",
			Table: tty_table.Table{
				Headers: []string{"ONE", "TWO", "THREE"},
				Cells:   [][]string{{"ONE", "TWO", "LOOOOOOOOOOONG"}},
			},
			ExpectedRender: []string{
				"ONE|TWO|THREE",
				"ONE|TWO|LOOOOOOOOOOONG",
			},
		},
		{
			Title: "Final cells are not padded",
			Table: tty_table.Table{
				Headers: []string{"ONE", "TWO", "LOOOOOOOOOOONG"},
				Cells: [][]string{
					{"ONE", "TWO", "THE"},
					{"ONE", "TWO", "OTHER"},
					{"ONE", "TWO", "THINGS"},
				},
			},
			ExpectedRender: []string{
				"ONE|TWO|LOOOOOOOOOOONG",
				"ONE|TWO|THE",
				"ONE|TWO|OTHER",
				"ONE|TWO|THINGS",
			},
		},
		{
			Title: "Extra cells are padded (beyond the headers)",
			Table: tty_table.Table{
				Headers: []string{"ONE", "TWO"},
				Cells: [][]string{
					{"ONE", "TWO", "THE", "LONG", "THINGS"},
					{"ONE", "TWO", "OTHER", "THINGS"},
					{"ONE", "TWO", "THINGS"},
				},
			},
			ExpectedRender: []string{
				"ONE|TWO",
				"ONE|TWO|THE   |LONG  |THINGS",
				"ONE|TWO|OTHER |THINGS",
				"ONE|TWO|THINGS",
			},
		},
		{
			Title: "Unicode headers & columns",
			Table: tty_table.Table{
				Headers: []string{"DÎACRÎTÎCS", "TWO"},
				Cells:   [][]string{{"ONE", "ümläut"}},
			},
			ExpectedRender: []string{
				"DÎACRÎTÎCS|TWO",
				"ONE       |ümläut",
			},
		},
		{
			Title: "Eliding everything",
			Table: tty_table.Table{
				Headers: []string{"A", "SET", "OF", "HEADERS"},
				Cells: [][]string{
					{"THIS", "IS", "A", "ROW"},
					{"EVEN", "THE", "BEST", "FALL"},
				},
			},
			Renderer: &tty_table.Renderer{
				ColumnSeparator: "|",
				MaxWidth:        13,
			},
			ExpectedRender: []string{
				"A   |SET|OF …",
				"THIS|IS |A  …",
				"EVEN|THE|BES…",
			},
		},
		{
			Title: "Partial eliding",
			Table: tty_table.Table{
				Headers: []string{"A", "SET", "OF", "HEADERS"},
				Cells: [][]string{
					{"THIS", "IS", "A", "ROW"},
					{"EVEN", "THE", "BEST", "FALL"},
				},
			},
			Renderer: &tty_table.Renderer{
				ColumnSeparator: "|",
				MaxWidth:        17,
			},
			ExpectedRender: []string{
				"A   |SET|OF  |HE…",
				"THIS|IS |A   |ROW",
				"EVEN|THE|BEST|FA…",
			},
		},
		{
			Title: "No eliding",
			Table: tty_table.Table{
				Headers: []string{"A", "SET", "OF", "HEADERS"},
				Cells: [][]string{
					{"THIS", "IS", "A", "ROW"},
					{"EVEN", "THE", "BEST", "FALL"},
				},
			},
			Renderer: &tty_table.Renderer{
				ColumnSeparator: "|",
				MaxWidth:        21,
			},
			ExpectedRender: []string{
				"A   |SET|OF  |HEADERS",
				"THIS|IS |A   |ROW",
				"EVEN|THE|BEST|FALL",
			},
		},
	}

	for _, testCase := range testCases {
		originalHeaders := make([]string, len(testCase.Table.Headers))
		copy(originalHeaders, testCase.Table.Headers)

		originalCells := make([][]string, len(testCase.Table.Cells))
		for i, testCaseRow := range testCase.Table.Cells {
			originalRow := make([]string, len(testCaseRow))
			copy(originalRow, testCaseRow)
			originalCells[i] = originalRow
		}

		renderer := testCase.Renderer
		if renderer == nil {
			renderer = DefaultRenderer
		}

		goodSoFar := true
		actualRender := renderer.Render(&testCase.Table)
		if len(actualRender) != len(testCase.ExpectedRender) {
			t.Errorf("[%s] Incorrect number of rows rendered. Actual: %d; Expected: %d", testCase.Title, len(actualRender), len(testCase.ExpectedRender))
			goodSoFar = false
		}

		for i := 0; i < len(actualRender) && i < len(testCase.ExpectedRender); i++ {
			if actualRender[i] != testCase.ExpectedRender[i] {
				t.Errorf("[%s] Row #%d rendered incorrectly. Actual: %q; Expected %q", testCase.Title, i, actualRender[i], testCase.ExpectedRender[i])
				goodSoFar = false
			}
		}

		// mutated ColumnSeparator
		if goodSoFar && renderer == DefaultRenderer {
			sepTable := testCase.Table
			sepRenderer := tty_table.Renderer{
				ColumnSeparator: "< = >",
			}

			sepActualRender := sepRenderer.Render(&sepTable)
			if len(sepActualRender) != len(testCase.ExpectedRender) {
				t.Errorf("[%s] Incorrect number of (sep) rows rendered. Actual: %d; Expected: %d", testCase.Title, len(sepActualRender), len(testCase.ExpectedRender))
			}

			for i := 0; i < len(sepActualRender) && i < len(testCase.ExpectedRender); i++ {
				expected := strings.Replace(testCase.ExpectedRender[i], "|", "< = >", -1)
				if sepActualRender[i] != expected {
					t.Errorf("[%s] (sep) Row #%d rendered incorrectly. Actual: %q; Expected %q", testCase.Title, i, sepActualRender[i], expected)
				}
			}
		}

		// INVARIANTS
		if !reflect.DeepEqual(testCase.Table.Headers, originalHeaders) {
			t.Errorf("[%s] INVARIANT VIOLATION! Headers have been altered. (%#v)", testCase.Title, testCase.Table.Headers)
		}

		if !reflect.DeepEqual(testCase.Table.Cells, originalCells) {
			t.Errorf("[%s] INVARIANT VIOLATION! Cells have been altered. (%#v)", testCase.Title, testCase.Table.Cells)
		}
	}
}
