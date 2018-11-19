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
	"testing"

	"github.com/instructure/ddb-sync/tty_table"
)

type ColumnWidthsTestCase struct {
	Title          string
	Table          tty_table.Table
	ExpectedWidths []int
}

func TestColumnWidths(t *testing.T) {
	testCases := []ColumnWidthsTestCase{
		{
			Title: "Thin headers, wide columns",
			Table: tty_table.Table{
				Headers: []string{"T", "H", "I", "N"},
				Cells:   [][]string{{"A", "WIDE", "CELL'S", "HERE"}},
			},
			ExpectedWidths: []int{1, 4, 6, 4},
		},
		{
			Title: "Wide headers, thin columns",
			Table: tty_table.Table{
				Headers: []string{"WIDE", "HEADERS"},
				Cells:   [][]string{{"THIN", "CELLS"}},
			},
			ExpectedWidths: []int{4, 7},
		},
		{
			Title: "More headers than cell columns",
			Table: tty_table.Table{
				Headers: []string{"MORE", "HEADERS", "THAN", "ROW", "CELL", "COLUMNS"},
				Cells: [][]string{
					{"ROW", "CELL", "COLUMNS"},
					{"ANOTHER", "ROW"},
				},
			},
			ExpectedWidths: []int{7, 7, 7, 3, 4, 7},
		},
		{
			Title: "More cell columns than headers",
			Table: tty_table.Table{
				Headers: []string{"FEW", "HEADERS"},
				Cells: [][]string{
					{"LOTS", "OF", "CELLS"},
					{"OF", "VARYING", "ROW", "LENGTHS"},
				},
			},
			ExpectedWidths: []int{4, 7, 5, 7},
		},
		{
			Title: "Unicode headers & columns",
			Table: tty_table.Table{
				Headers: []string{"DÎACRÎTÎCS", "TWO"},
				Cells:   [][]string{{"ONE", "ümläut"}},
			},
			ExpectedWidths: []int{10, 6},
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

		actualWidths := testCase.Table.ColumnWidths()
		if !reflect.DeepEqual(actualWidths, testCase.ExpectedWidths) {
			t.Errorf("[%s] Incorrect column widths. Actual: %v; Expected: %v", testCase.Title, actualWidths, testCase.ExpectedWidths)
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
