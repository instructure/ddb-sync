package tty_table_test

import (
	"reflect"
	"testing"

	"gerrit.instructure.com/ddb-sync/tty_table"
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