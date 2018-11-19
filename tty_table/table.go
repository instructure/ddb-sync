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

package tty_table

// Represents the current state of a table, including headers and cell data.
type Table struct {
	Headers []string
	Cells   [][]string
}

// Calculates the current maximal width for each column in the table.
//
// A column width will be included if a header or any row within the table
// includes data in the corresponding column.
func (t *Table) ColumnWidths() []int {
	return newTableWidths(t).columnWidths()
}

type tableWidths struct {
	Headers []int
	Cells   [][]int
}

func newTableWidths(t *Table) *tableWidths {
	w := &tableWidths{
		Headers: make([]int, len(t.Headers)),
		Cells:   make([][]int, len(t.Cells)),
	}

	for headerIdx, header := range t.Headers {
		w.Headers[headerIdx] = len([]rune(header))
	}

	for rowIdx, row := range t.Cells {
		rowWidths := make([]int, len(row))
		w.Cells[rowIdx] = rowWidths
		for cellIdx, cell := range row {
			rowWidths[cellIdx] = len([]rune(cell))
		}
	}

	return w
}

func (w *tableWidths) columnWidths() []int {
	// find the maximum number of columns (in headers or cell rows)
	maxColumns := len(w.Headers)
	for _, row := range w.Cells {
		if maxColumns < len(row) {
			maxColumns = len(row)
		}
	}
	columnWidths := make([]int, maxColumns)

	// check column widths
	for headerIdx, headerLen := range w.Headers {
		columnWidths[headerIdx] = headerLen
	}

	for _, row := range w.Cells {
		for cellIdx, cellLen := range row {
			if columnWidths[cellIdx] < cellLen {
				columnWidths[cellIdx] = cellLen
			}
		}
	}

	return columnWidths
}
