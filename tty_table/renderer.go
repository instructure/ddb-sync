package tty_table

import (
	"strings"
)

// Maintains the options used to render a Table.
type Renderer struct {
	// The separator used to delineate columns in rendered output.
	//
	// Defaults to a single space character (" ").
	ColumnSeparator string

	// The maximum width allowed for any rendered line.
	//
	// Lines that exceed the maximum width are truncated. The last character is
	// then replaced with an ellipsis character ("…").
	MaxWidth int
}

// Renders the given Table headers and cells into column-aligned lines.
//
// With one exception, all columns are right-padded with space characters. The
// final column is not padded to promote better display in terminals.
//
// Rendered lines are elided (…) if they exceed the max width setting.
func (r *Renderer) Render(table *Table) []string {
	columnSeparator := r.ColumnSeparator
	if len(columnSeparator) == 0 {
		columnSeparator = " "
	}

	widths := newTableWidths(table)
	tableColumnWidths := widths.columnWidths()

	paddedTableCells := r.padTableCells(table, widths, tableColumnWidths)
	renderedRows := make([]string, len(paddedTableCells))
	for rowIdx, paddedRowCells := range paddedTableCells {
		renderedRows[rowIdx] = strings.Join(paddedRowCells, columnSeparator)
	}

	if r.MaxWidth > 0 {
		for rowIdx, renderedRow := range renderedRows {
			rowRunes := []rune(renderedRow)
			if len(rowRunes) > r.MaxWidth {
				renderedRows[rowIdx] = string(rowRunes[:r.MaxWidth-1]) + "…"
			}
		}
	}

	return renderedRows
}

func (r *Renderer) padTableCells(table *Table, widths *tableWidths, tableColumnWidths []int) [][]string {
	renderedTable := make([][]string, 0, 1+len(table.Cells))
	renderedTable = append(renderedTable, padRowCells(table.Headers, widths.Headers, tableColumnWidths))
	for rowIdx, row := range table.Cells {
		renderedTable = append(renderedTable, padRowCells(row, widths.Cells[rowIdx], tableColumnWidths))
	}

	return renderedTable
}

func padRowCells(row []string, rowCellWidths []int, tableColumnWidths []int) []string {
	paddedColumns := make([]string, len(row))
	for cellIdx, cell := range row {
		if cellIdx < len(row)-1 {
			cellLen := rowCellWidths[cellIdx]
			if tableColumnWidths[cellIdx] < cellLen {
				panic("INVARIANT VIOLATION! Invalid column width!")
			}

			cell += strings.Repeat(" ", tableColumnWidths[cellIdx]-cellLen)
		}
		paddedColumns[cellIdx] = cell
	}

	return paddedColumns
}
