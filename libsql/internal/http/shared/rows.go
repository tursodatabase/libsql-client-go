package shared

import (
	"database/sql/driver"
	"fmt"
	"io"
)

type rowsProvider interface {
	SetsCount() int
	RowsCount(setIdx int) int
	Columns(setIdx int) []string
	FieldValue(setIdx, rowIdx int, columnIdx int) driver.Value
	Error(setIdx int) string
	HasResult(setIdx int) bool
}

func NewRows(result rowsProvider) driver.Rows {
	return &rows{result: result}
}

type rows struct {
	result                rowsProvider
	currentResultSetIndex int
	currentRowIdx         int
}

func (r *rows) Columns() []string {
	return r.result.Columns(r.currentResultSetIndex)
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.currentRowIdx == r.result.RowsCount(r.currentResultSetIndex) {
		return io.EOF
	}
	count := len(r.result.Columns(r.currentResultSetIndex))
	for idx := 0; idx < count; idx++ {
		dest[idx] = r.result.FieldValue(r.currentResultSetIndex, r.currentRowIdx, idx)
	}
	r.currentRowIdx++
	return nil
}

func (r *rows) HasNextResultSet() bool {
	return r.currentResultSetIndex < r.result.SetsCount()-1
}

func (r *rows) NextResultSet() error {
	if !r.HasNextResultSet() {
		return io.EOF
	}

	r.currentResultSetIndex++
	r.currentRowIdx = 0

	errStr := r.result.Error(r.currentResultSetIndex)
	if errStr != "" {
		return fmt.Errorf("failed to execute statement\n%s", errStr)
	}
	if !r.result.HasResult(r.currentResultSetIndex) {
		return fmt.Errorf("no results for statement")
	}

	return nil
}
