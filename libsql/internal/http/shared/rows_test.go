package shared

import (
	"database/sql/driver"
	"io"
	"testing"
)

// Mock implementation of rowsProvider for testing
type mockRowsProvider struct {
	sets []mockResultSet
}

type mockResultSet struct {
	columns  []string
	rows     [][]driver.Value
	hasError bool
	errorMsg string
	hasRes   bool
}

func (m *mockRowsProvider) SetsCount() int {
	return len(m.sets)
}

func (m *mockRowsProvider) RowsCount(setIdx int) int {
	if setIdx >= len(m.sets) {
		return 0
	}
	return len(m.sets[setIdx].rows)
}

func (m *mockRowsProvider) Columns(setIdx int) []string {
	if setIdx >= len(m.sets) {
		return nil
	}
	return m.sets[setIdx].columns
}

func (m *mockRowsProvider) FieldValue(setIdx, rowIdx, columnIdx int) driver.Value {
	if setIdx >= len(m.sets) || rowIdx >= len(m.sets[setIdx].rows) {
		return nil
	}
	return m.sets[setIdx].rows[rowIdx][columnIdx]
}

func (m *mockRowsProvider) Error(setIdx int) string {
	if setIdx >= len(m.sets) {
		return ""
	}
	return m.sets[setIdx].errorMsg
}

func (m *mockRowsProvider) HasResult(setIdx int) bool {
	if setIdx >= len(m.sets) {
		return false
	}
	return m.sets[setIdx].hasRes
}

func TestRows_SingleResultSet(t *testing.T) {
	provider := &mockRowsProvider{
		sets: []mockResultSet{
			{
				columns: []string{"id", "name"},
				rows: [][]driver.Value{
					{int64(1), "Alice"},
					{int64(2), "Bob"},
				},
				hasRes: true,
			},
		},
	}

	rows := NewRows(provider)

	// Check columns
	cols := rows.Columns()
	if len(cols) != 2 {
		t.Errorf("expected 2 columns, got %d", len(cols))
	}
	if cols[0] != "id" || cols[1] != "name" {
		t.Errorf("unexpected columns: %v", cols)
	}

	// Read first row
	dest := make([]driver.Value, 2)
	err := rows.Next(dest)
	if err != nil {
		t.Fatalf("unexpected error on first row: %v", err)
	}
	if dest[0] != int64(1) || dest[1] != "Alice" {
		t.Errorf("unexpected first row: %v", dest)
	}

	// Read second row
	err = rows.Next(dest)
	if err != nil {
		t.Fatalf("unexpected error on second row: %v", err)
	}
	if dest[0] != int64(2) || dest[1] != "Bob" {
		t.Errorf("unexpected second row: %v", dest)
	}

	// Should return EOF when no more rows
	err = rows.Next(dest)
	if err != io.EOF {
		t.Errorf("expected EOF, got %v", err)
	}

	// Close should not error
	err = rows.Close()
	if err != nil {
		t.Errorf("unexpected error on close: %v", err)
	}
}

func TestRows_EmptyResultSet(t *testing.T) {
	provider := &mockRowsProvider{
		sets: []mockResultSet{
			{
				columns: []string{"id", "name"},
				rows:    [][]driver.Value{},
				hasRes:  true,
			},
		},
	}

	rows := NewRows(provider)
	dest := make([]driver.Value, 2)

	// Should immediately return EOF
	err := rows.Next(dest)
	if err != io.EOF {
		t.Errorf("expected EOF for empty result set, got %v", err)
	}
}

func TestRows_MultipleResultSets(t *testing.T) {
	provider := &mockRowsProvider{
		sets: []mockResultSet{
			{
				columns: []string{"id"},
				rows: [][]driver.Value{
					{int64(1)},
				},
				hasRes: true,
			},
			{
				columns: []string{"name"},
				rows: [][]driver.Value{
					{"Alice"},
				},
				hasRes: true,
			},
		},
	}

	rows := NewRows(provider)

	// First result set
	dest := make([]driver.Value, 1)
	err := rows.Next(dest)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dest[0] != int64(1) {
		t.Errorf("expected 1, got %v", dest[0])
	}

	// Check if there's a next result set
	if !rows.(driver.RowsNextResultSet).HasNextResultSet() {
		t.Errorf("expected to have next result set")
	}

	// Move to next result set
	err = rows.(driver.RowsNextResultSet).NextResultSet()
	if err != nil {
		t.Fatalf("unexpected error moving to next result set: %v", err)
	}

	// Read from second result set
	err = rows.Next(dest)
	if err != nil {
		t.Fatalf("unexpected error reading from second result set: %v", err)
	}
	if dest[0] != "Alice" {
		t.Errorf("expected 'Alice', got %v", dest[0])
	}

	// Should not have more result sets
	if rows.(driver.RowsNextResultSet).HasNextResultSet() {
		t.Errorf("should not have more result sets")
	}

	// Moving to next when there isn't one should return EOF
	err = rows.(driver.RowsNextResultSet).NextResultSet()
	if err != io.EOF {
		t.Errorf("expected EOF when no more result sets, got %v", err)
	}
}

func TestRows_NextResultSet_WithError(t *testing.T) {
	provider := &mockRowsProvider{
		sets: []mockResultSet{
			{
				columns: []string{"id"},
				rows:    [][]driver.Value{{int64(1)}},
				hasRes:  true,
			},
			{
				columns:  []string{"name"},
				rows:     [][]driver.Value{},
				hasRes:   false,
				errorMsg: "some error occurred",
			},
		},
	}

	rows := NewRows(provider)

	// Read from first result set
	dest := make([]driver.Value, 1)
	err := rows.Next(dest)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Move to next result set which has an error
	err = rows.(driver.RowsNextResultSet).NextResultSet()
	if err == nil {
		t.Errorf("expected error when moving to result set with error")
	}
}

func TestRows_NextResultSet_NoResults(t *testing.T) {
	provider := &mockRowsProvider{
		sets: []mockResultSet{
			{
				columns: []string{"id"},
				rows:    [][]driver.Value{{int64(1)}},
				hasRes:  true,
			},
			{
				columns:  []string{"name"},
				rows:     [][]driver.Value{},
				hasRes:   false,
				errorMsg: "",
			},
		},
	}

	rows := NewRows(provider)

	// Read from first result set
	dest := make([]driver.Value, 1)
	err := rows.Next(dest)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Move to next result set which has no results
	err = rows.(driver.RowsNextResultSet).NextResultSet()
	if err == nil {
		t.Errorf("expected error when moving to result set with no results")
	}
}

func TestRows_MultipleIterations(t *testing.T) {
	provider := &mockRowsProvider{
		sets: []mockResultSet{
			{
				columns: []string{"value"},
				rows: [][]driver.Value{
					{1},
					{2},
					{3},
					{4},
					{5},
				},
				hasRes: true,
			},
		},
	}

	rows := NewRows(provider)
	dest := make([]driver.Value, 1)

	// Read all rows
	for i := 1; i <= 5; i++ {
		err := rows.Next(dest)
		if err != nil {
			t.Fatalf("unexpected error at row %d: %v", i, err)
		}
		if dest[0] != i {
			t.Errorf("expected %d, got %v", i, dest[0])
		}
	}

	// Should get EOF after all rows
	err := rows.Next(dest)
	if err != io.EOF {
		t.Errorf("expected EOF after all rows, got %v", err)
	}
}
