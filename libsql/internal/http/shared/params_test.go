package shared

import (
	"database/sql/driver"
	"encoding/json"
	"testing"
)

func TestConvertArgs_PositionalParams(t *testing.T) {
	args := []driver.NamedValue{
		{Ordinal: 1, Value: "value1"},
		{Ordinal: 2, Value: "value2"},
		{Ordinal: 3, Value: 42},
	}

	params, err := ConvertArgs(args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if params.Type() != positionalParameters {
		t.Errorf("expected positionalParameters type")
	}

	if len(params.Positional()) != 3 {
		t.Errorf("expected 3 positional params, got %d", len(params.Positional()))
	}

	if params.Positional()[0] != "value1" {
		t.Errorf("expected 'value1', got %v", params.Positional()[0])
	}
}

func TestConvertArgs_NamedParams(t *testing.T) {
	args := []driver.NamedValue{
		{Name: "param1", Ordinal: 1, Value: "value1"},
		{Name: "param2", Ordinal: 2, Value: "value2"},
	}

	params, err := ConvertArgs(args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if params.Type() != namedParameters {
		t.Errorf("expected namedParameters type")
	}

	if len(params.Named()) != 2 {
		t.Errorf("expected 2 named params, got %d", len(params.Named()))
	}

	if params.Named()["param1"] != "value1" {
		t.Errorf("expected 'value1', got %v", params.Named()["param1"])
	}
}

func TestConvertArgs_EmptyArgs(t *testing.T) {
	params, err := ConvertArgs([]driver.NamedValue{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if params.Type() != positionalParameters {
		t.Errorf("expected positionalParameters type for empty args")
	}

	if len(params.Positional()) != 0 {
		t.Errorf("expected 0 params, got %d", len(params.Positional()))
	}
}

func TestConvertArgs_MixedParamsError(t *testing.T) {
	args := []driver.NamedValue{
		{Ordinal: 1, Value: "value1"},
		{Name: "param2", Ordinal: 2, Value: "value2"},
	}

	_, err := ConvertArgs(args)
	if err == nil {
		t.Errorf("expected error when mixing named and positional params")
	}
}

func TestConvertArgs_UnsortedArgs(t *testing.T) {
	// Args provided out of order
	args := []driver.NamedValue{
		{Ordinal: 3, Value: "value3"},
		{Ordinal: 1, Value: "value1"},
		{Ordinal: 2, Value: "value2"},
	}

	params, err := ConvertArgs(args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be sorted by ordinal
	if params.Positional()[0] != "value1" {
		t.Errorf("expected 'value1' at position 0, got %v", params.Positional()[0])
	}
	if params.Positional()[1] != "value2" {
		t.Errorf("expected 'value2' at position 1, got %v", params.Positional()[1])
	}
	if params.Positional()[2] != "value3" {
		t.Errorf("expected 'value3' at position 2, got %v", params.Positional()[2])
	}
}

func TestParams_Len(t *testing.T) {
	tests := []struct {
		name     string
		params   Params
		expected int
	}{
		{
			name: "positional params",
			params: Params{
				positional: []any{"a", "b", "c"},
			},
			expected: 3,
		},
		{
			name: "named params",
			params: Params{
				named: map[string]any{"a": 1, "b": 2},
			},
			expected: 2,
		},
		{
			name:     "empty params",
			params:   Params{},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.params.Len() != tt.expected {
				t.Errorf("expected length %d, got %d", tt.expected, tt.params.Len())
			}
		})
	}
}

func TestParams_MarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		params   Params
		expected string
	}{
		{
			name: "positional params",
			params: Params{
				positional: []any{"a", "b", "c"},
			},
			expected: `["a","b","c"]`,
		},
		{
			name: "named params",
			params: Params{
				named: map[string]any{"key": "value"},
			},
			expected: `{"key":"value"}`,
		},
		{
			name:     "empty params",
			params:   Params{},
			expected: `[]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(&tt.params)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if string(data) != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, string(data))
			}
		})
	}
}

func TestNewParams(t *testing.T) {
	tests := []struct {
		name       string
		paramsType paramsType
		checkNamed bool
		checkPos   bool
	}{
		{
			name:       "named params",
			paramsType: namedParameters,
			checkNamed: true,
		},
		{
			name:       "positional params",
			paramsType: positionalParameters,
			checkPos:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := NewParams(tt.paramsType)

			if tt.checkNamed && params.named == nil {
				t.Errorf("expected named map to be initialized")
			}

			if tt.checkPos && params.positional == nil {
				t.Errorf("expected positional slice to be initialized")
			}
		})
	}
}

func TestRemoveParamPrefix(t *testing.T) {
	tests := []struct {
		name      string
		param     string
		expected  string
		expectErr bool
	}{
		{
			name:     "colon prefix",
			param:    ":param",
			expected: "param",
		},
		{
			name:     "at prefix",
			param:    "@param",
			expected: "param",
		},
		{
			name:     "dollar prefix",
			param:    "$param",
			expected: "param",
		},
		{
			name:      "no prefix",
			param:     "param",
			expectErr: true,
		},
		{
			name:      "invalid prefix",
			param:     "#param",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := removeParamPrefix(tt.param)

			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if result != tt.expected {
					t.Errorf("expected %q, got %q", tt.expected, result)
				}
			}
		})
	}
}

func TestParseStatement(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		expectedStmts int
		expectedErr   bool
	}{
		{
			name:          "single statement",
			sql:           "SELECT * FROM users",
			expectedStmts: 1,
		},
		{
			name:          "multiple statements",
			sql:           "SELECT * FROM users; SELECT * FROM posts",
			expectedStmts: 2,
		},
		{
			name:          "statement with named params",
			sql:           "SELECT * FROM users WHERE id = :id",
			expectedStmts: 1,
		},
		{
			name:          "statement with positional params",
			sql:           "SELECT * FROM users WHERE id = ?",
			expectedStmts: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, paramsInfo, err := ParseStatement(tt.sql)

			if tt.expectedErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(stmts) != tt.expectedStmts {
				t.Errorf("expected %d statements, got %d", tt.expectedStmts, len(stmts))
			}

			if len(paramsInfo) != tt.expectedStmts {
				t.Errorf("expected %d param info, got %d", tt.expectedStmts, len(paramsInfo))
			}
		})
	}
}

func TestParseStatementAndArgs_NoArgs(t *testing.T) {
	sql := "SELECT * FROM users"
	stmts, params, err := ParseStatementAndArgs(sql, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(stmts) != 1 {
		t.Errorf("expected 1 statement, got %d", len(stmts))
	}

	if params != nil {
		t.Errorf("expected nil params for no args, got %v", params)
	}
}

func TestParseStatementAndArgs_WithPositionalArgs(t *testing.T) {
	sql := "SELECT * FROM users WHERE id = ?"
	args := []driver.NamedValue{
		{Ordinal: 1, Value: 123},
	}

	stmts, params, err := ParseStatementAndArgs(sql, args)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(stmts) != 1 {
		t.Errorf("expected 1 statement, got %d", len(stmts))
	}

	if len(params) != 1 {
		t.Errorf("expected 1 param set, got %d", len(params))
	}

	if params[0].Len() != 1 {
		t.Errorf("expected 1 param, got %d", params[0].Len())
	}
}

func TestParseStatementAndArgs_WithNamedArgs(t *testing.T) {
	sql := "SELECT * FROM users WHERE id = :id AND name = :name"
	args := []driver.NamedValue{
		{Name: "id", Ordinal: 1, Value: 123},
		{Name: "name", Ordinal: 2, Value: "John"},
	}

	stmts, params, err := ParseStatementAndArgs(sql, args)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(stmts) != 1 {
		t.Errorf("expected 1 statement, got %d", len(stmts))
	}

	if len(params) != 1 {
		t.Errorf("expected 1 param set, got %d", len(params))
	}

	if params[0].Named()["id"] != 123 {
		t.Errorf("expected id=123, got %v", params[0].Named()["id"])
	}

	if params[0].Named()["name"] != "John" {
		t.Errorf("expected name='John', got %v", params[0].Named()["name"])
	}
}

func TestParseStatementAndArgs_MultipleStatements(t *testing.T) {
	sql := "INSERT INTO users VALUES (?); INSERT INTO posts VALUES (?)"
	args := []driver.NamedValue{
		{Ordinal: 1, Value: "user1"},
		{Ordinal: 2, Value: "post1"},
	}

	stmts, params, err := ParseStatementAndArgs(sql, args)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(stmts) != 2 {
		t.Errorf("expected 2 statements, got %d", len(stmts))
	}

	if len(params) != 2 {
		t.Errorf("expected 2 param sets, got %d", len(params))
	}

	// First statement should get first param
	if params[0].Positional()[0] != "user1" {
		t.Errorf("expected 'user1' for first statement, got %v", params[0].Positional()[0])
	}

	// Second statement should get second param
	if params[1].Positional()[0] != "post1" {
		t.Errorf("expected 'post1' for second statement, got %v", params[1].Positional()[0])
	}
}

func TestIsExplain(t *testing.T) {
	tests := []struct {
		name     string
		stmt     string
		expected bool
	}{
		{
			name:     "EXPLAIN statement",
			stmt:     "EXPLAIN SELECT * FROM users",
			expected: true,
		},
		{
			name:     "EXPLAIN QUERY PLAN",
			stmt:     "EXPLAIN QUERY PLAN SELECT * FROM users",
			expected: true,
		},
		{
			name:     "regular SELECT",
			stmt:     "SELECT * FROM users",
			expected: false,
		},
		{
			name:     "INSERT statement",
			stmt:     "INSERT INTO users VALUES (1)",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isExplain(tt.stmt)
			if result != tt.expected {
				t.Errorf("isExplain(%q) = %v, want %v", tt.stmt, result, tt.expected)
			}
		})
	}
}
