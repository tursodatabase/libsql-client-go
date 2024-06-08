package shared

import (
	"reflect"
	"sort"
	"testing"
)

func TestExtractParameters(t *testing.T) {
	tests := []struct {
		name                  string
		value                 string
		nameParams            []string
		positionalParamsCount int
		err                   error
	}{
		{
			name:       "OnlyColonNameParams",
			value:      "select :column from :table",
			nameParams: []string{"column", "table"},
		},
		{
			name:       "OnlyAtNameParams",
			value:      "select @column from @table",
			nameParams: []string{"column", "table"},
		},
		{
			name:       "OnlyDollarSignNameParams",
			value:      "select $column from $table",
			nameParams: []string{"column", "table"},
		},
		{
			name:       "RepeatedNamedParameter",
			value:      "select :number, :number",
			nameParams: []string{"number"},
		},
		{
			name:                  "OnlyPositionalParams",
			value:                 "select ? from ?",
			nameParams:            []string{},
			positionalParamsCount: 2,
		},
		{
			name:                  "OnlyPositionalParamsWithoutIndexes",
			value:                 "select ? from ?",
			nameParams:            []string{},
			positionalParamsCount: 2,
		},
		{
			name:                  "OnlyPositionalParamsWithIndexes",
			value:                 "select ?1 from ?1",
			nameParams:            []string{},
			positionalParamsCount: 2,
		},
		{
			name:                  "PositionalParamsWithIndexes",
			value:                 "select ? from ?1",
			nameParams:            []string{},
			positionalParamsCount: 2,
		},
		{
			name:                  "MixedParams",
			value:                 "select :column1, @column2, $column3, ? from ?",
			nameParams:            []string{"column1", "column2", "column3"},
			positionalParamsCount: 2,
		},
		{
			name:       "NoParams",
			value:      "select myColumn from myTable",
			nameParams: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNameParams, gotPositionalParamsCount, gotErr := extractParameters(tt.value)
			sort.Strings(gotNameParams)
			sort.Strings(tt.nameParams)
			if !reflect.DeepEqual(gotNameParams, tt.nameParams) {
				t.Errorf("got nameParams %#v, want %#v", gotNameParams, tt.nameParams)
			}
			if !reflect.DeepEqual(gotPositionalParamsCount, tt.positionalParamsCount) {
				t.Errorf("got positionalParams %#v, want %#v", gotPositionalParamsCount, tt.positionalParamsCount)
			}
			if !reflect.DeepEqual(gotPositionalParamsCount, tt.positionalParamsCount) {
				t.Errorf("got err %v, want %v", gotErr, tt.err)
			}
		})
	}
}

func TestUniqueParamCount(t *testing.T) {
	tests := []struct {
		name     string
		stmt     string
		expected int
	}{
		{
			name:     "NoParams",
			stmt:     "SELECT * FROM users",
			expected: 0,
		},
		{
			name:     "SingleNoIndexParam",
			stmt:     "SELECT * FROM users WHERE id = ?",
			expected: 1,
		},
		{
			name:     "MultipleNoIndexParams",
			stmt:     "SELECT * FROM users WHERE id = ? AND name = ?",
			expected: 2,
		},
		{
			name:     "SingleIndexParam",
			stmt:     "SELECT * FROM users WHERE id = ?1",
			expected: 1,
		},
		{
			name:     "MultipleIndexParams",
			stmt:     "SELECT * FROM users WHERE id = ?1 AND name = ?2",
			expected: 2,
		},
		{
			name:     "MixedParams",
			stmt:     "SELECT * FROM users WHERE id = ?1 AND name = ?",
			expected: 2,
		},
		{
			name:     "RepeatedIndexParams",
			stmt:     "SELECT * FROM users WHERE id = ?1 AND name = ?1",
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count := uniquePositionalParamCount(tt.stmt)
			if count != tt.expected {
				t.Errorf("uniqueParamCount(%q) = %d, want %d", tt.stmt, count, tt.expected)
			}
		})
	}
}
