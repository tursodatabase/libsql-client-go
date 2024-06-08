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
			name:                  "OnlyPositionalParamsWithIndexes (dedup)",
			value:                 "select ?1 from ?1",
			nameParams:            []string{},
			positionalParamsCount: 1,
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
			gotNameParams, gotUniquePositionalParamsCount, gotErr := extractParameters(tt.value)
			sort.Strings(gotNameParams)
			sort.Strings(tt.nameParams)
			if !reflect.DeepEqual(gotNameParams, tt.nameParams) {
				t.Errorf("got nameParams %#v, want %#v", gotNameParams, tt.nameParams)
			}
			if !reflect.DeepEqual(gotUniquePositionalParamsCount, tt.positionalParamsCount) {
				t.Errorf("got positionalParams %#v, want %#v", gotUniquePositionalParamsCount, tt.positionalParamsCount)
			}
			if !reflect.DeepEqual(gotUniquePositionalParamsCount, tt.positionalParamsCount) {
				t.Errorf("got err %v, want %v", gotErr, tt.err)
			}
		})
	}
}
