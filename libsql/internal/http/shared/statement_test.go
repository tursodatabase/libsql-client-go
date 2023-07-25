package shared

import (
	"fmt"
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
			name:                  "PositionalParamsWithIndexes",
			value:                 "select ? from ?1",
			nameParams:            []string{},
			positionalParamsCount: 0,
			err:                   fmt.Errorf("unsuppoted positional parameter. This driver does not accept positional parameters with indexes (like ?<number>)"),
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
