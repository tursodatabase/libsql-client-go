package http

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func TestSplitStamentToPieces(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  []string
	}{
		{
			name:  "EmptyStatement",
			value: "",
			want:  []string{},
		},
		{
			name:  "OnlySemicolon",
			value: ";;;;",
			want:  []string{},
		},
		{
			name:  "SingleStatementWithoutSemicolon",
			value: "select 1",
			want:  []string{"select 1"},
		},
		{
			name:  "SingleStatementWithSemicolon",
			value: "select 1;",
			want:  []string{"select 1"},
		},
		{
			name:  "MultpleCorrectStatements",
			value: "select 1; INSERT INTO counter(country, city, value) VALUES(?, ?, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = ? AND city = ?; select 2",
			want:  []string{"select 1", "INSERT INTO counter(country, city, value) VALUES(?, ?, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = ? AND city = ?", "select 2"},
		},
		{
			name:  "MultpleWrongStatements",
			value: "select from table; INSERT counter(country, city, value) VALUES(?, ?, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = ? AND city = ?; create something",
			want:  []string{"select from table", "INSERT counter(country, city, value) VALUES(?, ?, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = ? AND city = ?", "create something"},
		},
		{
			name:  "MultpleWrongTokens",
			value: "sdfasdfigosdfg sadfgsd ggsadgf; sdfasdfasd; 1230kfvcasd; 213 dsf s 0 fs229dt",
			want:  []string{"sdfasdfigosdfg sadfgsd ggsadgf", "sdfasdfasd", "1230kfvcasd", "213 dsf s 0 fs229dt"},
		},
		{
			name:  "MultpleSemicolonsBetweenStatements",
			value: "select 1;;;;;; ;;; ; ; ; ; select 2",
			want:  []string{"select 1", "select 2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitStatementToPieces(tt.value)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %#v, want %#v", got, tt.want)
			}
		})
	}
}

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
			name:       "RepetedNamedParamer",
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
