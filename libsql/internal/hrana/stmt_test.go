package hrana

import (
	"reflect"
	"testing"
)

func TestStmtWithPositionalArgs(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		args    []any
		want    *Stmt
		wantErr bool
	}{
		{
			name: "int args",
			sql:  "SELECT * FROM table WHERE col1 = ? AND col2 = ?",
			args: []any{1, 2},
			want: &Stmt{
				Sql:      "SELECT * FROM table WHERE col1 = ? AND col2 = ?",
				Args:     []Value{{Type: "integer", Value: "1"}, {Type: "integer", Value: "2"}},
				WantRows: false,
			},
		},
		{
			name: "string args",
			sql:  "SELECT * FROM table WHERE col1 = ? AND col2 = ?",
			args: []any{"a", "b"},
			want: &Stmt{
				Sql:      "SELECT * FROM table WHERE col1 = ? AND col2 = ?",
				Args:     []Value{{Type: "text", Value: "a"}, {Type: "text", Value: "b"}},
				WantRows: false,
			},
		},
		{
			name:    "invalid arg",
			sql:     "SELECT * FROM table WHERE col1 = ?",
			args:    []any{make(chan int)},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := StmtWithPositionalArgs(tt.sql, tt.args, false)
			if (err != nil) != tt.wantErr {
				t.Errorf("StmtWithPositionalArgs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StmtWithPositionalArgs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStmtWithNamedArgs(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		args    map[string]any
		want    *Stmt
		wantErr bool
	}{
		{
			name: "int args",
			sql:  "SELECT * FROM table WHERE col1 = :arg1 AND col2 = :arg2",
			args: map[string]any{"arg1": 1, "arg2": int64(2)},
			want: &Stmt{
				Sql: "SELECT * FROM table WHERE col1 = :arg1 AND col2 = :arg2",
				NamedArgs: []NamedArg{
					{Name: "arg1", Value: Value{Type: "integer", Value: "1"}},
					{Name: "arg2", Value: Value{Type: "integer", Value: "2"}},
				},
				WantRows: false,
			},
		},
		{
			name: "string args",
			sql:  "SELECT * FROM table WHERE col1 = :arg1 AND col2 = :arg2",
			args: map[string]any{"arg1": "a", "arg2": "b"},
			want: &Stmt{
				Sql: "SELECT * FROM table WHERE col1 = :arg1 AND col2 = :arg2",
				NamedArgs: []NamedArg{
					{Name: "arg1", Value: Value{Type: "text", Value: "a"}},
					{Name: "arg2", Value: Value{Type: "text", Value: "b"}},
				},
				WantRows: false,
			},
		},
		{
			name:    "invalid arg",
			sql:     "SELECT * FROM table WHERE col1 = :arg1",
			args:    map[string]any{"arg1": make(chan int)},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := StmtWithNamedArgs(tt.sql, tt.args, false)
			if (err != nil) != tt.wantErr {
				t.Errorf("StmtWithNamedArgs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StmtWithNamedArgs() = %v, want %v", got, tt.want)
			}
		})
	}
}
