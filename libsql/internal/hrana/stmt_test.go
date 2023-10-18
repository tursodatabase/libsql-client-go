package hrana

import (
	"reflect"
	"testing"
)

func TestStmtWithPositionalArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    []any
		want    []Value
		wantErr bool
	}{
		{
			name: "int args",
			args: []any{1, 2},
			want: []Value{{Type: "integer", Value: "1"}, {Type: "integer", Value: "2"}},
		},
		{
			name: "string args",
			args: []any{"a", "b"},
			want: []Value{{Type: "text", Value: "a"}, {Type: "text", Value: "b"}},
		},
		{
			name:    "invalid arg",
			args:    []any{make(chan int)},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := Stmt{}
			err := stmt.AddPositionalArgs(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("StmtWithPositionalArgs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(stmt.Args, tt.want) {
				t.Errorf("got = %v, want %v", stmt.Args, tt.want)
			}
		})
	}
}

func TestStmtWithNamedArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    map[string]any
		want    []NamedArg
		wantErr bool
	}{
		{
			name: "int args",
			args: map[string]any{"arg1": 1, "arg2": int64(2)},
			want: []NamedArg{
				{Name: "arg1", Value: Value{Type: "integer", Value: "1"}},
				{Name: "arg2", Value: Value{Type: "integer", Value: "2"}},
			},
		},
		{
			name: "string args",
			args: map[string]any{"arg1": "a", "arg2": "b"},
			want: []NamedArg{
				{Name: "arg1", Value: Value{Type: "text", Value: "a"}},
				{Name: "arg2", Value: Value{Type: "text", Value: "b"}},
			},
		},
		{
			name:    "invalid arg",
			args:    map[string]any{"arg1": make(chan int)},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := Stmt{}
			err := stmt.AddNamedArgs(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("StmtWithNamedArgs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got := make(map[NamedArg]struct{})
			want := make(map[NamedArg]struct{})
			for _, arg := range stmt.NamedArgs {
				got[arg] = struct{}{}
			}
			for _, arg := range tt.want {
				want[arg] = struct{}{}
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("got = %v, want %v", stmt.NamedArgs, tt.want)
			}
		})
	}
}
