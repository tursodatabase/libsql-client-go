package hrana

import (
	"github.com/tursodatabase/libsql-client-go/libsql/internal/http/shared"
)

type Stmt struct {
	Sql              *string    `json:"sql,omitempty"`
	SqlId            *int32     `json:"sql_id,omitempty"`
	Args             []Value    `json:"args,omitempty"`
	NamedArgs        []NamedArg `json:"named_args,omitempty"`
	WantRows         bool       `json:"want_rows"`
	ReplicationIndex *uint64    `json:"replication_index,omitempty"`
}

type NamedArg struct {
	Name  string `json:"name"`
	Value Value  `json:"value"`
}

func (s *Stmt) AddArgs(params shared.Params) error {
	if len(params.Named()) > 0 {
		return s.AddNamedArgs(params.Named())
	} else {
		return s.AddPositionalArgs(params.Positional())
	}
}

func (s *Stmt) AddPositionalArgs(args []any) error {
	argValues := make([]Value, len(args))
	for idx := range args {
		var err error
		if argValues[idx], err = ToValue(args[idx]); err != nil {
			return err
		}
	}
	s.Args = argValues
	return nil
}

func (s *Stmt) AddNamedArgs(args map[string]any) error {
	argValues := make([]NamedArg, len(args))
	idx := 0
	for key, value := range args {
		var err error
		var v Value
		if v, err = ToValue(value); err != nil {
			return err
		}
		argValues[idx] = NamedArg{
			Name:  key,
			Value: v,
		}
		idx++
	}
	s.NamedArgs = argValues
	return nil
}
