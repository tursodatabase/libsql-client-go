package hrana

type Stmt struct {
	Sql       string     `json:"sql"`
	Args      []Value    `json:"args,omitempty"`
	NamedArgs []NamedArg `json:"named_args,omitempty"`
	WantRows  bool       `json:"want_rows"`
}

type NamedArg struct {
	Name  string `json:"name"`
	Value Value  `json:"value"`
}

func ToStmt(sql string, wantRows bool) *Stmt {
	return &Stmt{
		Sql:      sql,
		WantRows: wantRows,
	}
}

func StmtWithPositionalArgs(sql string, args []any, wantRows bool) (*Stmt, error) {
	argValues := make([]Value, len(args))
	for idx := range args {
		var err error
		if argValues[idx], err = ToValue(args[idx]); err != nil {
			return nil, err
		}
	}
	return &Stmt{
		Sql:      sql,
		Args:     argValues,
		WantRows: wantRows,
	}, nil
}

func StmtWithNamedArgs(sql string, args map[string]any, wantRows bool) (*Stmt, error) {
	argValues := make([]NamedArg, len(args))
	idx := 0
	for key, value := range args {
		var err error
		var v Value
		if v, err = ToValue(value); err != nil {
			return nil, err
		}
		argValues[idx] = NamedArg{
			Name:  key,
			Value: v,
		}
		idx++
	}
	return &Stmt{
		Sql:       sql,
		NamedArgs: argValues,
		WantRows:  wantRows,
	}, nil
}
