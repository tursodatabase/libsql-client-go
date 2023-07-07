package hrana

import (
	"github.com/libsql/libsql-client-go/libsql/internal/http/shared"
)

type StreamRequest struct {
	Type  string  `json:"type"`
	Stmt  *Stmt   `json:"stmt,omitempty"`
	Batch *Batch  `json:"batch,omitempty"`
	Sql   *string `json:"sql,omitempty"`
	SqlId *int32  `json:"sql_id,omitempty"`
}

func CloseStream() StreamRequest {
	return StreamRequest{Type: "close"}
}

func ExecuteStream(sql string, params shared.Params, wantRows bool) (*StreamRequest, error) {
	stmt, err := createStmt(sql, params, wantRows)
	if err != nil {
		return nil, err
	}
	return &StreamRequest{Type: "execute", Stmt: stmt}, nil
}

func BatchStream(sqls []string, params []shared.Params, wantRows bool) (*StreamRequest, error) {
	batch := &Batch{}
	for idx, sql := range sqls {
		stmt, err := createStmt(sql, params[idx], wantRows)
		if err != nil {
			return nil, err
		}
		batch.Add(*stmt)
	}
	return &StreamRequest{Type: "batch", Batch: batch}, nil
}

func createStmt(sql string, params shared.Params, wantRows bool) (*Stmt, error) {
	if len(params.Named()) > 0 {
		return StmtWithNamedArgs(sql, params.Named(), wantRows)
	} else {
		return StmtWithPositionalArgs(sql, params.Positional(), wantRows)
	}
}
