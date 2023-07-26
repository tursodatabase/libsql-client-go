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
	stmt := &Stmt{
		Sql:      &sql,
		WantRows: wantRows,
	}
	if err := stmt.AddArgs(params); err != nil {
		return nil, err
	}
	return &StreamRequest{Type: "execute", Stmt: stmt}, nil
}

func ExecuteStoredStream(sqlId int32, params shared.Params, wantRows bool) (*StreamRequest, error) {
	stmt := &Stmt{
		SqlId:    &sqlId,
		WantRows: wantRows,
	}
	if err := stmt.AddArgs(params); err != nil {
		return nil, err
	}
	return &StreamRequest{Type: "execute", Stmt: stmt}, nil
}

func BatchStream(sqls []string, params []shared.Params, wantRows bool) (*StreamRequest, error) {
	batch := &Batch{}
	for idx, sql := range sqls {
		s := sql
		stmt := &Stmt{
			Sql:      &s,
			WantRows: wantRows,
		}
		if err := stmt.AddArgs(params[idx]); err != nil {
			return nil, err
		}
		batch.Add(*stmt)
	}
	return &StreamRequest{Type: "batch", Batch: batch}, nil
}

func StoreSqlStream(sql string, sqlId int32) StreamRequest {
	return StreamRequest{Type: "store_sql", Sql: &sql, SqlId: &sqlId}
}

func CloseStoredSqlStream(sqlId int32) StreamRequest {
	return StreamRequest{Type: "close_sql", SqlId: &sqlId}
}
