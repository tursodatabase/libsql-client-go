package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/xwb1989/sqlparser"
)

type params struct {
	Names  []string
	Values []any
}

func (p *params) MarshalJSON() ([]byte, error) {
	if len(p.Values) == 0 {
		return json.Marshal([]string{})
	}
	if len(p.Names) == 0 {
		return json.Marshal(p.Values)
	}
	m := map[string]interface{}{}
	for idx := range p.Values {
		m["@"+p.Names[idx]] = p.Values[idx]
	}
	return json.Marshal(m)
}

type resultSet struct {
	Columns []string `json:"columns"`
	Rows    []Row    `json:"rows"`
}

type Row []interface{}

func callSqld(ctx context.Context, url string, sql string, sqlParams params) (*resultSet, error) {
	stmts, err := sqlparser.SplitStatementToPieces(sql)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("wrong number of statements in SQL: %s\nexpected 1 got %d", sql, len(stmts))
	}

	type Statement struct {
		Query  string `json:"q"`
		Params params `json:"params"`
	}

	rawReq := struct {
		Statements []Statement `json:"statements"`
	}{
		Statements: []Statement{{sql, sqlParams}},
	}

	reqBody, err := json.Marshal(rawReq)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		var err_response struct {
			Message string `json:"error"`
		}
		if err := json.Unmarshal(body, &err_response); err != nil {
			return nil, fmt.Errorf("failed to execute SQL: %s", sql)
		}
		return nil, fmt.Errorf("failed to execute SQL: %s\n%s", sql, err_response.Message)
	}

	type errObject struct {
		Message string `json:"message"`
	}

	var results []struct {
		Results *resultSet `json:"results"`
		Error   *errObject `json:"error"`
	}
	if err := json.Unmarshal(body, &results); err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, fmt.Errorf("wrong number of results for SQL: %s\nexpected 1 got %d", sql, len(results))
	}
	if results[0].Error != nil {
		return nil, fmt.Errorf("failed to execute SQL: %s\n%s", sql, results[0].Error.Message)
	}
	if results[0].Results == nil {
		return nil, fmt.Errorf("no results for SQL: %s", sql)
	}
	return results[0].Results, nil
}
