package hranaV2

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/libsql/libsql-client-go/libsql/internal/hrana"
	"github.com/libsql/libsql-client-go/libsql/internal/http/shared"
	"io"
	"net/http"
	"time"
)

func IsSupported(url, jwt string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", url+"/v2", nil)
	if err != nil {
		return false
	}
	if len(jwt) > 0 {
		req.Header.Set("Authorization", "Bearer "+jwt)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func Connect(url, jwt string) driver.Conn {
	return &hranaV2Conn{url, jwt}
}

type hranaV2Conn struct {
	url string
	jwt string
}

func (h *hranaV2Conn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare method not implemented")
}

func (h *hranaV2Conn) Close() error {
	return nil
}

func (h *hranaV2Conn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("begin method not implemented")
}

func (h *hranaV2Conn) executeStmt(ctx context.Context, query string, args []driver.NamedValue, wantRows bool) (*hrana.PipelineResponse, error) {
	stmts, params, err := shared.ParseStatement(query, args)
	if err != nil {
		return nil, fmt.Errorf("failed to execute SQL: %s\n%w", query, err)
	}
	msg := &hrana.PipelineRequest{}
	if len(stmts) == 1 {
		executeStream, err := hrana.ExecuteStream(stmts[0], params[0], wantRows)
		if err != nil {
			return nil, fmt.Errorf("failed to execute SQL: %s\n%w", query, err)
		}
		msg.Add(*executeStream)
	} else {
		batchStream, err := hrana.BatchStream(stmts, params, wantRows)
		if err != nil {
			return nil, fmt.Errorf("failed to execute SQL: %s\n%w", query, err)
		}
		msg.Add(*batchStream)
	}
	msg.Add(hrana.CloseStream())

	reqBody, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", h.url+"/v2/pipeline", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	if len(h.jwt) > 0 {
		req.Header.Set("Authorization", "Bearer "+h.jwt)
	}
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
		var errResponse struct {
			Message string `json:"error"`
		}
		if err := json.Unmarshal(body, &errResponse); err == nil {
			return nil, fmt.Errorf("failed to execute SQL: %s\n%s", query, errResponse.Message)
		}
		return nil, fmt.Errorf("failed to execute SQL: %s\n%s", query, string(body))
	}
	var result hrana.PipelineResponse
	if err = json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	if result.Results[0].Error != nil {
		return nil, fmt.Errorf("failed to execute SQL: %s\n%s", query, *result.Results[0].Error)
	}
	if result.Results[0].Response == nil {
		return nil, fmt.Errorf("failed to execute SQL: %s\n%s", query, "no response received")
	}
	return &result, nil
}

func (h *hranaV2Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := h.executeStmt(ctx, query, args, false)
	if err != nil {
		return nil, err
	}
	switch result.Results[0].Response.Type {
	case "execute":
		res, err := result.Results[0].Response.ExecuteResult()
		if err != nil {
			return nil, err
		}
		return shared.NewResult(res.GetLastInsertRowId(), int64(res.AffectedRowCount)), nil
	case "batch":
		res, err := result.Results[0].Response.BatchResult()
		if err != nil {
			return nil, err
		}
		lastInsertRowId := int64(0)
		affectedRowCount := int64(0)
		for _, r := range res.StepResults {
			rowId := r.GetLastInsertRowId()
			if rowId > 0 {
				lastInsertRowId = rowId
			}
			affectedRowCount += int64(r.AffectedRowCount)
		}
		return shared.NewResult(lastInsertRowId, affectedRowCount), nil
	default:
		return nil, fmt.Errorf("failed to execute SQL: %s\n%s", query, "unknown response type")
	}
}

type StmtResultRowsProvider struct {
	r *hrana.StmtResult
}

func (p *StmtResultRowsProvider) SetsCount() int {
	return 1
}

func (p *StmtResultRowsProvider) RowsCount(setIdx int) int {
	if setIdx != 0 {
		return 0
	}
	return len(p.r.Rows)
}

func (p *StmtResultRowsProvider) Columns(setIdx int) []string {
	if setIdx != 0 {
		return nil
	}
	res := make([]string, len(p.r.Cols))
	for i, c := range p.r.Cols {
		if c.Name != nil {
			res[i] = *c.Name
		}
	}
	return res
}

func (p *StmtResultRowsProvider) FieldValue(setIdx, rowIdx, colIdx int) driver.Value {
	if setIdx != 0 {
		return nil
	}
	return p.r.Rows[rowIdx][colIdx].ToValue()
}

func (p *StmtResultRowsProvider) Error(setIdx int) string {
	return ""
}

func (p *StmtResultRowsProvider) HasResult(setIdx int) bool {
	if setIdx != 0 {
		return false
	}
	return true
}

type BatchResultRowsProvider struct {
	r *hrana.BatchResult
}

func (p *BatchResultRowsProvider) SetsCount() int {
	return len(p.r.StepResults)
}

func (p *BatchResultRowsProvider) RowsCount(setIdx int) int {
	if setIdx >= len(p.r.StepResults) || p.r.StepResults[setIdx] == nil {
		return 0
	}
	return len(p.r.StepResults[setIdx].Rows)
}

func (p *BatchResultRowsProvider) Columns(setIdx int) []string {
	if setIdx >= len(p.r.StepResults) || p.r.StepResults[setIdx] == nil {
		return nil
	}
	res := make([]string, len(p.r.StepResults[setIdx].Cols))
	for i, c := range p.r.StepResults[setIdx].Cols {
		if c.Name != nil {
			res[i] = *c.Name
		}
	}
	return res
}

func (p *BatchResultRowsProvider) FieldValue(setIdx, rowIdx, colIdx int) driver.Value {
	if setIdx >= len(p.r.StepResults) || p.r.StepResults[setIdx] == nil {
		return nil
	}
	return p.r.StepResults[setIdx].Rows[rowIdx][colIdx].ToValue()
}

func (p *BatchResultRowsProvider) Error(setIdx int) string {
	if setIdx >= len(p.r.StepErrors) || p.r.StepErrors[setIdx] == nil {
		return ""
	}
	return p.r.StepErrors[setIdx].Message
}

func (p *BatchResultRowsProvider) HasResult(setIdx int) bool {
	return setIdx < len(p.r.StepResults) && p.r.StepResults[setIdx] != nil
}

func (h *hranaV2Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	result, err := h.executeStmt(ctx, query, args, true)
	if err != nil {
		return nil, err
	}
	switch result.Results[0].Response.Type {
	case "execute":
		res, err := result.Results[0].Response.ExecuteResult()
		if err != nil {
			return nil, err
		}
		return shared.NewRows(&StmtResultRowsProvider{res}), nil
	case "batch":
		res, err := result.Results[0].Response.BatchResult()
		if err != nil {
			return nil, err
		}
		return shared.NewRows(&BatchResultRowsProvider{res}), nil
	default:
		return nil, fmt.Errorf("failed to execute SQL: %s\n%s", query, "unknown response type")
	}
}
