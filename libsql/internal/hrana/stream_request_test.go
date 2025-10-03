package hrana

import (
	"testing"

	"github.com/tursodatabase/libsql-client-go/libsql/internal/http/shared"
)

func TestCloseStream(t *testing.T) {
	req := CloseStream()

	if req.Type != "close" {
		t.Errorf("expected type 'close', got %q", req.Type)
	}
}

func TestExecuteStream(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		params   *shared.Params
		wantRows bool
	}{
		{
			name:     "simple query without params",
			sql:      "SELECT * FROM users",
			params:   nil,
			wantRows: true,
		},
		{
			name:     "query without rows",
			sql:      "INSERT INTO users VALUES (1)",
			params:   nil,
			wantRows: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := ExecuteStream(tt.sql, tt.params, tt.wantRows)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if req.Type != "execute" {
				t.Errorf("expected type 'execute', got %q", req.Type)
			}

			if req.Stmt == nil {
				t.Fatal("expected stmt to be non-nil")
			}

			if req.Stmt.Sql == nil || *req.Stmt.Sql != tt.sql {
				t.Errorf("expected sql %q, got %v", tt.sql, req.Stmt.Sql)
			}

			if req.Stmt.WantRows != tt.wantRows {
				t.Errorf("expected wantRows %v, got %v", tt.wantRows, req.Stmt.WantRows)
			}
		})
	}
}

func TestExecuteStreamWithParams(t *testing.T) {
	params := shared.NewParams(1) // positional
	params = shared.Params{}
	params = shared.NewParams(0) // named

	namedParams := map[string]any{
		"id":   123,
		"name": "test",
	}

	for k, v := range namedParams {
		if params.Named() == nil {
			t.Fatal("named params map is nil")
		}
		params.Named()[k] = v
	}

	req, err := ExecuteStream("SELECT * FROM users WHERE id = :id", &params, true)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(req.Stmt.NamedArgs) != 2 {
		t.Errorf("expected 2 named args, got %d", len(req.Stmt.NamedArgs))
	}
}

func TestExecuteStoredStream(t *testing.T) {
	sqlId := int32(42)
	params := shared.NewParams(1) // positional
	params = shared.Params{}

	req, err := ExecuteStoredStream(sqlId, params, true)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if req.Type != "execute" {
		t.Errorf("expected type 'execute', got %q", req.Type)
	}

	if req.Stmt == nil {
		t.Fatal("expected stmt to be non-nil")
	}

	if req.Stmt.SqlId == nil || *req.Stmt.SqlId != sqlId {
		t.Errorf("expected sqlId %d, got %v", sqlId, req.Stmt.SqlId)
	}
}

func TestBatchStream(t *testing.T) {
	tests := []struct {
		name          string
		sqls          []string
		params        []shared.Params
		wantRows      bool
		transactional bool
		expectSteps   int
	}{
		{
			name:          "single statement non-transactional",
			sqls:          []string{"SELECT 1"},
			params:        []shared.Params{},
			wantRows:      true,
			transactional: false,
			expectSteps:   1,
		},
		{
			name:          "single statement transactional",
			sqls:          []string{"SELECT 1"},
			params:        []shared.Params{},
			wantRows:      true,
			transactional: true,
			expectSteps:   2, // original + rollback
		},
		{
			name:          "multiple statements transactional",
			sqls:          []string{"INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (2)"},
			params:        []shared.Params{},
			wantRows:      false,
			transactional: true,
			expectSteps:   3, // 2 inserts + rollback
		},
		{
			name:          "multiple statements non-transactional",
			sqls:          []string{"SELECT 1", "SELECT 2"},
			params:        []shared.Params{},
			wantRows:      true,
			transactional: false,
			expectSteps:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := BatchStream(tt.sqls, tt.params, tt.wantRows, tt.transactional)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if req.Type != "batch" {
				t.Errorf("expected type 'batch', got %q", req.Type)
			}

			if req.Batch == nil {
				t.Fatal("expected batch to be non-nil")
			}

			if len(req.Batch.Steps) != tt.expectSteps {
				t.Errorf("expected %d steps, got %d", tt.expectSteps, len(req.Batch.Steps))
			}

			// Check that all steps have wantRows set correctly
			for i, step := range req.Batch.Steps {
				if i < len(tt.sqls) {
					if step.Stmt.WantRows != tt.wantRows {
						t.Errorf("step %d: expected wantRows %v, got %v", i, tt.wantRows, step.Stmt.WantRows)
					}
				}
			}

			// If transactional, check conditions
			if tt.transactional && len(tt.sqls) > 1 {
				// Second statement should have a condition on first
				if req.Batch.Steps[1].Condition == nil {
					t.Error("expected second step to have a condition")
				} else {
					if req.Batch.Steps[1].Condition.Type != "ok" {
						t.Errorf("expected condition type 'ok', got %q", req.Batch.Steps[1].Condition.Type)
					}
					if req.Batch.Steps[1].Condition.Step == nil || *req.Batch.Steps[1].Condition.Step != 0 {
						t.Error("expected condition to reference step 0")
					}
				}
			}
		})
	}
}

func TestStoreSqlStream(t *testing.T) {
	sql := "SELECT * FROM users"
	sqlId := int32(42)

	req := StoreSqlStream(sql, sqlId)

	if req.Type != "store_sql" {
		t.Errorf("expected type 'store_sql', got %q", req.Type)
	}

	if req.Sql == nil || *req.Sql != sql {
		t.Errorf("expected sql %q, got %v", sql, req.Sql)
	}

	if req.SqlId == nil || *req.SqlId != sqlId {
		t.Errorf("expected sqlId %d, got %v", sqlId, req.SqlId)
	}
}

func TestCloseStoredSqlStream(t *testing.T) {
	sqlId := int32(42)

	req := CloseStoredSqlStream(sqlId)

	if req.Type != "close_sql" {
		t.Errorf("expected type 'close_sql', got %q", req.Type)
	}

	if req.SqlId == nil || *req.SqlId != sqlId {
		t.Errorf("expected sqlId %d, got %v", sqlId, req.SqlId)
	}
}
