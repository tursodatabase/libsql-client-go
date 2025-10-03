package hrana

import (
	"testing"
)

func TestBatch_Add(t *testing.T) {
	batch := &Batch{}

	sql1 := "SELECT 1"
	stmt1 := Stmt{Sql: &sql1, WantRows: true}

	sql2 := "SELECT 2"
	stmt2 := Stmt{Sql: &sql2, WantRows: false}

	stepIdx := int32(0)
	condition := &BatchCondition{
		Type: "ok",
		Step: &stepIdx,
	}

	// Add first step without condition
	batch.Add(stmt1, nil)

	if len(batch.Steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(batch.Steps))
	}

	if batch.Steps[0].Condition != nil {
		t.Error("expected first step to have no condition")
	}

	if batch.Steps[0].Stmt.Sql == nil || *batch.Steps[0].Stmt.Sql != sql1 {
		t.Errorf("expected sql %q, got %v", sql1, batch.Steps[0].Stmt.Sql)
	}

	// Add second step with condition
	batch.Add(stmt2, condition)

	if len(batch.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(batch.Steps))
	}

	if batch.Steps[1].Condition == nil {
		t.Fatal("expected second step to have a condition")
	}

	if batch.Steps[1].Condition.Type != "ok" {
		t.Errorf("expected condition type 'ok', got %q", batch.Steps[1].Condition.Type)
	}

	if batch.Steps[1].Condition.Step == nil || *batch.Steps[1].Condition.Step != 0 {
		t.Error("expected condition to reference step 0")
	}
}

func TestBatchCondition_Types(t *testing.T) {
	tests := []struct {
		name      string
		condition BatchCondition
	}{
		{
			name: "ok condition",
			condition: BatchCondition{
				Type: "ok",
				Step: ptrInt32(0),
			},
		},
		{
			name: "error condition",
			condition: BatchCondition{
				Type: "error",
				Step: ptrInt32(1),
			},
		},
		{
			name: "not condition",
			condition: BatchCondition{
				Type: "not",
				Cond: &BatchCondition{
					Type: "ok",
					Step: ptrInt32(0),
				},
			},
		},
		{
			name: "and condition",
			condition: BatchCondition{
				Type: "and",
				Conds: []BatchCondition{
					{Type: "ok", Step: ptrInt32(0)},
					{Type: "ok", Step: ptrInt32(1)},
				},
			},
		},
		{
			name: "or condition",
			condition: BatchCondition{
				Type: "or",
				Conds: []BatchCondition{
					{Type: "ok", Step: ptrInt32(0)},
					{Type: "error", Step: ptrInt32(1)},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just verify the structure is valid
			if tt.condition.Type == "" {
				t.Error("condition type should not be empty")
			}

			if tt.condition.Type == "not" && tt.condition.Cond == nil {
				t.Error("not condition should have Cond field")
			}

			if (tt.condition.Type == "and" || tt.condition.Type == "or") && len(tt.condition.Conds) == 0 {
				t.Error("and/or condition should have Conds field")
			}
		})
	}
}

func ptrInt32(v int32) *int32 {
	return &v
}
