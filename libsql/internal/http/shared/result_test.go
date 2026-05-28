package shared

import (
	"testing"
)

func TestResult_LastInsertId(t *testing.T) {
	r := NewResult(42, 5)

	id, err := r.LastInsertId()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if id != 42 {
		t.Errorf("expected LastInsertId=42, got %d", id)
	}
}

func TestResult_RowsAffected(t *testing.T) {
	r := NewResult(42, 5)

	affected, err := r.RowsAffected()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if affected != 5 {
		t.Errorf("expected RowsAffected=5, got %d", affected)
	}
}

func TestResult_ZeroValues(t *testing.T) {
	r := NewResult(0, 0)

	id, err := r.LastInsertId()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != 0 {
		t.Errorf("expected LastInsertId=0, got %d", id)
	}

	affected, err := r.RowsAffected()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if affected != 0 {
		t.Errorf("expected RowsAffected=0, got %d", affected)
	}
}

func TestResult_NegativeValues(t *testing.T) {
	r := NewResult(-1, -1)

	id, err := r.LastInsertId()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != -1 {
		t.Errorf("expected LastInsertId=-1, got %d", id)
	}

	affected, err := r.RowsAffected()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if affected != -1 {
		t.Errorf("expected RowsAffected=-1, got %d", affected)
	}
}
