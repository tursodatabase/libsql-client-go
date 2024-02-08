package hrana

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type Column struct {
	Name *string `json:"name"`
	Type *string `json:"decltype"`
}

type StmtResult struct {
	Cols             []Column  `json:"cols"`
	Rows             [][]Value `json:"rows"`
	AffectedRowCount int32     `json:"affected_row_count"`
	LastInsertRowId  *string   `json:"last_insert_rowid"`
	ReplicationIndex *uint64   `json:"replication_index"`
}

func (r *StmtResult) GetLastInsertRowId() int64 {
	if r.LastInsertRowId != nil {
		if integer, err := strconv.ParseInt(*r.LastInsertRowId, 10, 64); err == nil {
			return integer
		}
	}
	return 0
}

func (r *StmtResult) UnmarshalJSON(data []byte) error {
	type Alias StmtResult
	aux := &struct {
		ReplicationIndex interface{} `json:"replication_index,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(r),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if aux.ReplicationIndex == nil {
		return nil
	}

	switch v := aux.ReplicationIndex.(type) {
	case float64:
		repIndex := uint64(v)
		r.ReplicationIndex = &repIndex
	case string:
		if v == "" {
			return nil
		}
		repIndex, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return err
		}
		r.ReplicationIndex = &repIndex
	default:
		return fmt.Errorf("invalid type for replication index: %T", v)
	}
	return nil
}
