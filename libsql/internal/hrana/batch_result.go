package hrana

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type BatchResult struct {
	StepResults      []*StmtResult `json:"step_results"`
	StepErrors       []*Error      `json:"step_errors"`
	ReplicationIndex *uint64       `json:"replication_index"`
}

func (b *BatchResult) UnmarshalJSON(data []byte) error {
	type Alias BatchResult
	aux := &struct {
		ReplicationIndex interface{} `json:"replication_index,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(b),
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
		b.ReplicationIndex = &repIndex
	case string:
		if v == "" {
			return nil
		}
		repIndex, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return err
		}
		b.ReplicationIndex = &repIndex
	default:
		return fmt.Errorf("invalid type for replication index: %T", v)
	}
	return nil
}
