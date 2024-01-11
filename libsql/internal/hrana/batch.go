package hrana

import (
	"encoding/json"
	"fmt"
)

type Batch struct {
	Steps            []BatchStep `json:"steps"`
	ReplicationIndex *uint64     `json:"replication_index"`
}

type BatchStep struct {
	Stmt      Stmt            `json:"stmt"`
	Condition *BatchCondition `json:"condition,omitempty"`
}

type BatchCondition struct {
	Type  string           `json:"type"`
	Step  *int32           `json:"step,omitempty"`
	Cond  *BatchCondition  `json:"cond,omitempty"`
	Conds []BatchCondition `json:"conds,omitempty"`
}

func (b *Batch) Add(stmt Stmt) {
	b.Steps = append(b.Steps, BatchStep{Stmt: stmt})
}

func (b *Batch) MarshalJSON() ([]byte, error) {
	type Alias Batch
	var repIndex string
	if b.ReplicationIndex != nil {
		repIndex = fmt.Sprint(*b.ReplicationIndex)
	}
	return json.Marshal(&struct {
		ReplicationIndex string `json:"replication_index,omitempty"`
		*Alias
	}{
		ReplicationIndex: repIndex,
		Alias:            (*Alias)(b),
	})
}
