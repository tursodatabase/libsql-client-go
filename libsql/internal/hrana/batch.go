package hrana

type Batch struct {
	Steps            []BatchStep `json:"steps"`
	ReplicationIndex *uint64     `json:"replication_index,omitempty"`
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

func (b *Batch) Add(stmt Stmt, condition *BatchCondition) {
	b.Steps = append(b.Steps, BatchStep{Stmt: stmt, Condition: condition})
}
