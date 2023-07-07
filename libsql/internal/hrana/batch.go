package hrana

type Batch struct {
	Steps []BatchStep `json:"steps"`
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
