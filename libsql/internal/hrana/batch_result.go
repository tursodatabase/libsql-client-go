package hrana

type BatchResult struct {
	StepResults []*StmtResult `json:"step_results"`
	StepErrors  []*Error      `json:"step_errors"`
}
