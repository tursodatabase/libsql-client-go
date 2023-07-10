package hrana

type PipelineRequest struct {
	Baton    string          `json:"baton,omitempty"`
	Requests []StreamRequest `json:"requests"`
}

func (pr *PipelineRequest) Add(request StreamRequest) {
	pr.Requests = append(pr.Requests, request)
}
