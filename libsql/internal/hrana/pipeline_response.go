package hrana

type PipelineResponse struct {
	Baton   string         `json:"baton,omitempty"`
	BaseUrl string         `json:"base_url,omitempty"`
	Results []StreamResult `json:"results"`
}
