package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

var httpClient = &http.Client{Timeout: 120 * time.Second}

type paramsType int

const (
	namedParameters paramsType = iota
	positionalParameters
)

type params struct {
	positional []any
	named      map[string]any
}

func (p *params) MarshalJSON() ([]byte, error) {
	if len(p.named) > 0 {
		return json.Marshal(p.named)
	}
	if len(p.positional) > 0 {
		return json.Marshal(p.positional)
	}
	return json.Marshal(make([]any, 0))

}

func NewParams(t paramsType) params {
	p := params{}
	switch t {
	case namedParameters:
		p.named = make(map[string]any)
	case positionalParameters:
		p.positional = make([]any, 0)
	}

	return p
}

func (p *params) Len() int {
	if p.named != nil {
		return len(p.named)
	}

	return len(p.positional)
}

func (p *params) Type() paramsType {
	if p.named != nil {
		return namedParameters
	}

	return positionalParameters
}

type postBody struct {
	Statements []statement `json:"statements"`
}

type statement struct {
	Query  string `json:"q"`
	Params params `json:"params"`
}

type resultSet struct {
	Columns []string `json:"columns"`
	Rows    []Row    `json:"rows"`
}

type httpErrObject struct {
	Message string `json:"message"`
}

type httpResults struct {
	Results *resultSet     `json:"results"`
	Error   *httpErrObject `json:"error"`
}

type Row []interface{}

func callSqld(ctx context.Context, url string, jwt string, stmts []string, parameters params) ([]httpResults, error) {
	rawReq, err := generatePostBody(stmts, parameters)
	if err != nil {
		return nil, err
	}

	reqBody, err := json.Marshal(rawReq)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if len(jwt) > 0 {
		req.Header.Set("Authorization", "Bearer "+jwt)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		var err_response struct {
			Message string `json:"error"`
		}
		if err := json.Unmarshal(body, &err_response); err != nil {
			return nil, err
		}
		return nil, errors.New(err_response.Message)
	}

	var results []httpResults

	if err := unmarshalResponse(body, &results); err != nil {
		return nil, err
	}

	if results[0].Error != nil {
		return nil, errors.New(results[0].Error.Message)
	}
	if results[0].Results == nil {
		return nil, errors.New("no results")
	}
	return results, nil
}

func generatePostBody(stmts []string, sqlParams params) (*postBody, error) {
	postBody := postBody{}

	totalParametersAlreadyUsed := 0
	for _, stmt := range stmts {
		stmtParameters, err := generateStatementParameters(stmt, sqlParams, totalParametersAlreadyUsed)
		if err != nil {
			return nil, fmt.Errorf("fail to generate statement parameter. statement: %s. error: %v", stmt, err)
		}
		postBody.Statements = append(postBody.Statements, statement{stmt, stmtParameters})
		totalParametersAlreadyUsed += stmtParameters.Len()
	}

	return &postBody, nil
}

// httpResultsAlternative is an alternative struct for unmarshalling the response
// see more info here: https://github.com/libsql/sqld/issues/466
type httpResultsAlternative struct {
	Results *resultSet `json:"results"`
	Error   string     `json:"error"`
}

func unmarshalResponse(body []byte, result *[]httpResults) error {
	err := json.Unmarshal(body, result)
	if err == nil {
		return nil
	}

	var alternativeResults []httpResultsAlternative
	errArray := json.Unmarshal(body, &alternativeResults)
	if errArray != nil {
		return err
	}

	convertedResult := make([]httpResults, len(alternativeResults))
	for _, alternativeResult := range alternativeResults {
		convertedResult = append(convertedResult, httpResults{
			Results: alternativeResult.Results,
			Error:   &httpErrObject{Message: alternativeResult.Error}})
	}
	result = &convertedResult

	return nil
}
