package shared

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"

	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/libsql/sqlite-antlr4-parser/sqliteparser"
	"github.com/libsql/sqlite-antlr4-parser/sqliteparserutils"
)

type ParamsInfo struct {
	NamedParameters           []string
	PositionalParametersCount int
}

func ParseStatement(sql string) ([]string, []ParamsInfo, error) {
	stmts, _ := sqliteparserutils.SplitStatement(sql)

	stmtsParams := make([]ParamsInfo, len(stmts))
	for idx, stmt := range stmts {
		nameParams, positionalParamsCount, err := extractParameters(stmt)
		if err != nil {
			return nil, nil, err
		}
		stmtsParams[idx] = ParamsInfo{nameParams, positionalParamsCount}
	}
	return stmts, stmtsParams, nil
}

func ParseStatementAndArgs(sql string, args []driver.NamedValue) ([]string, []Params, error) {
	parameters, err := ConvertArgs(args)
	if err != nil {
		return nil, nil, err
	}

	stmts, _ := sqliteparserutils.SplitStatement(sql)

	stmtsParams := make([]Params, len(stmts))
	totalParametersAlreadyUsed := 0
	for idx, stmt := range stmts {
		stmtParams, err := generateStatementParameters(stmt, parameters, totalParametersAlreadyUsed)
		if err != nil {
			return nil, nil, fmt.Errorf("fail to generate statement parameter. statement: %s. error: %v", stmt, err)
		}
		stmtsParams[idx] = stmtParams
		totalParametersAlreadyUsed += stmtParams.Len()
	}
	return stmts, stmtsParams, nil
}

type paramsType int

const (
	namedParameters paramsType = iota
	positionalParameters
)

type Params struct {
	positional []any
	named      map[string]any
}

func (p *Params) MarshalJSON() ([]byte, error) {
	if len(p.named) > 0 {
		return json.Marshal(p.named)
	}
	if len(p.positional) > 0 {
		return json.Marshal(p.positional)
	}
	return json.Marshal(make([]any, 0))
}

func (p *Params) Named() map[string]any {
	return p.named
}

func (p *Params) Positional() []any {
	return p.positional
}

func (p *Params) Len() int {
	if p.named != nil {
		return len(p.named)
	}

	return len(p.positional)
}

func (p *Params) Type() paramsType {
	if p.named != nil {
		return namedParameters
	}

	return positionalParameters
}

func NewParams(t paramsType) Params {
	p := Params{}
	switch t {
	case namedParameters:
		p.named = make(map[string]any)
	case positionalParameters:
		p.positional = make([]any, 0)
	}

	return p
}

func getParamType(arg *driver.NamedValue) paramsType {
	if arg.Name == "" {
		return positionalParameters
	}
	return namedParameters
}

func ConvertArgs(args []driver.NamedValue) (Params, error) {
	if len(args) == 0 {
		return NewParams(positionalParameters), nil
	}

	var sortedArgs []*driver.NamedValue
	for idx := range args {
		sortedArgs = append(sortedArgs, &args[idx])
	}
	sort.Slice(sortedArgs, func(i, j int) bool {
		return sortedArgs[i].Ordinal < sortedArgs[j].Ordinal
	})

	parametersType := getParamType(sortedArgs[0])
	parameters := NewParams(parametersType)
	for _, arg := range sortedArgs {
		if parametersType != getParamType(arg) {
			return Params{}, fmt.Errorf("driver does not accept positional and named parameters at the same time")
		}

		switch parametersType {
		case positionalParameters:
			parameters.positional = append(parameters.positional, arg.Value)
		case namedParameters:
			parameters.named[arg.Name] = arg.Value
		}
	}
	return parameters, nil
}

func generateStatementParameters(stmt string, queryParams Params, positionalParametersOffset int) (Params, error) {
	nameParams, positionalParamsCount, err := extractParameters(stmt)
	if err != nil {
		return Params{}, err
	}

	stmtParams := NewParams(queryParams.Type())

	switch queryParams.Type() {
	case positionalParameters:
		if positionalParametersOffset+positionalParamsCount > len(queryParams.positional) {
			return Params{}, fmt.Errorf("missing positional parameters")
		}
		stmtParams.positional = queryParams.positional[positionalParametersOffset : positionalParametersOffset+positionalParamsCount]
	case namedParameters:
		stmtParametersNeeded := make(map[string]bool)
		for _, stmtParametersName := range nameParams {
			stmtParametersNeeded[stmtParametersName] = true
		}
		for queryParamsName, queryParamsValue := range queryParams.named {
			if stmtParametersNeeded[queryParamsName] {
				stmtParams.named[queryParamsName] = queryParamsValue
				delete(stmtParametersNeeded, queryParamsName)
			}
		}
	}

	return stmtParams, nil
}

func extractParameters(stmt string) (nameParams []string, positionalParamsCount int, err error) {
	statementStream := antlr.NewInputStream(stmt)
	sqliteparser.NewSQLiteLexer(statementStream)
	lexer := sqliteparser.NewSQLiteLexer(statementStream)

	allTokens := lexer.GetAllTokens()

	nameParamsSet := make(map[string]bool)

	for _, token := range allTokens {
		tokenType := token.GetTokenType()
		if tokenType == sqliteparser.SQLiteLexerBIND_PARAMETER {
			parameter := token.GetText()

			isPositionalParameter, err := isPositionalParameter(parameter)
			if err != nil {
				return []string{}, 0, err
			}

			if isPositionalParameter {
				positionalParamsCount++
			} else {
				paramWithoutPrefix, err := removeParamPrefix(parameter)
				if err != nil {
					return []string{}, 0, err
				} else {
					nameParamsSet[paramWithoutPrefix] = true
				}
			}
		}
	}

	nameParams = make([]string, 0, len(nameParamsSet))
	for k := range nameParamsSet {
		nameParams = append(nameParams, k)
	}

	return nameParams, positionalParamsCount, nil
}

func isPositionalParameter(param string) (ok bool, err error) {
	re := regexp.MustCompile(`\?([0-9]*).*`)
	match := re.FindSubmatch([]byte(param))
	if match == nil {
		return false, nil
	}

	posS := string(match[1])
	if posS == "" {
		return true, nil
	}

	return true, fmt.Errorf("unsuppoted positional parameter. This driver does not accept positional parameters with indexes (like ?<number>)")
}

func removeParamPrefix(paramName string) (string, error) {
	if paramName[0] == ':' || paramName[0] == '@' || paramName[0] == '$' {
		return paramName[1:], nil
	}
	return "", fmt.Errorf("all named parameters must start with ':', or '@' or '$'")
}
