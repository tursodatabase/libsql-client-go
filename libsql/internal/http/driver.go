package http

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"

	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/libsql/sqlite-antlr4-parser/sqliteparser"
)

type result struct {
	id      int64
	changes int64
}

func (r *result) LastInsertId() (int64, error) {
	return r.id, nil
}

func (r *result) RowsAffected() (int64, error) {
	return r.changes, nil
}

type rows struct {
	resultSets            []httpResults
	currentResultSetIndex int
	currentRowIdx         int
}

func (r *rows) Columns() []string {
	return r.currentResults().Columns
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	currentResults := r.currentResults()
	if r.currentRowIdx == len(currentResults.Rows) {
		return io.EOF
	}
	count := len(currentResults.Rows[r.currentRowIdx])
	for idx := 0; idx < count; idx++ {
		dest[idx] = currentResults.Rows[r.currentRowIdx][idx]
	}
	r.currentRowIdx++
	return nil
}

func (r *rows) HasNextResultSet() bool {
	return r.currentResultSetIndex < len(r.resultSets)-1
}

func (r *rows) NextResultSet() error {
	if !r.HasNextResultSet() {
		return io.EOF
	}

	r.currentResultSetIndex++
	r.currentRowIdx = 0

	currentResultSet := r.resultSets[r.currentResultSetIndex]
	if currentResultSet.Error != nil {
		return fmt.Errorf("failed to execute statement\n%s", currentResultSet.Error.Message)
	}
	if currentResultSet.Results == nil {
		return fmt.Errorf("no results for statement")
	}

	return nil
}

func (r *rows) currentResults() *resultSet {
	return r.resultSets[r.currentResultSetIndex].Results
}

type conn struct {
	url string
	jwt string
}

func Connect(url, jwt string) *conn {
	return &conn{url, jwt}
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare method not implemented")
}

func (c *conn) Close() error {
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("begin method not implemented")
}

func convertArgs(args []driver.NamedValue) (params, error) {
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
			return params{}, fmt.Errorf("driver does not accept positional and named parameters at the same time")
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

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	paramaters, err := convertArgs(args)
	if err != nil {
		return nil, err
	}

	rs, err := callSqld(ctx, c.url, c.jwt, splitStatementToPieces(query), paramaters)
	if err != nil {
		return nil, fmt.Errorf("failed to execute SQL: %s\n%w", query, err)
	}

	if err := assertNoResultWithError(rs, query); err != nil {
		return nil, err
	}

	return &result{0, 0}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	paramaters, err := convertArgs(args)
	if err != nil {
		return nil, err
	}

	rs, err := callSqld(ctx, c.url, c.jwt, splitStatementToPieces(query), paramaters)
	if err != nil {
		return nil, fmt.Errorf("failed to execute SQL: %s\n%w", query, err)
	}
	return &rows{rs, 0, 0}, nil
}

func assertNoResultWithError(resultSets []httpResults, query string) error {
	for _, result := range resultSets {
		if result.Error != nil {
			return fmt.Errorf("failed to execute SQL: %s\n%s", query, result.Error.Message)
		}
		if result.Results == nil {
			return fmt.Errorf("no results for statement")
		}
	}
	return nil
}

func getParamType(arg *driver.NamedValue) paramsType {
	if arg.Name == "" {
		return positionalParameters
	}
	return namedParameters
}

func splitStatementToPieces(statementsString string) (pieces []string) {
	statementStream := antlr.NewInputStream(statementsString)
	sqliteparser.NewSQLiteLexer(statementStream)
	lexer := sqliteparser.NewSQLiteLexer(statementStream)

	allTokens := lexer.GetAllTokens()

	statements := make([]string, 0, 8)

	var currentStatement string
	for _, token := range allTokens {
		tokenType := token.GetTokenType()
		if tokenType == sqliteparser.SQLiteLexerSCOL {
			currentStatement = strings.TrimSpace(currentStatement)
			if currentStatement != "" {
				statements = append(statements, currentStatement)
				currentStatement = ""
			}
		} else {
			currentStatement += token.GetText()
		}
	}

	currentStatement = strings.TrimSpace(currentStatement)
	if currentStatement != "" {
		statements = append(statements, currentStatement)
	}

	return statements
}

func generateStatementParameters(stmt string, queryParams params, positionalParametersOffset int) (params, error) {
	nameParams, positionalParamsCount, err := extractParameters(stmt)
	if err != nil {
		return params{}, err
	}

	stmtParams := NewParams(queryParams.Type())

	switch queryParams.Type() {
	case positionalParameters:
		if positionalParametersOffset+positionalParamsCount > len(queryParams.positional) {
			return params{}, fmt.Errorf("missing positional parameters")
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
	re := regexp.MustCompile("\\?([0-9]*).*")
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
