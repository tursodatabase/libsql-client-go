package sqliteparserutils

import (
	"github.com/antlr4-go/antlr/v4"

	"github.com/tursodatabase/libsql-client-go/sqliteparser"
)

// TODO: Shell test begin transaction on shell

type SplitStatementExtraInfo struct {
	IncompleteCreateTriggerStatement bool
	IncompleteMultilineComment       bool
	LastTokenType                    int
}

type StatementIterator struct {
	tokenizer    *bufferedTokenizer
	currentToken antlr.Token
}

func CreateStatementIterator(statement string) *StatementIterator {
	return &StatementIterator{tokenizer: createStringTokenizer(statement)}
}

func (iterator *StatementIterator) Next() (statement string, extraInfo SplitStatementExtraInfo, isEOF bool) {
	var (
		insideCreateTriggerStmt = false
		insideMultilineComment  = false
		startPosition           = -1
		previousToken           = iterator.currentToken
	)
	for !iterator.tokenizer.IsEOF() {
		// We break loop here because we're sure multiline comment didn't finished, otherwise lexer would have just ignored it
		if atIncompleteMultilineCommentStart(iterator.tokenizer) {
			insideMultilineComment = true
			break
		}
		iterator.currentToken = iterator.tokenizer.Get(0)
		// skip empty statements (e.g. ... ; /* some comment */ ; ... )
		if startPosition == -1 && iterator.currentToken.GetTokenType() == sqliteparser.SQLiteLexerSCOL {
			iterator.tokenizer.Consume()
			continue
		}
		if startPosition == -1 {
			// initialize current statement start position
			insideCreateTriggerStmt = atCreateTriggerStart(iterator.tokenizer)
			startPosition = iterator.currentToken.GetStart()
		} else if insideCreateTriggerStmt {
			// extend trigger creation statement to include END token after last semicolon
			if iterator.currentToken.GetTokenType() == sqliteparser.SQLiteLexerEND_ {
				insideCreateTriggerStmt = false
			}
		} else if iterator.currentToken.GetTokenType() == sqliteparser.SQLiteLexerSCOL {
			// finish current statement (don't forget to consume as we are breaking here)
			iterator.tokenizer.Consume()
			break
		}
		previousToken = iterator.currentToken
		iterator.tokenizer.Consume()
	}
	lastTokenType := antlr.TokenInvalidType
	if iterator.currentToken != nil {
		lastTokenType = iterator.currentToken.GetTokenType()
	}
	extraInfo = SplitStatementExtraInfo{
		IncompleteCreateTriggerStatement: insideCreateTriggerStmt,
		IncompleteMultilineComment:       insideMultilineComment,
		LastTokenType:                    lastTokenType,
	}
	statement = ""
	if startPosition != -1 {
		statement = iterator.tokenizer.source.GetInputStream().GetText(startPosition, previousToken.GetStop())
	}
	return statement, extraInfo, iterator.tokenizer.IsEOF() || insideMultilineComment
}

func SplitStatement(statement string) (statements []string, extraInfo SplitStatementExtraInfo) {
	iterator := CreateStatementIterator(statement)

	statements = make([]string, 0)
	for {
		statement, extraInfo, isEOF := iterator.Next()
		if statement != "" {
			statements = append(statements, statement)
		}
		if isEOF {
			return statements, extraInfo
		}
	}
}

func atCreateTriggerStart(tokenStream *bufferedTokenizer) bool {
	token1 := tokenStream.Get(0).GetTokenType()
	token2 := tokenStream.Get(1).GetTokenType()
	token3 := tokenStream.Get(2).GetTokenType()

	if token1 == sqliteparser.SQLiteLexerCREATE_ && token2 == sqliteparser.SQLiteLexerTRIGGER_ {
		return true
	}
	if token1 == sqliteparser.SQLiteLexerCREATE_ &&
		(token2 == sqliteparser.SQLiteLexerTEMP_ || token2 == sqliteparser.SQLiteLexerTEMPORARY_) &&
		token3 == sqliteparser.SQLiteLexerTRIGGER_ {
		return true
	}
	return false
}

// Note: Only starts for incomplete multiline comments will be detected cause lexer automatically ignores complete
// multiline comments
func atIncompleteMultilineCommentStart(stream *bufferedTokenizer) bool {
	token1 := stream.Get(0).GetTokenType()
	token2 := stream.Get(1).GetTokenType()
	return token1 == sqliteparser.SQLiteLexerDIV && token2 == sqliteparser.SQLiteLexerSTAR
}

func createStringTokenizer(statement string) *bufferedTokenizer {
	statementStream := antlr.NewInputStream(statement)

	lexer := sqliteparser.NewSQLiteLexer(statementStream)
	return createBufferedTokenizer(lexer, 3)
}
