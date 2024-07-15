package sqliteparserutils_test

import (
	"reflect"
	"testing"

	"github.com/antlr4-go/antlr/v4"

	"github.com/tursodatabase/libsql-client-go/sqliteparser"
	"github.com/tursodatabase/libsql-client-go/sqliteparserutils"
)

func generateSplitStatementExtraInfo(lastTokenType int, incompleteCreateTriggerStatement bool, incompleteMultilineComment bool) sqliteparserutils.SplitStatementExtraInfo {
	return sqliteparserutils.SplitStatementExtraInfo{
		IncompleteCreateTriggerStatement: incompleteCreateTriggerStatement,
		IncompleteMultilineComment:       incompleteMultilineComment,
		LastTokenType:                    lastTokenType,
	}
}

func generateSimpleSplitStatementExtraInfo(lastTokenType int) sqliteparserutils.SplitStatementExtraInfo {
	return sqliteparserutils.SplitStatementExtraInfo{
		LastTokenType: lastTokenType,
	}
}

func TestSplitStatement(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		stmts     []string
		extraInfo sqliteparserutils.SplitStatementExtraInfo
	}{
		{
			name:      "EmptyStatement",
			value:     "",
			stmts:     []string{},
			extraInfo: generateSimpleSplitStatementExtraInfo(antlr.TokenInvalidType),
		},
		{
			name:      "OnlySemicolon",
			value:     ";;;;",
			stmts:     []string{},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerSCOL),
		},
		{
			name:      "SingleStatementWithoutSemicolon",
			value:     "select 1",
			stmts:     []string{"select 1"},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerNUMERIC_LITERAL),
		},
		{
			name:      "SingleStatementWithSemicolon",
			value:     "select 1;",
			stmts:     []string{"select 1"},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerSCOL),
		},
		{
			name:      "SingleStatementEndingWithWhitespaceAndSemicolon",
			value:     "select 1 ;",
			stmts:     []string{"select 1"},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerSCOL),
		},
		{
			name:      "OnlyWithSingleLineComment",
			value:     "-- a simple comment",
			stmts:     []string{},
			extraInfo: generateSimpleSplitStatementExtraInfo(antlr.TokenInvalidType),
		},
		{
			name:      "SingleStatementWithSingleLineCommentWithoutSemicolon",
			value:     "select 1; -- a simple comment",
			stmts:     []string{"select 1"},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerSCOL),
		},
		{
			name:      "SingleStatementWithSingleLineCommentWithSemicolon",
			value:     "select 1; -- ops; a comment with semicolon",
			stmts:     []string{"select 1"},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerSCOL),
		},
		{
			name:      "OnlyMultilineComment",
			value:     "/* a simple comment \n*/",
			stmts:     []string{},
			extraInfo: generateSimpleSplitStatementExtraInfo(antlr.TokenInvalidType),
		},
		{
			name:      "SingleStatementWithCompleteMultilineCommentWithoutSemicolon",
			value:     "select 1; /* \na simple comment \n*/",
			stmts:     []string{"select 1"},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerSCOL),
		},
		{
			name:      "SingleStatementWithCompleteMultilineCommentWithSemicolon",
			value:     "select 1; /* \nops;\n a comment with semicolon \n*/",
			stmts:     []string{"select 1"},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerSCOL),
		},
		{
			name:      "OnlyWithIncompleteMultilineComment",
			value:     "/* a simple comment\n",
			stmts:     []string{},
			extraInfo: generateSplitStatementExtraInfo(antlr.TokenInvalidType, false, true),
		},
		{
			name:      "SingleStatementWithIncompleteMultilineCommentWithoutSemicolon",
			value:     "select 1; /* \na simple comment\n",
			stmts:     []string{"select 1"},
			extraInfo: generateSplitStatementExtraInfo(sqliteparser.SQLiteLexerSCOL, false, true),
		},
		{
			name:      "SingleStatementWithoutSemicolonWithIncompleteMultilineCommentWithoutSemicolon",
			value:     "select 1 /* \na simple comment\n",
			stmts:     []string{"select 1"},
			extraInfo: generateSplitStatementExtraInfo(sqliteparser.SQLiteLexerNUMERIC_LITERAL, false, true),
		},
		{
			name:      "SingleStatementWithIncompleteMultilineCommentWithSemicolon",
			value:     "select 1; /* \na simple comment\n",
			stmts:     []string{"select 1"},
			extraInfo: generateSplitStatementExtraInfo(sqliteparser.SQLiteLexerSCOL, false, true),
		},
		{
			name:      "MultipleStatementsWithMultilineCommentBetween",
			value:     "select 1; /* \na simple comment;\n*/ select 2;",
			stmts:     []string{"select 1", "select 2"},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerSCOL),
		},
		{
			name:      "MultipleCorrectStatements",
			value:     "select 1; INSERT INTO counter(country, city, value) VALUES(?, ?, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = ? AND city = ?; select 2",
			stmts:     []string{"select 1", "INSERT INTO counter(country, city, value) VALUES(?, ?, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = ? AND city = ?", "select 2"},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerNUMERIC_LITERAL),
		},
		{
			name:      "MultipleWrongStatements",
			value:     "select from table; INSERT counter(country, city, value) VALUES(?, ?, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = ? AND city = ?; create something",
			stmts:     []string{"select from table", "INSERT counter(country, city, value) VALUES(?, ?, 1) ON CONFLICT DO UPDATE SET value = IFNULL(value, 0) + 1 WHERE country = ? AND city = ?", "create something"},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerIDENTIFIER),
		},
		{
			name:      "MultipleWrongTokens",
			value:     "sdfasdfigosdfg sadfgsd ggsadgf; sdfasdfasd; 1230kfvcasd; 213 dsf s 0 fs229dt",
			stmts:     []string{"sdfasdfigosdfg sadfgsd ggsadgf", "sdfasdfasd", "1230kfvcasd", "213 dsf s 0 fs229dt"},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerIDENTIFIER),
		},
		{
			name:      "MultipleSemicolonsBetweenStatements",
			value:     "select 1;;;;;; ;;; ; ; ; ; select 2",
			stmts:     []string{"select 1", "select 2"},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerNUMERIC_LITERAL),
		},
		{
			name:      "CompleteCreateTriggerStatement",
			value:     "CREATE TRIGGER update_updated_at AFTER UPDATE ON users FOR EACH ROW BEGIN UPDATE users SET updated_at = 0 WHERE id = NEW.id; END",
			stmts:     []string{"CREATE TRIGGER update_updated_at AFTER UPDATE ON users FOR EACH ROW BEGIN UPDATE users SET updated_at = 0 WHERE id = NEW.id; END"},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerEND_),
		},
		{
			name:      "CompleteCreateTempTriggerStatement",
			value:     "CREATE TEMP TRIGGER update_updated_at AFTER UPDATE ON users FOR EACH ROW BEGIN UPDATE users SET updated_at = 0 WHERE id = NEW.id; END",
			stmts:     []string{"CREATE TEMP TRIGGER update_updated_at AFTER UPDATE ON users FOR EACH ROW BEGIN UPDATE users SET updated_at = 0 WHERE id = NEW.id; END"},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerEND_),
		},
		{
			name:      "IncompleteCreateTriggerStatement",
			value:     "CREATE TRIGGER update_updated_at AFTER UPDATE ON users FOR EACH ROW BEGIN UPDATE users SET updated_at = 0 WHERE id = NEW.id;",
			stmts:     []string{"CREATE TRIGGER update_updated_at AFTER UPDATE ON users FOR EACH ROW BEGIN UPDATE users SET updated_at = 0 WHERE id = NEW.id;"},
			extraInfo: generateSplitStatementExtraInfo(sqliteparser.SQLiteLexerSCOL, true, false),
		},
		{
			name:      "IncompleteCreateTriggerStatementWithIncompleteMultilineComment",
			value:     "CREATE TRIGGER update_updated_at AFTER UPDATE ON users FOR EACH ROW BEGIN UPDATE users SET updated_at = 0 WHERE id = NEW.id; /* this is the trigger;",
			stmts:     []string{"CREATE TRIGGER update_updated_at AFTER UPDATE ON users FOR EACH ROW BEGIN UPDATE users SET updated_at = 0 WHERE id = NEW.id;"},
			extraInfo: generateSplitStatementExtraInfo(sqliteparser.SQLiteLexerSCOL, true, true),
		},
		{
			name:  "CompleteTransactionStatement",
			value: "BEGIN TRANSACTION; CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO test_table (value) VALUES ('Value 1'); COMMIT;",
			stmts: []string{"BEGIN TRANSACTION",
				"CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)",
				"INSERT INTO test_table (value) VALUES ('Value 1')",
				"COMMIT",
			},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerSCOL),
		},
		{
			name:  "IncompleteTransactionStatement",
			value: "BEGIN TRANSACTION; CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO test_table (value) VALUES ('Value 1');",
			stmts: []string{"BEGIN TRANSACTION",
				"CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)",
				"INSERT INTO test_table (value) VALUES ('Value 1')",
			},
			extraInfo: generateSimpleSplitStatementExtraInfo(sqliteparser.SQLiteLexerSCOL),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStmts, gotExtraInfo := sqliteparserutils.SplitStatement(tt.value)
			if !reflect.DeepEqual(gotStmts, tt.stmts) {
				t.Errorf("got %#v, want %#v", gotStmts, tt.stmts)
			}
			if !reflect.DeepEqual(gotExtraInfo, tt.extraInfo) {
				t.Errorf("got %#v, want %#v", gotExtraInfo, tt.extraInfo)
			}
		})
	}
}
