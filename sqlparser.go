// Package sqlparser is a high-performance, zero-allocation SQL parser for Go.
//
// Design goals:
//   - Zero heap allocations in the hot path (lexer)
//   - O(1) keyword recognition via length-bucketed tables
//   - Pratt (top-down operator precedence) expression parser
//   - Arena allocator eliminates per-node GC pressure
//   - Supports MySQL, PostgreSQL, SQLite, and standard SQL dialects
//   - Full DDL + DML coverage
//
// Usage:
//
//	stmt, err := sqlparser.ParseStatement("SELECT id, name FROM users WHERE id = 1")
//	stmts, err := sqlparser.ParseStatements(sql)
//	p := sqlparser.NewParser(src)
//	for stmt := range p.Iter() { ... }
package sqlparser

import (
	"github.com/oarkflow/sqlparser/ast"
	"github.com/oarkflow/sqlparser/lexer"
	"github.com/oarkflow/sqlparser/parser"
)

// Re-export core types so callers only import this package.
type (
	Statement          = ast.Statement
	Expr               = ast.Expr
	SelectStmt         = ast.SelectStmt
	InsertStmt         = ast.InsertStmt
	UpdateStmt         = ast.UpdateStmt
	DeleteStmt         = ast.DeleteStmt
	CreateTableStmt    = ast.CreateTableStmt
	CreateDatabaseStmt = ast.CreateDatabaseStmt
	AlterDatabaseStmt  = ast.AlterDatabaseStmt
	DropDatabaseStmt   = ast.DropDatabaseStmt
	AlterTableStmt     = ast.AlterTableStmt
	DropTableStmt      = ast.DropTableStmt
	CallStmt           = ast.CallStmt
	TransactionStmt    = ast.TransactionStmt
	GenericDDLStmt     = ast.GenericDDLStmt
	ParseError         = parser.ParseError
	Token              = lexer.Token
	TokenType          = lexer.TokenType
)

// ParseStatement parses a single SQL statement from a string.
// It returns the AST node and any parse error.
func ParseStatement(sql string) (Statement, error) {
	return parser.ParseStatement(sql)
}

// ParseStatements parses multiple semicolon-separated SQL statements.
func ParseStatements(sql string) ([]Statement, error) {
	return parser.ParseStatements(sql)
}

// Parser is a reusable, stateful SQL parser.
// Reuse a Parser across calls to amortise arena allocations.
type Parser struct {
	p *parser.Parser
}

// New creates a Parser backed by the given SQL bytes.
func New(src []byte) *Parser {
	return &Parser{p: parser.New(src)}
}

// NewString creates a Parser backed by the given SQL string.
func NewString(src string) *Parser {
	return &Parser{p: parser.NewString(src)}
}

// Reset reuses the Parser with new input, reusing internal allocations.
func (p *Parser) Reset(src []byte) {
	p.p.Reset(src)
}

// Next returns the next statement or (nil, nil) at EOF.
func (p *Parser) Next() (Statement, error) {
	return p.p.ParseOne()
}

// All parses all remaining statements.
func (p *Parser) All() ([]Statement, error) {
	return p.p.ParseAll()
}

// Tokenize breaks a SQL string into tokens.
// The returned slice is backed by the original byte slice to avoid copies.
// Provide a pre-allocated buffer to avoid heap allocation:
//
//	buf := make([]lexer.Token, 0, 128)
//	tokens := sqlparser.Tokenize([]byte(sql), buf)
func Tokenize(src []byte, buf []Token) []Token {
	return lexer.Tokenize(src, buf)
}
