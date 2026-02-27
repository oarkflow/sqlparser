// Package parser provides a high-performance, zero-allocation SQL parser.
// It uses a hand-rolled recursive descent strategy with a one-token lookahead
// and an arena allocator to minimise GC pressure.
package parser

import (
	"bytes"
	"fmt"
	"strconv"
	"unsafe"

	"github.com/oarkflow/sqlparser/ast"
	"github.com/oarkflow/sqlparser/lexer"
)

// ParseError records a parse failure.
type ParseError struct {
	Msg  string
	Pos  int32
	Line uint32
	Col  uint32
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error at line %d col %d: %s", e.Line, e.Col, e.Msg)
}

// Parser converts a stream of tokens into an AST.
// It maintains a 2-token lookahead for decisions that require peeking ahead.
type Parser struct {
	lex     *lexer.Lexer
	tok     lexer.Token // current (already consumed from lexer)
	peek    lexer.Token // one ahead
	hasPeek bool

	// arena is a monotonic allocator that owns all AST node memory.
	// Reusing the arena across parse calls (after Reset) avoids GC spikes.
	arena arena
}

// New creates a Parser for the given SQL bytes.
func New(src []byte) *Parser {
	p := &Parser{}
	p.lex = lexer.New(src)
	p.tok = p.lex.Next()
	return p
}

// NewString creates a Parser for a SQL string.
func NewString(src string) *Parser {
	p := &Parser{}
	p.lex = lexer.NewString(src)
	p.tok = p.lex.Next()
	return p
}

// Reset reuses the parser with new input, reusing internal memory.
func (p *Parser) Reset(src []byte) {
	if p.lex == nil {
		p.lex = lexer.New(src)
	} else {
		p.lex.Reset(src)
	}
	p.tok = p.lex.Next()
	p.hasPeek = false
	p.arena.reset()
}

// ParseOne parses a single SQL statement.
func (p *Parser) ParseOne() (ast.Statement, error) {
	p.skipSemis()
	if p.tok.Type == lexer.EOF {
		return nil, nil
	}
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	p.skipSemis()
	return stmt, nil
}

// ParseAll parses all statements separated by semicolons.
func (p *Parser) ParseAll() ([]ast.Statement, error) {
	var stmts []ast.Statement
	for {
		p.skipSemis()
		if p.tok.Type == lexer.EOF {
			break
		}
		stmt, err := p.parseStatement()
		if err != nil {
			return stmts, err
		}
		stmts = append(stmts, stmt)
	}
	return stmts, nil
}

// ParseStatement is the public entrypoint for parsing a single statement.
func ParseStatement(src string) (ast.Statement, error) {
	p := NewString(src)
	return p.ParseOne()
}

// ParseStatements parses multiple statements.
func ParseStatements(src string) ([]ast.Statement, error) {
	p := NewString(src)
	return p.ParseAll()
}

// ---- internal helpers ----

func (p *Parser) advance() lexer.Token {
	prev := p.tok
	if p.hasPeek {
		p.tok = p.peek
		p.hasPeek = false
	} else {
		p.tok = p.lex.Next()
	}
	return prev
}

func (p *Parser) peekToken() lexer.Token {
	if !p.hasPeek {
		p.peek = p.lex.Next()
		p.hasPeek = true
	}
	return p.peek
}

func (p *Parser) skipSemis() {
	for p.tok.Type == lexer.SEMICOLON {
		p.advance()
	}
}

func (p *Parser) is(typ lexer.TokenType) bool {
	return p.tok.Type == typ
}

func (p *Parser) isKeyword(kw lexer.TokenType) bool {
	return p.tok.Type == kw
}

func (p *Parser) eat(typ lexer.TokenType) (lexer.Token, error) {
	if p.tok.Type != typ {
		return p.tok, p.errorf("expected %s, got %s (%q)", typ, p.tok.Type, p.tok.Raw)
	}
	return p.advance(), nil
}

func (p *Parser) eatKeyword(kw lexer.TokenType) error {
	if p.tok.Type != kw {
		return p.errorf("expected keyword %s, got %q", kw, p.tok.Raw)
	}
	p.advance()
	return nil
}

func (p *Parser) tryEat(typ lexer.TokenType) bool {
	if p.tok.Type == typ {
		p.advance()
		return true
	}
	return false
}

func (p *Parser) tryEatKeyword(kw lexer.TokenType) bool {
	if p.tok.Type == kw {
		p.advance()
		return true
	}
	return false
}

func (p *Parser) errorf(format string, args ...any) *ParseError {
	return &ParseError{
		Msg:  fmt.Sprintf(format, args...),
		Pos:  p.tok.Pos,
		Line: p.tok.Line,
		Col:  p.tok.Col,
	}
}

func arenaNode[T any](a *arena, v T) *T {
	n := (*T)(a.allocPtr(unsafe.Sizeof(v)))
	*n = v
	return n
}

// ---- statement dispatch ----

func (p *Parser) parseStatement() (ast.Statement, error) {
	switch p.tok.Type {
	case lexer.SELECT:
		return p.parseSelect()
	case lexer.WITH:
		return p.parseWithStatement()
	case lexer.INSERT:
		return p.parseInsert()
	case lexer.REPLACE:
		return p.parseReplace()
	case lexer.UPDATE:
		return p.parseUpdate()
	case lexer.DELETE:
		return p.parseDelete()
	case lexer.CREATE:
		return p.parseCreate()
	case lexer.ALTER:
		return p.parseAlter()
	case lexer.DROP:
		return p.parseDrop()
	case lexer.TRUNCATE:
		return p.parseTruncate()
	case lexer.USE:
		return p.parseUse()
	case lexer.ROLLBACK:
		return p.parseRollback()
	case lexer.SET:
		return p.parseSetStmt()
	case lexer.SHOW:
		return p.parseShow()
	case lexer.EXPLAIN:
		return p.parseExplain()
	case lexer.IDENT:
		return p.parseIdentLedStatement()
	default:
		return nil, p.errorf("unexpected token %q at start of statement", p.tok.Raw)
	}
}

func (p *Parser) parseWithStatement() (ast.Statement, error) {
	with, err := p.parseWith()
	if err != nil {
		return nil, err
	}
	switch p.tok.Type {
	case lexer.SELECT:
		stmt, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		stmt.With = with
		return stmt, nil
	case lexer.INSERT:
		stmt, err := p.parseInsert()
		if err != nil {
			return nil, err
		}
		stmt.With = with
		return stmt, nil
	case lexer.REPLACE:
		stmt, err := p.parseReplace()
		if err != nil {
			return nil, err
		}
		stmt.With = with
		return stmt, nil
	case lexer.UPDATE:
		stmt, err := p.parseUpdate()
		if err != nil {
			return nil, err
		}
		stmt.With = with
		return stmt, nil
	case lexer.DELETE:
		stmt, err := p.parseDelete()
		if err != nil {
			return nil, err
		}
		stmt.With = with
		return stmt, nil
	default:
		return nil, p.errorf("WITH must be followed by SELECT/INSERT/UPDATE/DELETE, got %q", p.tok.Raw)
	}
}

func (p *Parser) parseIdentLedStatement() (ast.Statement, error) {
	switch {
	case equalASCIIFold(p.tok.Raw, "begin"):
		return p.parseBegin()
	case equalASCIIFold(p.tok.Raw, "commit"):
		return p.parseCommit()
	case equalASCIIFold(p.tok.Raw, "start"):
		return p.parseStartTransaction()
	case equalASCIIFold(p.tok.Raw, "savepoint"):
		return p.parseSavepoint()
	case equalASCIIFold(p.tok.Raw, "release"):
		return p.parseReleaseSavepoint()
	case equalASCIIFold(p.tok.Raw, "call"):
		return p.parseCall()
	default:
		return nil, p.errorf("unexpected token %q at start of statement", p.tok.Raw)
	}
}

// ---- SELECT ----

func (p *Parser) parseSelect() (*ast.SelectStmt, error) {
	pos := p.tok.Pos
	var with *ast.WithClause
	var err error
	if p.is(lexer.WITH) {
		with, err = p.parseWith()
		if err != nil {
			return nil, err
		}
	}
	stmt, err := p.parseSelectCore(pos)
	if err != nil {
		return nil, err
	}
	stmt.With = with

	// Handle UNION / INTERSECT / EXCEPT
	for {
		var op ast.SetOp
		switch p.tok.Type {
		case lexer.UNION:
			op = ast.Union
		case lexer.INTERSECT:
			op = ast.Intersect
		case lexer.EXCEPT:
			op = ast.Except
		default:
			return stmt, nil
		}
		p.advance()
		all := p.tryEatKeyword(lexer.ALL)
		right, err := p.parseSelectCore(p.tok.Pos)
		if err != nil {
			return nil, err
		}
		cur := stmt
		for cur.SetOp != nil {
			cur = cur.SetOp.Right
		}
		cur.SetOp = arenaNode(&p.arena, ast.SetOperation{Op: op, All: all, Right: right})
	}
}

func (p *Parser) parseSelectCore(pos int32) (*ast.SelectStmt, error) {
	if err := p.eatKeyword(lexer.SELECT); err != nil {
		return nil, err
	}
	stmt := arenaNode(&p.arena, ast.SelectStmt{TokPos: pos})
	stmt.Distinct = p.tryEatKeyword(lexer.DISTINCT)
	_ = p.tryEatKeyword(lexer.ALL)

	// Column list
	cols, err := p.parseSelectColumns()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols

	// FROM
	if p.tryEatKeyword(lexer.FROM) {
		refs, err := p.parseTableRefs()
		if err != nil {
			return nil, err
		}
		stmt.From = refs
	}

	// WHERE
	if p.tryEatKeyword(lexer.WHERE) {
		where, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// GROUP BY
	if p.is(lexer.GROUP) && p.peekToken().Type == lexer.BY {
		p.advance()
		p.advance()
		grp, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = grp
	}

	// HAVING
	if p.tryEatKeyword(lexer.HAVING) {
		hav, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		stmt.Having = hav
	}

	// ORDER BY
	if p.is(lexer.ORDER) && p.peekToken().Type == lexer.BY {
		p.advance()
		p.advance()
		ord, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = ord
	}

	// LIMIT / OFFSET
	if p.tryEatKeyword(lexer.LIMIT) {
		lim, err := p.parseLimit()
		if err != nil {
			return nil, err
		}
		stmt.Limit = lim
	}

	return stmt, nil
}

func (p *Parser) parseWith() (*ast.WithClause, error) {
	p.advance() // WITH
	w := arenaNode(&p.arena, ast.WithClause{})
	w.Recursive = p.tryEatKeyword(lexer.RECURSIVE)
	for {
		name, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		cte := ast.CTE{Name: name}
		if p.is(lexer.LPAREN) && p.peekToken().Type == lexer.IDENT {
			// column list
			p.advance()
			cols, err := p.parseIdentList()
			if err != nil {
				return nil, err
			}
			cte.Columns = cols
			if _, err := p.eat(lexer.RPAREN); err != nil {
				return nil, err
			}
		}
		if err := p.eatKeyword(lexer.AS); err != nil {
			return nil, err
		}
		if _, err := p.eat(lexer.LPAREN); err != nil {
			return nil, err
		}
		sq, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		cte.Subq = sq
		if _, err := p.eat(lexer.RPAREN); err != nil {
			return nil, err
		}
		w.CTEs = arenaAppend(&p.arena, w.CTEs, cte)
		if !p.tryEat(lexer.COMMA) {
			break
		}
	}
	return w, nil
}

func (p *Parser) parseSelectColumns() ([]ast.SelectColumn, error) {
	var cols []ast.SelectColumn
	for {
		col, err := p.parseSelectColumn()
		if err != nil {
			return nil, err
		}
		cols = arenaAppend(&p.arena, cols, col)
		if !p.tryEat(lexer.COMMA) {
			break
		}
	}
	return cols, nil
}

func (p *Parser) parseSelectColumn() (ast.SelectColumn, error) {
	if p.is(lexer.STAR) {
		p.advance()
		return ast.SelectColumn{Star: true, Expr: arenaNode(&p.arena, ast.StarExpr{TokPos: p.tok.Pos})}, nil
	}
	expr, err := p.parseExpr(0)
	if err != nil {
		return ast.SelectColumn{}, err
	}
	col := ast.SelectColumn{Expr: expr}
	if p.tryEatKeyword(lexer.AS) || p.is(lexer.IDENT) || p.is(lexer.BACKTICK) || p.is(lexer.DQUOTE) {
		alias, err := p.parseIdent()
		if err != nil {
			return ast.SelectColumn{}, err
		}
		col.Alias = alias
	}
	return col, nil
}

// ---- Table references ----

func (p *Parser) parseTableRefs() ([]ast.TableRef, error) {
	ref, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	var refs []ast.TableRef
	refs = arenaAppend(&p.arena, refs, ref)
	for p.tryEat(lexer.COMMA) {
		ref, err := p.parseTableRef()
		if err != nil {
			return nil, err
		}
		refs = arenaAppend(&p.arena, refs, ref)
	}
	return refs, nil
}

func (p *Parser) parseTableRef() (ast.TableRef, error) {
	var left ast.TableRef
	var err error
	if p.is(lexer.LPAREN) {
		p.advance()
		if p.is(lexer.SELECT) || p.is(lexer.WITH) {
			sq, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			if _, err := p.eat(lexer.RPAREN); err != nil {
				return nil, err
			}
			sub := arenaNode(&p.arena, ast.SubqueryTable{Subq: sq, TokPos: sq.TokPos})
			sub.Alias, _ = p.parseOptionalAlias()
			left = sub
		} else {
			// Parenthesized join
			inner, err := p.parseTableRef()
			if err != nil {
				return nil, err
			}
			if _, err := p.eat(lexer.RPAREN); err != nil {
				return nil, err
			}
			left = inner
		}
	} else {
		name, err := p.parseQualifiedIdent()
		if err != nil {
			return nil, err
		}
		st := arenaNode(&p.arena, ast.SimpleTable{Name: name})
		st.Alias, _ = p.parseOptionalAlias()
		left = st
	}

	// JOIN chains
	for {
		left, err = p.parseJoin(left)
		if err != nil {
			return nil, err
		}
		if _, ok := left.(*ast.JoinTable); !ok {
			break
		}
		// keep chaining
		switch p.tok.Type {
		case lexer.INNER, lexer.LEFT, lexer.RIGHT, lexer.FULL, lexer.CROSS, lexer.NATURAL, lexer.JOIN:
			continue
		}
		break
	}
	return left, nil
}

func (p *Parser) parseJoin(left ast.TableRef) (ast.TableRef, error) {
	var kind ast.JoinKind
	switch p.tok.Type {
	case lexer.INNER:
		p.advance()
		if err := p.eatKeyword(lexer.JOIN); err != nil {
			return nil, err
		}
		kind = ast.InnerJoin
	case lexer.LEFT:
		p.advance()
		p.tryEatKeyword(lexer.OUTER)
		if err := p.eatKeyword(lexer.JOIN); err != nil {
			return nil, err
		}
		kind = ast.LeftJoin
	case lexer.RIGHT:
		p.advance()
		p.tryEatKeyword(lexer.OUTER)
		if err := p.eatKeyword(lexer.JOIN); err != nil {
			return nil, err
		}
		kind = ast.RightJoin
	case lexer.FULL:
		p.advance()
		p.tryEatKeyword(lexer.OUTER)
		if err := p.eatKeyword(lexer.JOIN); err != nil {
			return nil, err
		}
		kind = ast.FullJoin
	case lexer.CROSS:
		p.advance()
		if err := p.eatKeyword(lexer.JOIN); err != nil {
			return nil, err
		}
		kind = ast.CrossJoin
	case lexer.NATURAL:
		p.advance()
		if err := p.eatKeyword(lexer.JOIN); err != nil {
			return nil, err
		}
		kind = ast.NaturalJoin
	case lexer.JOIN:
		p.advance()
		kind = ast.InnerJoin
	default:
		return left, nil
	}
	pos := p.tok.Pos
	right, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	jt := arenaNode(&p.arena, ast.JoinTable{Left: left, Right: right, Kind: kind, TokPos: pos})
	if p.tryEatKeyword(lexer.ON) {
		cond, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		jt.On = cond
	} else if p.tryEatKeyword(lexer.USING) {
		if _, err := p.eat(lexer.LPAREN); err != nil {
			return nil, err
		}
		cols, err := p.parseIdentList()
		if err != nil {
			return nil, err
		}
		jt.Using = cols
		if _, err := p.eat(lexer.RPAREN); err != nil {
			return nil, err
		}
	}
	return jt, nil
}

func (p *Parser) parseOptionalAlias() (*ast.Ident, error) {
	p.tryEatKeyword(lexer.AS)
	if p.is(lexer.IDENT) || p.is(lexer.BACKTICK) || p.is(lexer.DQUOTE) {
		return p.parseIdent()
	}
	return nil, nil
}

// ---- Expression parsing (Pratt / top-down operator precedence) ----

type precedence int

const (
	precLowest     precedence = 0
	precOr         precedence = 1
	precAnd        precedence = 2
	precNot        precedence = 3
	precComparison precedence = 4
	precBitOr      precedence = 5
	precBitAnd     precedence = 6
	precShift      precedence = 7
	precAddSub     precedence = 8
	precMulDiv     precedence = 9
	precUnary      precedence = 10
	precPostfix    precedence = 11
)

func tokenPrec(t lexer.TokenType) (precedence, bool) {
	switch t {
	case lexer.OR:
		return precOr, true
	case lexer.AND, lexer.DAMP:
		return precAnd, true
	case lexer.EQ, lexer.NEQ, lexer.LT, lexer.GT, lexer.LTE, lexer.GTE:
		return precComparison, true
	case lexer.ATGT, lexer.LTAT, lexer.QUESTION, lexer.QMARKPIPE, lexer.QMARKAMP:
		return precComparison, true
	case lexer.PIPE:
		return precBitOr, true
	case lexer.CARET:
		return precBitOr, true
	case lexer.AMPERSAND:
		return precBitAnd, true
	case lexer.LSHIFT, lexer.RSHIFT:
		return precShift, true
	case lexer.PLUS, lexer.MINUS:
		return precAddSub, true
	case lexer.STAR, lexer.SLASH, lexer.PERCENT:
		return precMulDiv, true
	case lexer.DBAR: // || is concat in std SQL
		return precAddSub, true
	case lexer.ARROW, lexer.DARROW2, lexer.HASHARROW, lexer.HASHDARROW:
		return precPostfix, true
	}
	return 0, false
}

func (p *Parser) parseExpr(minPrec precedence) (ast.Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for {
		// Infix / postfix operators
		switch p.tok.Type {
		case lexer.IS:
			pos := p.tok.Pos
			p.advance()
			not := p.tryEatKeyword(lexer.NOT)
			if _, err := p.eat(lexer.NULL_KW); err != nil {
				return nil, err
			}
			left = arenaNode(&p.arena, ast.IsNullExpr{Expr: left, Not: not, TokPos: pos})
			continue

		case lexer.NOT:
			pos := p.tok.Pos
			switch p.peekToken().Type {
			case lexer.LIKE:
				p.advance()
				p.advance()
				right, err := p.parseExpr(precMulDiv)
				if err != nil {
					return nil, err
				}
				like := arenaNode(&p.arena, ast.LikeExpr{Expr: left, Pattern: right, Not: true, TokPos: pos})
				if p.tryEatKeyword(lexer.ESCAPE) {
					esc, err := p.parseExpr(precMulDiv)
					if err != nil {
						return nil, err
					}
					like.Escape = esc
				}
				left = like
				continue
			case lexer.IN:
				p.advance()
				p.advance()
				inExpr, err := p.parseInRHS(left, pos, true)
				if err != nil {
					return nil, err
				}
				left = inExpr
				continue
			case lexer.BETWEEN:
				p.advance()
				p.advance()
				lo, err := p.parseExpr(precComparison + 1)
				if err != nil {
					return nil, err
				}
				if err := p.eatKeyword(lexer.AND); err != nil {
					return nil, err
				}
				hi, err := p.parseExpr(precComparison + 1)
				if err != nil {
					return nil, err
				}
				left = arenaNode(&p.arena, ast.BetweenExpr{Expr: left, Lo: lo, Hi: hi, Not: true, TokPos: pos})
				continue
			}

		case lexer.LIKE:
			pos := p.tok.Pos
			p.advance()
			right, err := p.parseExpr(precMulDiv)
			if err != nil {
				return nil, err
			}
			like := arenaNode(&p.arena, ast.LikeExpr{Expr: left, Pattern: right, TokPos: pos})
			if p.tryEatKeyword(lexer.ESCAPE) {
				esc, err := p.parseExpr(precMulDiv)
				if err != nil {
					return nil, err
				}
				like.Escape = esc
			}
			left = like
			continue

		case lexer.IN:
			pos := p.tok.Pos
			p.advance()
			inExpr, err := p.parseInRHS(left, pos, false)
			if err != nil {
				return nil, err
			}
			left = inExpr
			continue

		case lexer.BETWEEN:
			pos := p.tok.Pos
			p.advance()
			lo, err := p.parseExpr(precComparison + 1)
			if err != nil {
				return nil, err
			}
			if err := p.eatKeyword(lexer.AND); err != nil {
				return nil, err
			}
			hi, err := p.parseExpr(precComparison + 1)
			if err != nil {
				return nil, err
			}
			left = arenaNode(&p.arena, ast.BetweenExpr{Expr: left, Lo: lo, Hi: hi, TokPos: pos})
			continue
		}

		// Standard binary operators
		prec, ok := tokenPrec(p.tok.Type)
		if !ok || prec <= minPrec {
			break
		}
		op := p.tok.Type
		pos := p.tok.Pos
		p.advance()
		right, err := p.parseExpr(prec)
		if err != nil {
			return nil, err
		}
		left = arenaNode(&p.arena, ast.BinaryExpr{Left: left, Right: right, Op: op, TokPos: pos})
	}
	return left, nil
}

func (p *Parser) parseInRHS(left ast.Expr, pos int32, not bool) (ast.Expr, error) {
	if _, err := p.eat(lexer.LPAREN); err != nil {
		return nil, err
	}
	inExpr := arenaNode(&p.arena, ast.InExpr{Expr: left, Not: not, TokPos: pos})
	if p.is(lexer.SELECT) || p.is(lexer.WITH) {
		sq, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		inExpr.Subq = sq
	} else {
		list, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		inExpr.List = list
	}
	if _, err := p.eat(lexer.RPAREN); err != nil {
		return nil, err
	}
	return inExpr, nil
}

func (p *Parser) parseUnary() (ast.Expr, error) {
	switch p.tok.Type {
	case lexer.MINUS:
		pos := p.tok.Pos
		p.advance()
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return arenaNode(&p.arena, ast.UnaryExpr{Expr: expr, Op: lexer.MINUS, TokPos: pos}), nil
	case lexer.PLUS:
		pos := p.tok.Pos
		p.advance()
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return arenaNode(&p.arena, ast.UnaryExpr{Expr: expr, Op: lexer.PLUS, TokPos: pos}), nil
	case lexer.TILDE:
		pos := p.tok.Pos
		p.advance()
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return arenaNode(&p.arena, ast.UnaryExpr{Expr: expr, Op: lexer.TILDE, TokPos: pos}), nil
	case lexer.NOT:
		pos := p.tok.Pos
		p.advance()
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return arenaNode(&p.arena, ast.UnaryExpr{Expr: expr, Op: lexer.NOT, TokPos: pos}), nil
	case lexer.EXISTS:
		pos := p.tok.Pos
		p.advance()
		if _, err := p.eat(lexer.LPAREN); err != nil {
			return nil, err
		}
		sq, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		if _, err := p.eat(lexer.RPAREN); err != nil {
			return nil, err
		}
		return arenaNode(&p.arena, ast.ExistsExpr{Subq: sq, TokPos: pos}), nil
	}
	return p.parsePrimary()
}

func (p *Parser) parsePrimary() (ast.Expr, error) {
	switch p.tok.Type {
	case lexer.INT, lexer.FLOAT, lexer.STRING, lexer.HEXLIT, lexer.BITLIT:
		t := p.advance()
		return arenaNode(&p.arena, ast.Literal{Raw: t.Raw, Kind: t.Type, TokPos: t.Pos}), nil

	case lexer.NULL_KW:
		t := p.advance()
		return arenaNode(&p.arena, ast.NullLit{TokPos: t.Pos}), nil

	case lexer.TRUE_KW, lexer.FALSE_KW:
		t := p.advance()
		return arenaNode(&p.arena, ast.Literal{Raw: t.Raw, Kind: t.Type, TokPos: t.Pos}), nil

	case lexer.NAMEDPARAM, lexer.QUESTION:
		t := p.advance()
		return arenaNode(&p.arena, ast.Param{Raw: t.Raw, TokPos: t.Pos}), nil

	case lexer.STAR:
		t := p.advance()
		return arenaNode(&p.arena, ast.StarExpr{TokPos: t.Pos}), nil

	case lexer.LPAREN:
		p.advance()
		if p.is(lexer.SELECT) || p.is(lexer.WITH) {
			sq, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			if _, err := p.eat(lexer.RPAREN); err != nil {
				return nil, err
			}
			return arenaNode(&p.arena, ast.SubqueryExpr{Subq: sq, TokPos: sq.TokPos}), nil
		}
		expr, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		if _, err := p.eat(lexer.RPAREN); err != nil {
			return nil, err
		}
		return expr, nil

	case lexer.CASE:
		return p.parseCaseExpr()

	case lexer.CAST:
		return p.parseCast()

	case lexer.IDENT, lexer.BACKTICK, lexer.DQUOTE:
		// Could be a function call, qualified ident, or plain ident.
		name, err := p.parseQualifiedIdent()
		if err != nil {
			return nil, err
		}
		if p.is(lexer.LPAREN) {
			return p.parseFuncCall(name)
		}
		if len(name.Parts) == 1 {
			return name.Parts[0], nil
		}
		return name, nil

	// Handle keywords that can be used as function names (e.g. REPLACE, LEFT...)
	case lexer.REPLACE, lexer.LEFT, lexer.RIGHT, lexer.INSERT:
		part := arenaNode(&p.arena, ast.Ident{Raw: p.tok.Raw, Unquoted: lowerASCIIStringArena(&p.arena, p.tok.Raw), TokPos: p.tok.Pos})
		var parts []*ast.Ident
		parts = arenaAppend(&p.arena, parts, part)
		name := arenaNode(&p.arena, ast.QualifiedIdent{Parts: parts})
		p.advance()
		if p.is(lexer.LPAREN) {
			return p.parseFuncCall(name)
		}
		return name.Parts[0], nil
	}

	return nil, p.errorf("unexpected token %q in expression", p.tok.Raw)
}

func (p *Parser) parseCaseExpr() (ast.Expr, error) {
	pos := p.tok.Pos
	p.advance() // CASE
	c := arenaNode(&p.arena, ast.CaseExpr{TokPos: pos})
	// optional operand
	if !p.is(lexer.WHEN) {
		op, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		c.Operand = op
	}
	for p.tryEatKeyword(lexer.WHEN) {
		cond, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		if err := p.eatKeyword(lexer.THEN); err != nil {
			return nil, err
		}
		res, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		c.Whens = arenaAppend(&p.arena, c.Whens, ast.WhenClause{Cond: cond, Result: res})
	}
	if p.tryEatKeyword(lexer.ELSE) {
		el, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		c.Else = el
	}
	if err := p.eatKeyword(lexer.END); err != nil {
		return nil, err
	}
	return c, nil
}

func (p *Parser) parseCast() (ast.Expr, error) {
	pos := p.tok.Pos
	p.advance() // CAST
	if _, err := p.eat(lexer.LPAREN); err != nil {
		return nil, err
	}
	expr, err := p.parseExpr(0)
	if err != nil {
		return nil, err
	}
	if err := p.eatKeyword(lexer.AS); err != nil {
		return nil, err
	}
	dt, err := p.parseDataType()
	if err != nil {
		return nil, err
	}
	if _, err := p.eat(lexer.RPAREN); err != nil {
		return nil, err
	}
	return arenaNode(&p.arena, ast.CastExpr{Expr: expr, Type: dt, TokPos: pos}), nil
}

func (p *Parser) parseFuncCall(name *ast.QualifiedIdent) (*ast.FuncCall, error) {
	pos := p.tok.Pos
	p.advance() // (
	fc := arenaNode(&p.arena, ast.FuncCall{Name: name, TokPos: pos})
	if p.is(lexer.RPAREN) {
		p.advance()
		return fc, nil
	}
	if p.is(lexer.STAR) {
		p.advance()
		fc.Star = true
	} else {
		fc.Distinct = p.tryEatKeyword(lexer.DISTINCT)
		args, err := p.parseExprList()
		if err != nil {
			return nil, err
		}
		fc.Args = args
	}
	if _, err := p.eat(lexer.RPAREN); err != nil {
		return nil, err
	}
	return fc, nil
}

func (p *Parser) parseExprList() ([]ast.Expr, error) {
	var exprs []ast.Expr
	for {
		e, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		exprs = arenaAppend(&p.arena, exprs, e)
		if !p.tryEat(lexer.COMMA) {
			break
		}
	}
	return exprs, nil
}

func (p *Parser) parseOrderBy() ([]ast.OrderByItem, error) {
	var items []ast.OrderByItem
	for {
		expr, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		item := ast.OrderByItem{Expr: expr}
		if p.tryEatKeyword(lexer.DESC) {
			item.Desc = true
		} else {
			p.tryEatKeyword(lexer.ASC)
		}
		items = arenaAppend(&p.arena, items, item)
		if !p.tryEat(lexer.COMMA) {
			break
		}
	}
	return items, nil
}

func (p *Parser) parseLimit() (*ast.LimitClause, error) {
	count, err := p.parseExpr(0)
	if err != nil {
		return nil, err
	}
	lim := arenaNode(&p.arena, ast.LimitClause{Count: count})
	if p.tryEatKeyword(lexer.OFFSET) {
		off, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		lim.Offset = off
	} else if p.tryEat(lexer.COMMA) {
		// MySQL: LIMIT offset, count
		off, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		lim.Offset = lim.Count
		lim.Count = off
	}
	return lim, nil
}

// ---- INSERT ----

func (p *Parser) parseInsert() (*ast.InsertStmt, error) {
	pos := p.tok.Pos
	p.advance() // INSERT
	stmt := arenaNode(&p.arena, ast.InsertStmt{TokPos: pos})
	stmt.Ignore = p.tryEatKeyword(lexer.IGNORE)
	p.tryEatKeyword(lexer.INTO)
	name, err := p.parseQualifiedIdent()
	if err != nil {
		return nil, err
	}
	stmt.Table = name

	if p.is(lexer.LPAREN) {
		p.advance()
		cols, err := p.parseIdentList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = cols
		if _, err := p.eat(lexer.RPAREN); err != nil {
			return nil, err
		}
	}

	if p.is(lexer.SELECT) || p.is(lexer.WITH) {
		sq, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		stmt.Select = sq
	} else if p.tryEatKeyword(lexer.VALUES) {
		for {
			if _, err := p.eat(lexer.LPAREN); err != nil {
				return nil, err
			}
			row, err := p.parseExprList()
			if err != nil {
				return nil, err
			}
			stmt.Values = arenaAppend(&p.arena, stmt.Values, row)
			if _, err := p.eat(lexer.RPAREN); err != nil {
				return nil, err
			}
			if !p.tryEat(lexer.COMMA) {
				break
			}
		}
	}

	// ON DUPLICATE KEY UPDATE
	if p.is(lexer.ON) {
		next := p.peekToken()
		if next.Type == lexer.IDENT && bytes.EqualFold(next.Raw, []byte("duplicate")) {
			p.advance() // ON
			p.advance() // DUPLICATE (as IDENT)
			p.advance() // KEY (as IDENT or keyword)
			if err := p.eatKeyword(lexer.UPDATE); err != nil {
				return nil, err
			}
			asgn, err := p.parseAssignments()
			if err != nil {
				return nil, err
			}
			stmt.OnDupKey = asgn
		} else if next.Type == lexer.IDENT && bytes.EqualFold(next.Raw, []byte("conflict")) {
			p.advance() // ON
			p.advance() // CONFLICT
			if p.is(lexer.LPAREN) {
				p.advance()
				cols, err := p.parseIdentList()
				if err != nil {
					return nil, err
				}
				stmt.OnConflictTarget = cols
				if _, err := p.eat(lexer.RPAREN); err != nil {
					return nil, err
				}
			}
			if !(p.is(lexer.IDENT) && bytes.EqualFold(p.tok.Raw, []byte("do"))) {
				return nil, p.errorf("expected DO in ON CONFLICT clause, got %q", p.tok.Raw)
			}
			p.advance() // DO
			if p.is(lexer.IDENT) && bytes.EqualFold(p.tok.Raw, []byte("nothing")) {
				p.advance() // NOTHING
				stmt.OnConflictDoNothing = true
			} else if p.tryEatKeyword(lexer.UPDATE) {
				if err := p.eatKeyword(lexer.SET); err != nil {
					return nil, err
				}
				asgn, err := p.parseAssignments()
				if err != nil {
					return nil, err
				}
				stmt.OnConflictUpdate = asgn
			} else {
				return nil, p.errorf("expected NOTHING or UPDATE in ON CONFLICT DO clause, got %q", p.tok.Raw)
			}
		}
	}

	return stmt, nil
}

func (p *Parser) parseReplace() (*ast.InsertStmt, error) {
	pos := p.tok.Pos
	p.advance() // REPLACE
	stmt := arenaNode(&p.arena, ast.InsertStmt{TokPos: pos, Replace: true})
	p.tryEatKeyword(lexer.INTO)
	name, err := p.parseQualifiedIdent()
	if err != nil {
		return nil, err
	}
	stmt.Table = name

	if p.is(lexer.LPAREN) {
		p.advance()
		cols, err := p.parseIdentList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = cols
		if _, err := p.eat(lexer.RPAREN); err != nil {
			return nil, err
		}
	}

	if p.tryEatKeyword(lexer.VALUES) {
		for {
			if _, err := p.eat(lexer.LPAREN); err != nil {
				return nil, err
			}
			row, err := p.parseExprList()
			if err != nil {
				return nil, err
			}
			stmt.Values = arenaAppend(&p.arena, stmt.Values, row)
			if _, err := p.eat(lexer.RPAREN); err != nil {
				return nil, err
			}
			if !p.tryEat(lexer.COMMA) {
				break
			}
		}
	}
	return stmt, nil
}

// ---- UPDATE ----

func (p *Parser) parseUpdate() (*ast.UpdateStmt, error) {
	pos := p.tok.Pos
	p.advance()
	stmt := arenaNode(&p.arena, ast.UpdateStmt{TokPos: pos})
	refs, err := p.parseTableRefs()
	if err != nil {
		return nil, err
	}
	stmt.Tables = refs
	if err := p.eatKeyword(lexer.SET); err != nil {
		return nil, err
	}
	asgn, err := p.parseAssignments()
	if err != nil {
		return nil, err
	}
	stmt.Set = asgn
	if p.tryEatKeyword(lexer.WHERE) {
		w, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		stmt.Where = w
	}
	if p.is(lexer.ORDER) && p.peekToken().Type == lexer.BY {
		p.advance()
		p.advance()
		ord, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		stmt.Order = ord
	}
	if p.tryEatKeyword(lexer.LIMIT) {
		lim, err := p.parseLimit()
		if err != nil {
			return nil, err
		}
		stmt.Limit = lim
	}
	return stmt, nil
}

// ---- DELETE ----

func (p *Parser) parseDelete() (*ast.DeleteStmt, error) {
	pos := p.tok.Pos
	p.advance()
	stmt := arenaNode(&p.arena, ast.DeleteStmt{TokPos: pos})
	p.tryEatKeyword(lexer.FROM)
	refs, err := p.parseTableRefs()
	if err != nil {
		return nil, err
	}
	stmt.From = refs
	if p.tryEatKeyword(lexer.WHERE) {
		w, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		stmt.Where = w
	}
	if p.is(lexer.ORDER) && p.peekToken().Type == lexer.BY {
		p.advance()
		p.advance()
		ord, err := p.parseOrderBy()
		if err != nil {
			return nil, err
		}
		stmt.Order = ord
	}
	if p.tryEatKeyword(lexer.LIMIT) {
		lim, err := p.parseLimit()
		if err != nil {
			return nil, err
		}
		stmt.Limit = lim
	}
	return stmt, nil
}

// ---- CREATE ----

func (p *Parser) parseCreate() (ast.Statement, error) {
	p.advance() // CREATE
	orReplace := false
	if p.is(lexer.IDENT) && bytes.EqualFold(p.tok.Raw, []byte("or")) {
		p.advance() // OR
		if err := p.eatKeyword(lexer.REPLACE); err != nil {
			return nil, err
		}
		orReplace = true
	}
	temporary := false
	if p.is(lexer.IDENT) && equalASCIIFold(p.tok.Raw, "temporary") {
		p.advance()
		temporary = true
	}
	_ = temporary
	switch p.tok.Type {
	case lexer.DATABASE:
		return p.parseCreateDatabase()
	case lexer.TABLE:
		return p.parseCreateTable(orReplace)
	case lexer.VIEW:
		return p.parseCreateView(orReplace)
	case lexer.INDEX, lexer.UNIQUE:
		return p.parseCreateIndex()
	case lexer.FUNCTION, lexer.PROCEDURE, lexer.TRIGGER:
		return p.parseGenericDDL([]byte("create"), p.tok.Raw)
	case lexer.IDENT:
		if equalASCIIFold(p.tok.Raw, "schema") {
			return p.parseCreateDatabase()
		}
		return p.parseGenericDDL([]byte("create"), p.tok.Raw)
	default:
		return p.parseGenericDDL([]byte("create"), p.tok.Raw)
	}
}

func (p *Parser) parseCreateDatabase() (*ast.CreateDatabaseStmt, error) {
	pos := p.tok.Pos
	p.advance() // DATABASE|SCHEMA
	stmt := arenaNode(&p.arena, ast.CreateDatabaseStmt{TokPos: pos})
	if p.is(lexer.IF) {
		p.advance()
		if !p.tryEatKeyword(lexer.NOT) {
			return nil, p.errorf("expected NOT in IF NOT EXISTS")
		}
		if !p.tryEatKeyword(lexer.EXISTS) {
			return nil, p.errorf("expected EXISTS in IF NOT EXISTS")
		}
		stmt.IfNotExists = true
	}
	name, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Name = name
	for !p.is(lexer.SEMICOLON) && !p.is(lexer.EOF) {
		key := p.advance().Raw
		if p.is(lexer.SEMICOLON) || p.is(lexer.EOF) {
			stmt.Options = arenaAppend(&p.arena, stmt.Options, ast.TableOption{Key: key})
			break
		}
		p.tryEat(lexer.EQ)
		val := p.advance().Raw
		stmt.Options = arenaAppend(&p.arena, stmt.Options, ast.TableOption{Key: key, Value: val})
	}
	return stmt, nil
}

func (p *Parser) parseCreateTable(orReplace bool) (*ast.CreateTableStmt, error) {
	pos := p.tok.Pos
	p.advance() // TABLE
	stmt := arenaNode(&p.arena, ast.CreateTableStmt{TokPos: pos})
	if p.is(lexer.IF) {
		p.advance()
		p.advance() // NOT
		p.advance() // EXISTS
		stmt.IfNotExists = true
	}
	name, err := p.parseQualifiedIdent()
	if err != nil {
		return nil, err
	}
	stmt.Table = name

	// LIKE
	if p.tryEatKeyword(lexer.LIKE) {
		like, err := p.parseQualifiedIdent()
		if err != nil {
			return nil, err
		}
		stmt.Like = like
		return stmt, nil
	}

	if p.is(lexer.LPAREN) {
		p.advance()
		cols, constraints, err := p.parseCreateTableBody()
		if err != nil {
			return nil, err
		}
		stmt.Columns = cols
		stmt.Constraints = constraints
		if _, err := p.eat(lexer.RPAREN); err != nil {
			return nil, err
		}
	}

	// Table options (ENGINE=..., CHARSET=..., etc.)
	for p.is(lexer.IDENT) || p.is(lexer.ENGINE) || p.is(lexer.COMMENT_KW) {
		key := p.advance().Raw
		p.tryEat(lexer.EQ)
		val := p.advance().Raw
		stmt.Options = arenaAppend(&p.arena, stmt.Options, ast.TableOption{Key: key, Value: val})
	}

	// AS SELECT
	if p.tryEatKeyword(lexer.AS) {
		sq, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		stmt.Select = sq
	}
	return stmt, nil
}

func (p *Parser) parseCreateTableBody() ([]*ast.ColumnDef, []*ast.TableConstraint, error) {
	var cols []*ast.ColumnDef
	var constraints []*ast.TableConstraint
	for {
		if p.is(lexer.RPAREN) || p.is(lexer.EOF) {
			break
		}
		// Constraint?
		if p.isConstraintStart() {
			c, err := p.parseTableConstraint()
			if err != nil {
				return nil, nil, err
			}
			constraints = arenaAppend(&p.arena, constraints, c)
		} else {
			col, err := p.parseColumnDef()
			if err != nil {
				return nil, nil, err
			}
			cols = arenaAppend(&p.arena, cols, col)
		}
		if !p.tryEat(lexer.COMMA) {
			break
		}
	}
	return cols, constraints, nil
}

func (p *Parser) isConstraintStart() bool {
	switch p.tok.Type {
	case lexer.PRIMARY, lexer.UNIQUE, lexer.INDEX, lexer.KEY, lexer.FOREIGN, lexer.CHECK, lexer.CONSTRAINT:
		return true
	case lexer.IDENT:
		return equalASCIIFold(p.tok.Raw, "fulltext") || equalASCIIFold(p.tok.Raw, "spatial")
	}
	return false
}

func (p *Parser) parseColumnDef() (*ast.ColumnDef, error) {
	name, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	col := arenaNode(&p.arena, ast.ColumnDef{Name: name, TokPos: name.TokPos})
	dt, err := p.parseDataType()
	if err != nil {
		return nil, err
	}
	col.Type = dt

	// column attributes
	for {
		switch p.tok.Type {
		case lexer.NOT:
			p.advance()
			if _, err := p.eat(lexer.NULL_KW); err != nil {
				return nil, err
			}
			col.NotNull = true
		case lexer.NULL_KW:
			p.advance()
		case lexer.DEFAULT:
			p.advance()
			def, err := p.parseExpr(0)
			if err != nil {
				return nil, err
			}
			col.Default = def
		case lexer.AUTO_INCREMENT:
			p.advance()
			col.AutoIncrement = true
		case lexer.PRIMARY:
			p.advance()
			p.tryEatKeyword(lexer.KEY)
			col.PrimaryKey = true
		case lexer.UNIQUE:
			p.advance()
			p.tryEatKeyword(lexer.KEY)
			col.Unique = true
		case lexer.COMMENT_KW:
			p.advance()
			t, err := p.eat(lexer.STRING)
			if err != nil {
				return nil, err
			}
			col.Comment = arenaNode(&p.arena, ast.Literal{Raw: t.Raw, Kind: t.Type, TokPos: t.Pos})
		case lexer.REFERENCES:
			ref, err := p.parseFKRef()
			if err != nil {
				return nil, err
			}
			col.References = ref
		case lexer.CHECK:
			p.advance()
			if _, err := p.eat(lexer.LPAREN); err != nil {
				return nil, err
			}
			expr, err := p.parseExpr(0)
			if err != nil {
				return nil, err
			}
			col.Check = expr
			if _, err := p.eat(lexer.RPAREN); err != nil {
				return nil, err
			}
		default:
			// unknown attribute keyword used as ident (e.g. COLLATE, CHARACTER SET)
			if p.is(lexer.COLLATE) {
				p.advance()
				p.advance() // skip collation name
				continue
			}
			return col, nil
		}
	}
}

func (p *Parser) parseDataType() (*ast.DataType, error) {
	name := p.tok.Raw
	pos := p.tok.Pos
	p.advance()
	dt := arenaNode(&p.arena, ast.DataType{Name: name, TokPos: pos})

	if p.is(lexer.LPAREN) {
		p.advance()
		if p.is(lexer.INT) {
			t := p.advance()
			n, _ := strconv.Atoi(string(t.Raw))
			dt.Precision = n
		}
		if p.tryEat(lexer.COMMA) {
			if p.is(lexer.INT) {
				t := p.advance()
				n, _ := strconv.Atoi(string(t.Raw))
				dt.Scale = n
			}
		}
		// ENUM/SET values
		if p.is(lexer.STRING) {
			for p.is(lexer.STRING) {
				dt.EnumVals = arenaAppend(&p.arena, dt.EnumVals, p.tok.Raw)
				p.advance()
				if !p.tryEat(lexer.COMMA) {
					break
				}
			}
		}
		if _, err := p.eat(lexer.RPAREN); err != nil {
			return nil, err
		}
	}
	// UNSIGNED / ZEROFILL
	if p.is(lexer.IDENT) {
		if equalASCIIFold(p.tok.Raw, "unsigned") {
			p.advance()
			dt.Unsigned = true
		}
		if equalASCIIFold(p.tok.Raw, "zerofill") {
			p.advance()
			dt.Zerofill = true
		}
	}
	return dt, nil
}

func (p *Parser) parseTableConstraint() (*ast.TableConstraint, error) {
	pos := p.tok.Pos
	c := arenaNode(&p.arena, ast.TableConstraint{TokPos: pos})

	if p.tryEatKeyword(lexer.CONSTRAINT) {
		if p.is(lexer.IDENT) || p.is(lexer.BACKTICK) {
			name, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			c.Name = name
		}
	}

	switch p.tok.Type {
	case lexer.PRIMARY:
		p.advance()
		p.tryEatKeyword(lexer.KEY)
		c.Type = ast.PrimaryKeyConstraint
		cols, err := p.parseIndexColDefs()
		if err != nil {
			return nil, err
		}
		c.Columns = cols
	case lexer.UNIQUE:
		p.advance()
		p.tryEatKeyword(lexer.KEY)
		p.tryEatKeyword(lexer.INDEX)
		c.Type = ast.UniqueConstraint
		// optional index name
		if p.is(lexer.IDENT) || p.is(lexer.BACKTICK) {
			name, _ := p.parseIdent()
			if c.Name == nil {
				c.Name = name
			}
		}
		cols, err := p.parseIndexColDefs()
		if err != nil {
			return nil, err
		}
		c.Columns = cols
	case lexer.INDEX, lexer.KEY:
		p.advance()
		c.Type = ast.IndexConstraint
		if p.is(lexer.IDENT) || p.is(lexer.BACKTICK) {
			c.Name, _ = p.parseIdent()
		}
		cols, err := p.parseIndexColDefs()
		if err != nil {
			return nil, err
		}
		c.Columns = cols
	case lexer.FOREIGN:
		p.advance()
		if err := p.eatKeyword(lexer.KEY); err != nil {
			return nil, err
		}
		c.Type = ast.ForeignKeyConstraint
		if p.is(lexer.IDENT) || p.is(lexer.BACKTICK) {
			c.Name, _ = p.parseIdent()
		}
		cols, err := p.parseIndexColDefs()
		if err != nil {
			return nil, err
		}
		c.Columns = cols
		ref, err := p.parseFKRef()
		if err != nil {
			return nil, err
		}
		c.RefTable = ref.Table
		c.RefCols = ref.Columns
		c.OnDelete = ref.OnDelete
		c.OnUpdate = ref.OnUpdate
	case lexer.CHECK:
		p.advance()
		c.Type = ast.CheckConstraint
		if _, err := p.eat(lexer.LPAREN); err != nil {
			return nil, err
		}
		expr, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		c.Check = expr
		if _, err := p.eat(lexer.RPAREN); err != nil {
			return nil, err
		}
	default:
		return nil, p.errorf("expected constraint type, got %q", p.tok.Raw)
	}
	return c, nil
}

func (p *Parser) parseIndexColDefs() ([]*ast.IndexColDef, error) {
	if _, err := p.eat(lexer.LPAREN); err != nil {
		return nil, err
	}
	var cols []*ast.IndexColDef
	for {
		name, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		icd := arenaNode(&p.arena, ast.IndexColDef{Name: name})
		if p.is(lexer.LPAREN) {
			p.advance()
			t, err := p.eat(lexer.INT)
			if err != nil {
				return nil, err
			}
			n, _ := strconv.Atoi(string(t.Raw))
			icd.Length = arenaNode(&p.arena, n)
			if _, err := p.eat(lexer.RPAREN); err != nil {
				return nil, err
			}
		}
		if p.tryEatKeyword(lexer.DESC) {
			icd.Desc = true
		} else {
			p.tryEatKeyword(lexer.ASC)
		}
		cols = arenaAppend(&p.arena, cols, icd)
		if !p.tryEat(lexer.COMMA) {
			break
		}
	}
	if _, err := p.eat(lexer.RPAREN); err != nil {
		return nil, err
	}
	return cols, nil
}

func (p *Parser) parseFKRef() (*ast.ForeignKeyRef, error) {
	if err := p.eatKeyword(lexer.REFERENCES); err != nil {
		return nil, err
	}
	table, err := p.parseQualifiedIdent()
	if err != nil {
		return nil, err
	}
	ref := arenaNode(&p.arena, ast.ForeignKeyRef{Table: table})
	if p.is(lexer.LPAREN) {
		p.advance()
		cols, err := p.parseIdentList()
		if err != nil {
			return nil, err
		}
		ref.Columns = cols
		if _, err := p.eat(lexer.RPAREN); err != nil {
			return nil, err
		}
	}
	for {
		if p.is(lexer.ON) {
			p.advance()
			switch p.tok.Type {
			case lexer.DELETE:
				p.advance()
				ref.OnDelete = p.parseRefAction()
			case lexer.UPDATE:
				p.advance()
				ref.OnUpdate = p.parseRefAction()
			}
		} else {
			break
		}
	}
	return ref, nil
}

func (p *Parser) parseRefAction() ast.RefAction {
	switch p.tok.Type {
	case lexer.RESTRICT:
		p.advance()
		return ast.Restrict
	case lexer.CASCADE:
		p.advance()
		return ast.Cascade
	case lexer.NULL_KW:
		// SET NULL
		p.advance()
		return ast.SetNull
	case lexer.SET:
		p.advance()
		if p.tryEatKeyword(lexer.NULL_KW) {
			return ast.SetNull
		}
		if p.is(lexer.DEFAULT) {
			p.advance()
			return ast.SetDefault
		}
	case lexer.NO:
		p.advance() // ACTION
		p.advance()
		return ast.NoAction
	default:
		// try as ident "NO ACTION"
		if bytes.EqualFold(p.tok.Raw, []byte("no")) {
			p.advance()
			p.advance()
			return ast.NoAction
		}
	}
	return ast.NoAction
}

// ---- CREATE INDEX ----

func (p *Parser) parseCreateIndex() (*ast.CreateIndexStmt, error) {
	pos := p.tok.Pos
	typ := ast.IndexConstraint
	if p.tryEatKeyword(lexer.UNIQUE) {
		typ = ast.UniqueConstraint
	}
	p.tryEatKeyword(lexer.INDEX)
	stmt := arenaNode(&p.arena, ast.CreateIndexStmt{Type: typ, TokPos: pos})
	name, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Name = name
	if err := p.eatKeyword(lexer.ON); err != nil {
		return nil, err
	}
	table, err := p.parseQualifiedIdent()
	if err != nil {
		return nil, err
	}
	stmt.Table = table
	cols, err := p.parseIndexColDefs()
	if err != nil {
		return nil, err
	}
	stmt.Columns = cols
	return stmt, nil
}

// ---- CREATE VIEW ----

func (p *Parser) parseCreateView(orReplace bool) (*ast.CreateViewStmt, error) {
	pos := p.tok.Pos
	p.advance() // VIEW
	stmt := arenaNode(&p.arena, ast.CreateViewStmt{TokPos: pos, OrReplace: orReplace})
	name, err := p.parseQualifiedIdent()
	if err != nil {
		return nil, err
	}
	stmt.Name = name
	if p.is(lexer.LPAREN) {
		p.advance()
		cols, err := p.parseIdentList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = cols
		if _, err := p.eat(lexer.RPAREN); err != nil {
			return nil, err
		}
	}
	if err := p.eatKeyword(lexer.AS); err != nil {
		return nil, err
	}
	sq, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	stmt.Select = sq
	return stmt, nil
}

// ---- ALTER TABLE ----

func (p *Parser) parseAlter() (ast.Statement, error) {
	pos := p.tok.Pos
	p.advance() // ALTER
	if p.is(lexer.DATABASE) || (p.is(lexer.IDENT) && equalASCIIFold(p.tok.Raw, "schema")) {
		return p.parseAlterDatabase(pos)
	}
	if !p.tryEatKeyword(lexer.TABLE) {
		return p.parseGenericDDL([]byte("alter"), p.tok.Raw)
	}
	name, err := p.parseQualifiedIdent()
	if err != nil {
		return nil, err
	}
	stmt := arenaNode(&p.arena, ast.AlterTableStmt{Table: name, TokPos: pos})

	for {
		cmd, err := p.parseAlterCmd()
		if err != nil {
			return nil, err
		}
		stmt.Cmds = arenaAppend(&p.arena, stmt.Cmds, cmd)
		if !p.tryEat(lexer.COMMA) {
			break
		}
	}
	return stmt, nil
}

func (p *Parser) parseAlterDatabase(pos int32) (*ast.AlterDatabaseStmt, error) {
	p.advance() // DATABASE|SCHEMA
	stmt := arenaNode(&p.arena, ast.AlterDatabaseStmt{TokPos: pos})
	name, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Name = name
	for !p.is(lexer.SEMICOLON) && !p.is(lexer.EOF) {
		key := p.advance().Raw
		if p.is(lexer.SEMICOLON) || p.is(lexer.EOF) {
			stmt.Options = arenaAppend(&p.arena, stmt.Options, ast.TableOption{Key: key})
			break
		}
		p.tryEat(lexer.EQ)
		val := p.advance().Raw
		stmt.Options = arenaAppend(&p.arena, stmt.Options, ast.TableOption{Key: key, Value: val})
	}
	return stmt, nil
}

func (p *Parser) parseAlterCmd() (ast.AlterCmd, error) {
	pos := p.tok.Pos
	switch p.tok.Type {
	case lexer.ADD:
		p.advance()
		p.tryEatKeyword(lexer.COLUMN)
		if p.isConstraintStart() {
			c, err := p.parseTableConstraint()
			if err != nil {
				return nil, err
			}
			return arenaNode(&p.arena, ast.AddConstraintCmd{Constraint: c, TokPos: pos}), nil
		}
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		cmd := arenaNode(&p.arena, ast.AddColumnCmd{Col: col, TokPos: pos})
		if p.tryEatKeyword(lexer.FIRST) {
			cmd.First = true
		} else if p.tryEatKeyword(lexer.AFTER) {
			after, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			cmd.After = after
		}
		return cmd, nil

	case lexer.DROP:
		p.advance()
		if p.tryEatKeyword(lexer.COLUMN) || p.is(lexer.IDENT) || p.is(lexer.BACKTICK) {
			name, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			return arenaNode(&p.arena, ast.DropColumnCmd{Name: name, TokPos: pos}), nil
		}
		if p.tryEatKeyword(lexer.INDEX) || p.tryEatKeyword(lexer.KEY) {
			name, err := p.parseIdent()
			if err != nil {
				return nil, err
			}
			return arenaNode(&p.arena, ast.DropIndexCmd{Name: name, TokPos: pos}), nil
		}

	case lexer.IDENT:
		if equalASCIIFold(p.tok.Raw, "modify") {
			p.advance()
			p.tryEatKeyword(lexer.COLUMN)
			col, err := p.parseColumnDef()
			if err != nil {
				return nil, err
			}
			cmd := arenaNode(&p.arena, ast.ModifyColumnCmd{Col: col, TokPos: pos})
			if p.tryEatKeyword(lexer.FIRST) {
				cmd.First = true
			} else if p.tryEatKeyword(lexer.AFTER) {
				after, err := p.parseIdent()
				if err != nil {
					return nil, err
				}
				cmd.After = after
			}
			return cmd, nil
		}

	case lexer.RENAME:
		p.advance()
		p.tryEatKeyword(lexer.TO)
		newName, err := p.parseQualifiedIdent()
		if err != nil {
			return nil, err
		}
		return arenaNode(&p.arena, ast.RenameTableCmd{NewName: newName, TokPos: pos}), nil
	}
	return nil, p.errorf("unexpected ALTER TABLE command: %q", p.tok.Raw)
}

// ---- DROP ----

func (p *Parser) parseDrop() (ast.Statement, error) {
	p.advance() // DROP
	switch p.tok.Type {
	case lexer.DATABASE:
		return p.parseDropDatabase()
	case lexer.TABLE:
		return p.parseDropTable()
	case lexer.INDEX:
		return p.parseDropIndex()
	case lexer.FUNCTION, lexer.PROCEDURE, lexer.TRIGGER:
		return p.parseGenericDDL([]byte("drop"), p.tok.Raw)
	case lexer.VIEW:
		p.advance()
		stmt := arenaNode(&p.arena, ast.DropTableStmt{TokPos: p.tok.Pos})
		n, err := p.parseQualifiedIdent()
		if err != nil {
			return nil, err
		}
		stmt.Tables = arenaAppend(&p.arena, stmt.Tables, n)
		return stmt, nil
	case lexer.IDENT:
		if equalASCIIFold(p.tok.Raw, "schema") {
			return p.parseDropDatabase()
		}
		return p.parseGenericDDL([]byte("drop"), p.tok.Raw)
	default:
		return p.parseGenericDDL([]byte("drop"), p.tok.Raw)
	}
}

func (p *Parser) parseGenericDDL(verb, obj []byte) (*ast.GenericDDLStmt, error) {
	pos := p.tok.Pos
	stmt := arenaNode(&p.arena, ast.GenericDDLStmt{Verb: verb, Object: obj, TokPos: pos})
	p.advance() // object token
	if p.is(lexer.IDENT) || p.is(lexer.BACKTICK) || p.is(lexer.DQUOTE) {
		name, err := p.parseIdent()
		if err == nil {
			stmt.Name = name
		}
	}
	for p.tok.Type != lexer.SEMICOLON && p.tok.Type != lexer.EOF {
		p.advance()
	}
	return stmt, nil
}

func (p *Parser) parseDropDatabase() (*ast.DropDatabaseStmt, error) {
	pos := p.tok.Pos
	p.advance() // DATABASE|SCHEMA
	stmt := arenaNode(&p.arena, ast.DropDatabaseStmt{TokPos: pos})
	if p.is(lexer.IF) {
		p.advance()
		if !p.tryEatKeyword(lexer.EXISTS) {
			return nil, p.errorf("expected EXISTS in IF EXISTS")
		}
		stmt.IfExists = true
	}
	name, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Name = name
	return stmt, nil
}

func (p *Parser) parseDropTable() (*ast.DropTableStmt, error) {
	pos := p.tok.Pos
	p.advance() // TABLE
	stmt := arenaNode(&p.arena, ast.DropTableStmt{TokPos: pos})
	if p.is(lexer.IF) {
		p.advance()
		p.advance() // EXISTS
		stmt.IfExists = true
	}
	for {
		name, err := p.parseQualifiedIdent()
		if err != nil {
			return nil, err
		}
		stmt.Tables = arenaAppend(&p.arena, stmt.Tables, name)
		if !p.tryEat(lexer.COMMA) {
			break
		}
	}
	stmt.Cascade = p.tryEatKeyword(lexer.CASCADE)
	return stmt, nil
}

func (p *Parser) parseDropIndex() (*ast.DropIndexStmt, error) {
	pos := p.tok.Pos
	p.advance() // INDEX
	stmt := arenaNode(&p.arena, ast.DropIndexStmt{TokPos: pos})
	if p.is(lexer.IF) {
		p.advance()
		p.advance()
		stmt.IfExists = true
	}
	name, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	stmt.Name = name
	if p.tryEatKeyword(lexer.ON) {
		table, err := p.parseQualifiedIdent()
		if err != nil {
			return nil, err
		}
		stmt.Table = table
	}
	return stmt, nil
}

// ---- Misc statements ----

func (p *Parser) parseBegin() (*ast.TransactionStmt, error) {
	pos := p.tok.Pos
	p.advance() // BEGIN
	if p.is(lexer.TRANSACTION) || (p.is(lexer.IDENT) && equalASCIIFold(p.tok.Raw, "transaction")) {
		p.advance()
	}
	return arenaNode(&p.arena, ast.TransactionStmt{Action: []byte("begin"), TokPos: pos}), nil
}

func (p *Parser) parseCommit() (*ast.TransactionStmt, error) {
	pos := p.tok.Pos
	p.advance() // COMMIT
	if p.is(lexer.IDENT) && equalASCIIFold(p.tok.Raw, "work") {
		p.advance()
	}
	return arenaNode(&p.arena, ast.TransactionStmt{Action: []byte("commit"), TokPos: pos}), nil
}

func (p *Parser) parseRollback() (*ast.TransactionStmt, error) {
	pos := p.tok.Pos
	p.advance() // ROLLBACK
	stmt := arenaNode(&p.arena, ast.TransactionStmt{Action: []byte("rollback"), TokPos: pos})
	if p.is(lexer.IDENT) && equalASCIIFold(p.tok.Raw, "work") {
		p.advance()
	}
	if p.tryEatKeyword(lexer.TO) {
		if p.is(lexer.IDENT) && equalASCIIFold(p.tok.Raw, "savepoint") {
			p.advance()
		}
		sp, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		stmt.Savepoint = sp
	}
	return stmt, nil
}

func (p *Parser) parseStartTransaction() (*ast.TransactionStmt, error) {
	pos := p.tok.Pos
	p.advance() // START
	if !(p.is(lexer.TRANSACTION) || (p.is(lexer.IDENT) && equalASCIIFold(p.tok.Raw, "transaction"))) {
		return nil, p.errorf("expected TRANSACTION after START")
	}
	p.advance()
	stmt := arenaNode(&p.arena, ast.TransactionStmt{Action: []byte("start_transaction"), TokPos: pos})
	for !p.is(lexer.SEMICOLON) && !p.is(lexer.EOF) {
		stmt.Options = arenaAppend(&p.arena, stmt.Options, p.advance().Raw)
	}
	return stmt, nil
}

func (p *Parser) parseSavepoint() (*ast.TransactionStmt, error) {
	pos := p.tok.Pos
	p.advance() // SAVEPOINT
	sp, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	return arenaNode(&p.arena, ast.TransactionStmt{
		Action:    []byte("savepoint"),
		Savepoint: sp,
		TokPos:    pos,
	}), nil
}

func (p *Parser) parseReleaseSavepoint() (*ast.TransactionStmt, error) {
	pos := p.tok.Pos
	p.advance() // RELEASE
	if p.is(lexer.IDENT) && equalASCIIFold(p.tok.Raw, "savepoint") {
		p.advance()
	}
	sp, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	return arenaNode(&p.arena, ast.TransactionStmt{
		Action:    []byte("release_savepoint"),
		Savepoint: sp,
		TokPos:    pos,
	}), nil
}

func (p *Parser) parseSetStmt() (ast.Statement, error) {
	pos := p.tok.Pos
	p.advance() // SET
	if !(p.is(lexer.TRANSACTION) || (p.is(lexer.IDENT) && equalASCIIFold(p.tok.Raw, "transaction"))) {
		return nil, p.errorf("unsupported SET statement %q", p.tok.Raw)
	}
	p.advance() // TRANSACTION
	stmt := arenaNode(&p.arena, ast.TransactionStmt{Action: []byte("set_transaction"), TokPos: pos})
	for !p.is(lexer.SEMICOLON) && !p.is(lexer.EOF) {
		stmt.Options = arenaAppend(&p.arena, stmt.Options, p.advance().Raw)
	}
	return stmt, nil
}

func (p *Parser) parseCall() (*ast.CallStmt, error) {
	pos := p.tok.Pos
	p.advance() // CALL
	name, err := p.parseQualifiedIdent()
	if err != nil {
		return nil, err
	}
	stmt := arenaNode(&p.arena, ast.CallStmt{Name: name, TokPos: pos})
	if p.tryEat(lexer.LPAREN) {
		if !p.is(lexer.RPAREN) {
			args, err := p.parseExprList()
			if err != nil {
				return nil, err
			}
			stmt.Args = args
		}
		if _, err := p.eat(lexer.RPAREN); err != nil {
			return nil, err
		}
	}
	return stmt, nil
}

func (p *Parser) parseTruncate() (*ast.TruncateStmt, error) {
	pos := p.tok.Pos
	p.advance()
	p.tryEatKeyword(lexer.TABLE)
	name, err := p.parseQualifiedIdent()
	if err != nil {
		return nil, err
	}
	return arenaNode(&p.arena, ast.TruncateStmt{Table: name, TokPos: pos}), nil
}

func (p *Parser) parseUse() (*ast.UseStmt, error) {
	pos := p.tok.Pos
	p.advance()
	db, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	return arenaNode(&p.arena, ast.UseStmt{Database: db, TokPos: pos}), nil
}

func (p *Parser) parseShow() (*ast.ShowStmt, error) {
	pos := p.tok.Pos
	p.advance()
	what := p.tok.Raw
	p.advance()
	stmt := arenaNode(&p.arena, ast.ShowStmt{What: what, TokPos: pos})
	if p.tryEatKeyword(lexer.LIKE) {
		t, err := p.eat(lexer.STRING)
		if err != nil {
			return nil, err
		}
		stmt.Like = arenaNode(&p.arena, ast.Literal{Raw: t.Raw, Kind: t.Type, TokPos: t.Pos})
	} else if p.tryEatKeyword(lexer.WHERE) {
		w, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		stmt.Where = w
	}
	return stmt, nil
}

func (p *Parser) parseExplain() (*ast.ExplainStmt, error) {
	pos := p.tok.Pos
	p.advance()
	inner, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	return arenaNode(&p.arena, ast.ExplainStmt{Stmt: inner, TokPos: pos}), nil
}

// parseUnknownStmt skips tokens until a semicolon or EOF.
func (p *Parser) parseUnknownStmt() (ast.Statement, error) {
	for p.tok.Type != lexer.SEMICOLON && p.tok.Type != lexer.EOF {
		p.advance()
	}
	return nil, nil
}

// ---- Identifier helpers ----

func (p *Parser) parseIdent() (*ast.Ident, error) {
	t := p.tok
	switch t.Type {
	case lexer.IDENT, lexer.BACKTICK, lexer.DQUOTE:
		p.advance()
		unquoted := unquoteIdentArena(&p.arena, t.Raw)
		return arenaNode(&p.arena, ast.Ident{Raw: t.Raw, Unquoted: unquoted, TokPos: t.Pos}), nil
	default:
		// Allow keywords as identifiers in column/table positions
		if t.Type > lexer.ILLEGAL && t.Type < lexer.INT {
			p.advance()
			return arenaNode(&p.arena, ast.Ident{Raw: t.Raw, Unquoted: lowerASCIIStringArena(&p.arena, t.Raw), TokPos: t.Pos}), nil
		}
		return nil, p.errorf("expected identifier, got %q", t.Raw)
	}
}

func (p *Parser) parseQualifiedIdent() (*ast.QualifiedIdent, error) {
	id, err := p.parseIdent()
	if err != nil {
		return nil, err
	}
	var parts []*ast.Ident
	parts = arenaAppend(&p.arena, parts, id)
	qi := arenaNode(&p.arena, ast.QualifiedIdent{Parts: parts})
	for p.is(lexer.DOT) {
		p.advance()
		next, err := p.parseIdent()
		if err != nil {
			// could be schema.* – treat as ident
			if p.is(lexer.STAR) {
				star := arenaNode(&p.arena, ast.Ident{Raw: p.tok.Raw, Unquoted: "*", TokPos: p.tok.Pos})
				p.advance()
				qi.Parts = arenaAppend(&p.arena, qi.Parts, star)
				return qi, nil
			}
			return nil, err
		}
		qi.Parts = arenaAppend(&p.arena, qi.Parts, next)
	}
	return qi, nil
}

func (p *Parser) parseIdentList() ([]*ast.Ident, error) {
	var ids []*ast.Ident
	for {
		id, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		ids = arenaAppend(&p.arena, ids, id)
		if !p.tryEat(lexer.COMMA) {
			break
		}
	}
	return ids, nil
}

func (p *Parser) parseAssignments() ([]ast.Assignment, error) {
	var asgn []ast.Assignment
	for {
		col, err := p.parseIdent()
		if err != nil {
			return nil, err
		}
		if _, err := p.eat(lexer.EQ); err != nil {
			return nil, err
		}
		val, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		asgn = arenaAppend(&p.arena, asgn, ast.Assignment{Column: col, Value: val})
		if !p.tryEat(lexer.COMMA) {
			break
		}
	}
	return asgn, nil
}

// unquoteIdent strips backtick or double-quote delimiters.
func unquoteIdentArena(a *arena, raw []byte) string {
	if len(raw) < 2 {
		return lowerASCIIStringArena(a, raw)
	}
	if (raw[0] == '`' || raw[0] == '"') && raw[len(raw)-1] == raw[0] {
		return bytesToString(raw[1 : len(raw)-1])
	}
	return lowerASCIIStringArena(a, raw)
}

func lowerASCIIStringArena(a *arena, raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	if !hasUpperASCII(raw) {
		return bytesToString(raw)
	}
	dst := a.alloc(len(raw))[:len(raw)]
	for i, c := range raw {
		if c >= 'A' && c <= 'Z' {
			dst[i] = c + 32
		} else {
			dst[i] = c
		}
	}
	return bytesToString(dst)
}

func equalASCIIFold(raw []byte, s string) bool {
	if len(raw) != len(s) {
		return false
	}
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if c >= 'A' && c <= 'Z' {
			c += 32
		}
		if c != s[i] {
			return false
		}
	}
	return true
}

func hasUpperASCII(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] >= 'A' && raw[i] <= 'Z' {
			return true
		}
	}
	return false
}

func bytesToString(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	return unsafe.String(&raw[0], len(raw))
}
