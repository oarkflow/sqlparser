package lexer

import (
	"unicode/utf8"
	"unsafe"
)

// Token represents a single SQL token. It holds a slice into the original
// input to avoid copying bytes. All string data is borrowed from the source.
type Token struct {
	// Raw is the exact bytes from the source (not unescaped).
	Raw []byte
	// Type is the token classification.
	Type TokenType
	// Pos is the byte offset of the first character.
	Pos int32
	// Line and Col are 1-based source positions.
	Line uint32
	Col  uint32
}

// Lexer tokenizes SQL input with zero heap allocations per token.
// It processes bytes directly and uses unsafe string conversion
// to avoid allocations in keyword lookups.
type Lexer struct {
	src  []byte
	pos  int
	line uint32
	col  uint32

	// scratch is reused to build lowercased keyword candidates.
	scratch [64]byte
}

// New creates a Lexer for the given SQL source.
func New(src []byte) *Lexer {
	return &Lexer{src: src, line: 1, col: 1}
}

// NewString creates a Lexer for a string input, avoiding a copy via unsafe.
func NewString(src string) *Lexer {
	// Convert string to []byte without copy using unsafe.
	b := unsafe.Slice(unsafe.StringData(src), len(src))
	return &Lexer{src: b, line: 1, col: 1}
}

// Reset reuses the lexer with new source, avoiding allocating a new lexer.
func (l *Lexer) Reset(src []byte) {
	l.src = src
	l.pos = 0
	l.line = 1
	l.col = 1
}

// Next returns the next token from the input. Returns EOF when exhausted.
// This function never allocates on the heap; all returned Token.Raw slices
// are sub-slices of the original source.
func (l *Lexer) Next() Token {
	for l.pos < len(l.src) {
		start := l.pos
		startLine := l.line
		startCol := l.col
		b := l.src[l.pos]

		switch {
		case b == '\n':
			l.pos++
			l.line++
			l.col = 1
			// continue without returning – skip whitespace

		case b == '\r':
			l.pos++
			if l.pos < len(l.src) && l.src[l.pos] == '\n' {
				l.pos++
			}
			l.line++
			l.col = 1

		case isSpace(b):
			l.pos++
			l.col++
			for l.pos < len(l.src) && isSpace(l.src[l.pos]) {
				if l.src[l.pos] == '\n' {
					break
				}
				l.pos++
				l.col++
			}
			// skip whitespace silently

		case b == '-' && l.pos+1 < len(l.src) && l.src[l.pos+1] == '-':
			// Single-line comment --
			l.pos += 2
			l.col += 2
			for l.pos < len(l.src) && l.src[l.pos] != '\n' {
				l.pos++
				l.col++
			}
			// skip comment

		case b == '#':
			// PostgreSQL JSON operators #> and #>> must be tokenized as punctuation.
			if l.pos+1 < len(l.src) && l.src[l.pos+1] == '>' {
				return l.lexPunct(start, startLine, startCol)
			}
			// MySQL hash comment
			l.pos++
			l.col++
			for l.pos < len(l.src) && l.src[l.pos] != '\n' {
				l.pos++
				l.col++
			}

		case b == '/' && l.pos+1 < len(l.src) && l.src[l.pos+1] == '*':
			// Block comment /* ... */
			l.pos += 2
			l.col += 2
			for l.pos+1 < len(l.src) {
				if l.src[l.pos] == '\n' {
					l.line++
					l.col = 1
					l.pos++
				} else if l.src[l.pos] == '*' && l.src[l.pos+1] == '/' {
					l.pos += 2
					l.col += 2
					break
				} else {
					l.pos++
					l.col++
				}
			}

		case isDigit(b) || (b == '.' && l.pos+1 < len(l.src) && isDigit(l.src[l.pos+1])):
			return l.lexNumber(start, startLine, startCol)

		case b == '\'':
			return l.lexQuoted(start, startLine, startCol, '\'', STRING)

		case b == '"':
			return l.lexQuoted(start, startLine, startCol, '"', DQUOTE)

		case b == '`':
			return l.lexQuoted(start, startLine, startCol, '`', BACKTICK)

		case b == 'x' || b == 'X':
			if l.pos+1 < len(l.src) && l.src[l.pos+1] == '\'' {
				return l.lexHexLit(start, startLine, startCol)
			}
			return l.lexIdent(start, startLine, startCol)

		case b == 'b' || b == 'B':
			if l.pos+1 < len(l.src) && l.src[l.pos+1] == '\'' {
				return l.lexBitLit(start, startLine, startCol)
			}
			return l.lexIdent(start, startLine, startCol)

		case b == '0' && l.pos+1 < len(l.src) && (l.src[l.pos+1] == 'x' || l.src[l.pos+1] == 'X'):
			return l.lexHex0x(start, startLine, startCol)

		case isAlpha(b) || b == '_':
			return l.lexIdent(start, startLine, startCol)

		default:
			return l.lexPunct(start, startLine, startCol)
		}

		// consumed whitespace/comment, restart
		_ = startLine
		_ = startCol
	}
	return Token{Type: EOF, Pos: int32(l.pos), Line: l.line, Col: l.col}
}

// lexIdent scans an identifier or keyword.
func (l *Lexer) lexIdent(start int, line, col uint32) Token {
	l.pos++
	l.col++
	for l.pos < len(l.src) && isIdentCont(l.src[l.pos]) {
		l.pos++
		l.col++
	}
	raw := l.src[start:l.pos]

	// Lowercase into scratch for keyword lookup (no heap alloc for <=64 bytes).
	n := len(raw)
	if n > len(l.scratch) {
		// Longer than scratch – cannot be a keyword (all keywords ≤ 14 chars).
		return Token{Type: IDENT, Raw: raw, Pos: int32(start), Line: line, Col: col}
	}
	for i, c := range raw {
		if c >= 'A' && c <= 'Z' {
			l.scratch[i] = c + 32
		} else {
			l.scratch[i] = c
		}
	}
	tok := lookupKeyword(l.scratch[:n])
	return Token{Type: tok, Raw: raw, Pos: int32(start), Line: line, Col: col}
}

// lexNumber scans integer or float literals.
func (l *Lexer) lexNumber(start int, line, col uint32) Token {
	typ := INT
	for l.pos < len(l.src) && isDigit(l.src[l.pos]) {
		l.pos++
		l.col++
	}
	if l.pos < len(l.src) && l.src[l.pos] == '.' {
		typ = FLOAT
		l.pos++
		l.col++
		for l.pos < len(l.src) && isDigit(l.src[l.pos]) {
			l.pos++
			l.col++
		}
	}
	// optional exponent
	if l.pos < len(l.src) && (l.src[l.pos] == 'e' || l.src[l.pos] == 'E') {
		typ = FLOAT
		l.pos++
		l.col++
		if l.pos < len(l.src) && (l.src[l.pos] == '+' || l.src[l.pos] == '-') {
			l.pos++
			l.col++
		}
		for l.pos < len(l.src) && isDigit(l.src[l.pos]) {
			l.pos++
			l.col++
		}
	}
	return Token{Type: TokenType(typ), Raw: l.src[start:l.pos], Pos: int32(start), Line: line, Col: col}
}

// lexQuoted scans a single, double, or backtick quoted string.
func (l *Lexer) lexQuoted(start int, line, col uint32, delim byte, typ TokenType) Token {
	l.pos++ // skip opening delimiter
	l.col++
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		if c == delim {
			l.pos++
			l.col++
			// doubled delimiter is an escape (e.g. '' or "")
			if l.pos < len(l.src) && l.src[l.pos] == delim {
				l.pos++
				l.col++
				continue
			}
			break
		}
		if c == '\\' && delim != '`' {
			l.pos++
			l.col++
			if l.pos < len(l.src) {
				l.pos++
				l.col++
			}
			continue
		}
		if c == '\n' {
			l.line++
			l.col = 1
			l.pos++
			continue
		}
		if c >= 0x80 {
			// Multi-byte UTF-8: skip the full rune.
			_, size := utf8.DecodeRune(l.src[l.pos:])
			l.pos += size
			l.col++
			continue
		}
		l.pos++
		l.col++
	}
	return Token{Type: typ, Raw: l.src[start:l.pos], Pos: int32(start), Line: line, Col: col}
}

func (l *Lexer) lexHexLit(start int, line, col uint32) Token {
	l.pos++ // x
	l.col++
	l.pos++ // '
	l.col++
	for l.pos < len(l.src) && l.src[l.pos] != '\'' {
		l.pos++
		l.col++
	}
	if l.pos < len(l.src) {
		l.pos++ // closing '
		l.col++
	}
	return Token{Type: HEXLIT, Raw: l.src[start:l.pos], Pos: int32(start), Line: line, Col: col}
}

func (l *Lexer) lexHex0x(start int, line, col uint32) Token {
	l.pos += 2 // 0x
	l.col += 2
	for l.pos < len(l.src) && isHexDigit(l.src[l.pos]) {
		l.pos++
		l.col++
	}
	return Token{Type: HEXLIT, Raw: l.src[start:l.pos], Pos: int32(start), Line: line, Col: col}
}

func (l *Lexer) lexBitLit(start int, line, col uint32) Token {
	l.pos++ // b/B
	l.col++
	return l.lexQuoted(l.pos-1, line, col, '\'', BITLIT)
}

// lexPunct handles single and multi-character punctuation/operators.
func (l *Lexer) lexPunct(start int, line, col uint32) Token {
	b := l.src[l.pos]
	l.pos++
	l.col++

	peek := func() byte {
		if l.pos < len(l.src) {
			return l.src[l.pos]
		}
		return 0
	}
	advance := func() {
		l.pos++
		l.col++
	}

	var typ TokenType
	switch b {
	case '(':
		typ = LPAREN
	case ')':
		typ = RPAREN
	case '{':
		typ = LBRACE
	case '}':
		typ = RBRACE
	case '[':
		typ = LBRACKET
	case ']':
		typ = RBRACKET
	case ',':
		typ = COMMA
	case ';':
		typ = SEMICOLON
	case '*':
		typ = STAR
	case '%':
		typ = PERCENT
	case '^':
		typ = CARET
	case '~':
		typ = TILDE
	case '?':
		switch peek() {
		case '|':
			advance()
			typ = QMARKPIPE
		case '&':
			advance()
			typ = QMARKAMP
		default:
			typ = QUESTION
		}
	case '+':
		typ = PLUS
	case '=':
		if peek() == '>' {
			advance()
			typ = DARROW
		} else {
			typ = EQ
		}
	case '!':
		if peek() == '=' {
			advance()
			typ = NEQ
		} else {
			typ = BANG
		}
	case '<':
		switch peek() {
		case '=':
			advance()
			typ = LTE
		case '>':
			advance()
			typ = NEQ
		case '@':
			advance()
			typ = LTAT
		case '<':
			advance()
			typ = LSHIFT
		default:
			typ = LT
		}
	case '>':
		switch peek() {
		case '=':
			advance()
			typ = GTE
		case '>':
			advance()
			typ = RSHIFT
		default:
			typ = GT
		}
	case '|':
		if peek() == '|' {
			advance()
			typ = DBAR
		} else {
			typ = PIPE
		}
	case '&':
		if peek() == '&' {
			advance()
			typ = DAMP
		} else {
			typ = AMPERSAND
		}
	case '-':
		if peek() == '>' {
			advance()
			if peek() == '>' {
				advance()
				typ = DARROW2
			} else {
				typ = ARROW
			}
		} else {
			typ = MINUS
		}
	case '/':
		typ = SLASH
	case '.':
		if peek() == '.' {
			advance()
			typ = DOTDOT
		} else {
			typ = DOT
		}
	case ':':
		// named parameter :name
		if isAlpha(peek()) || peek() == '_' {
			for l.pos < len(l.src) && isIdentCont(l.src[l.pos]) {
				advance()
			}
			typ = NAMEDPARAM
		} else {
			typ = COLON
		}
	case '@':
		if peek() == '>' {
			advance()
			typ = ATGT
		} else if isAlpha(peek()) || peek() == '_' || peek() == '@' {
			for l.pos < len(l.src) && isIdentCont(l.src[l.pos]) {
				advance()
			}
			typ = NAMEDPARAM
		} else {
			typ = AT
		}
	case '$':
		if isDigit(peek()) {
			for l.pos < len(l.src) && isDigit(l.src[l.pos]) {
				advance()
			}
			typ = NAMEDPARAM
		} else if isAlpha(peek()) || peek() == '_' {
			for l.pos < len(l.src) && isIdentCont(l.src[l.pos]) {
				advance()
			}
			typ = NAMEDPARAM
		} else {
			typ = DOLLAR
		}
	case '#':
		if peek() == '>' {
			advance()
			if peek() == '>' {
				advance()
				typ = HASHDARROW
			} else {
				typ = HASHARROW
			}
		} else {
			typ = HASH
		}
	default:
		typ = ILLEGAL
	}
	return Token{Type: typ, Raw: l.src[start:l.pos], Pos: int32(start), Line: line, Col: col}
}

// ---- character classification tables (no function call overhead) ----

var isSpaceTab = [256]bool{' ': true, '\t': true, '\v': true, '\f': true}

func isSpace(c byte) bool { return isSpaceTab[c] }

var identContTable [256]bool

func init() {
	for c := 'a'; c <= 'z'; c++ {
		identContTable[c] = true
	}
	for c := 'A'; c <= 'Z'; c++ {
		identContTable[c] = true
	}
	for c := '0'; c <= '9'; c++ {
		identContTable[c] = true
	}
	identContTable['_'] = true
	identContTable['$'] = true
}

func isIdentCont(c byte) bool { return identContTable[c] }
func isAlpha(c byte) bool     { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' }
func isDigit(c byte) bool     { return c >= '0' && c <= '9' }
func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// Tokenize is a convenience function that lexes all tokens from src
// into a pre-allocated slice (caller provides buffer to avoid alloc).
func Tokenize(src []byte, buf []Token) []Token {
	buf = buf[:0]
	l := New(src)
	for {
		t := l.Next()
		buf = append(buf, t)
		if t.Type == EOF {
			break
		}
	}
	return buf
}
