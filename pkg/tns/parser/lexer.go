// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// Lexer tokenizes Oracle SQL input.
type Lexer struct {
	input string
	pos   int // current byte position
	line  int // 1-based line number
	col   int // 1-based column number
}

// NewLexer creates a new Lexer for the given input string.
func NewLexer(input string) *Lexer {
	return &Lexer{
		input: input,
		pos:   0,
		line:  1,
		col:   1,
	}
}

// peek returns the next rune without advancing.
func (l *Lexer) peek() (rune, int) {
	if l.pos >= len(l.input) {
		return 0, 0
	}
	return utf8.DecodeRuneInString(l.input[l.pos:])
}

// advance moves forward by one rune and returns it.
func (l *Lexer) advance() rune {
	r, size := utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += size
	if r == '\n' {
		l.line++
		l.col = 1
	} else {
		l.col++
	}
	return r
}

// skipWhitespace skips spaces, tabs, newlines, and carriage returns.
func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		r, _ := l.peek()
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			l.advance()
		} else {
			break
		}
	}
}

// skipLineComment skips a -- line comment.
func (l *Lexer) skipLineComment() {
	for l.pos < len(l.input) {
		r, _ := l.peek()
		if r == '\n' {
			break
		}
		l.advance()
	}
}

// skipBlockComment skips a /* ... */ block comment.
func (l *Lexer) skipBlockComment() {
	// skip past the opening /*
	l.advance() // *
	for l.pos < len(l.input) {
		r := l.advance()
		if r == '*' {
			if next, _ := l.peek(); next == '/' {
				l.advance()
				return
			}
		}
	}
}

// NextToken returns the next token from the input.
func (l *Lexer) NextToken() Token {
	for {
		l.skipWhitespace()
		if l.pos >= len(l.input) {
			return Token{Type: EOF, Pos: l.pos, Line: l.line, Col: l.col}
		}

		startPos := l.pos
		startLine := l.line
		startCol := l.col
		r, _ := l.peek()

		// handle comments
		if r == '-' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '-' {
			l.advance() // -
			l.advance() // -
			l.skipLineComment()
			continue
		}
		if r == '/' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '*' {
			l.advance() // /
			l.skipBlockComment()
			continue
		}

		tok := func(typ TokenType, lit string) Token {
			return Token{Type: typ, Literal: lit, Pos: startPos, Line: startLine, Col: startCol}
		}

		switch {
		case r == '(':
			l.advance()
			return tok(LPAREN, "(")
		case r == ')':
			l.advance()
			return tok(RPAREN, ")")
		case r == ',':
			l.advance()
			return tok(COMMA, ",")
		case r == ';':
			l.advance()
			return tok(SEMI, ";")
		case r == '+':
			l.advance()
			return tok(PLUS, "+")
		case r == '-':
			l.advance()
			return tok(MINUS, "-")
		case r == '*':
			l.advance()
			return tok(STAR, "*")
		case r == '/':
			l.advance()
			return tok(SLASH, "/")
		case r == '%':
			l.advance()
			return tok(PERCENT, "%")
		case r == '=':
			l.advance()
			return tok(EQ, "=")
		case r == '.':
			l.advance()
			return tok(DOT, ".")
		case r == '|':
			l.advance()
			if next, _ := l.peek(); next == '|' {
				l.advance()
				return tok(CONCAT, "||")
			}
			return tok(ILLEGAL, "|")
		case r == '<':
			l.advance()
			if next, _ := l.peek(); next == '=' {
				l.advance()
				return tok(LTE, "<=")
			} else if next == '>' {
				l.advance()
				return tok(NEQ, "<>")
			}
			return tok(LT, "<")
		case r == '>':
			l.advance()
			if next, _ := l.peek(); next == '=' {
				l.advance()
				return tok(GTE, ">=")
			}
			return tok(GT, ">")
		case r == '!':
			l.advance()
			if next, _ := l.peek(); next == '=' {
				l.advance()
				return tok(NEQ, "!=")
			}
			return tok(ILLEGAL, "!")
		case r == ':':
			l.advance()
			// bind variable :name
			if next, _ := l.peek(); isIdentStart(next) {
				return l.readBind(startPos, startLine, startCol)
			}
			return tok(COLON, ":")
		case r == '\'':
			return l.readString(startPos, startLine, startCol)
		case r == '"':
			return l.readQuotedIdent(startPos, startLine, startCol)
		case isDigit(r):
			return l.readNumber(startPos, startLine, startCol)
		case isIdentStart(r):
			return l.readIdent(startPos, startLine, startCol)
		default:
			l.advance()
			return tok(ILLEGAL, string(r))
		}
	}
}

// readString reads a single-quoted string literal, handling '' escapes.
func (l *Lexer) readString(startPos, startLine, startCol int) Token {
	l.advance() // skip opening '
	var b strings.Builder
	for l.pos < len(l.input) {
		r := l.advance()
		if r == '\'' {
			// check for escaped ''
			if next, _ := l.peek(); next == '\'' {
				l.advance()
				b.WriteRune('\'')
			} else {
				return Token{
					Type: STRING, Literal: b.String(),
					Pos: startPos, Line: startLine, Col: startCol,
				}
			}
		} else {
			b.WriteRune(r)
		}
	}
	// unterminated string
	return Token{
		Type: ILLEGAL, Literal: b.String(),
		Pos: startPos, Line: startLine, Col: startCol,
	}
}

// readQuotedIdent reads a "double-quoted" identifier (preserves case).
func (l *Lexer) readQuotedIdent(startPos, startLine, startCol int) Token {
	l.advance() // skip opening "
	var b strings.Builder
	for l.pos < len(l.input) {
		r := l.advance()
		if r == '"' {
			// check for escaped ""
			if next, _ := l.peek(); next == '"' {
				l.advance()
				b.WriteRune('"')
			} else {
				return Token{
					Type: IDENT, Literal: b.String(),
					Pos: startPos, Line: startLine, Col: startCol,
				}
			}
		} else {
			b.WriteRune(r)
		}
	}
	// unterminated quoted identifier
	return Token{
		Type: ILLEGAL, Literal: b.String(),
		Pos: startPos, Line: startLine, Col: startCol,
	}
}

// readNumber reads an integer or decimal number.
func (l *Lexer) readNumber(startPos, startLine, startCol int) Token {
	start := l.pos
	for l.pos < len(l.input) {
		r, _ := l.peek()
		if !isDigit(r) {
			break
		}
		l.advance()
	}
	// check for decimal point
	if l.pos < len(l.input) {
		r, _ := l.peek()
		if r == '.' {
			// peek ahead to see if it's followed by a digit
			if l.pos+1 < len(l.input) {
				next, _ := utf8.DecodeRuneInString(l.input[l.pos+1:])
				if isDigit(next) {
					l.advance() // consume .
					for l.pos < len(l.input) {
						r, _ = l.peek()
						if !isDigit(r) {
							break
						}
						l.advance()
					}
				}
			}
		}
	}
	// optional exponent
	if l.pos < len(l.input) {
		r, _ := l.peek()
		if r == 'e' || r == 'E' {
			l.advance()
			if r, _ = l.peek(); r == '+' || r == '-' {
				l.advance()
			}
			for l.pos < len(l.input) {
				r, _ = l.peek()
				if !isDigit(r) {
					break
				}
				l.advance()
			}
		}
	}
	return Token{
		Type: NUMBER, Literal: l.input[start:l.pos],
		Pos: startPos, Line: startLine, Col: startCol,
	}
}

// readIdent reads an unquoted identifier or keyword. Unquoted identifiers
// are uppercased per Oracle convention.
func (l *Lexer) readIdent(startPos, startLine, startCol int) Token {
	start := l.pos
	for l.pos < len(l.input) {
		r, _ := l.peek()
		if !isIdentPart(r) {
			break
		}
		l.advance()
	}
	raw := l.input[start:l.pos]
	upper := strings.ToUpper(raw)
	typ := LookupKeyword(upper)
	return Token{
		Type: typ, Literal: upper,
		Pos: startPos, Line: startLine, Col: startCol,
	}
}

// readBind reads a bind variable after the colon has been consumed.
func (l *Lexer) readBind(startPos, startLine, startCol int) Token {
	start := l.pos
	for l.pos < len(l.input) {
		r, _ := l.peek()
		if !isIdentPart(r) {
			break
		}
		l.advance()
	}
	name := l.input[start:l.pos]
	return Token{
		Type: BIND, Literal: name,
		Pos: startPos, Line: startLine, Col: startCol,
	}
}

func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

func isIdentStart(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

func isIdentPart(r rune) bool {
	return r == '_' || r == '$' || r == '#' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
