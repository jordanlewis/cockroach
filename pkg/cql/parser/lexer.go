// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser

import (
	"fmt"
	"strings"
	"unicode"
)

// tokenKind classifies lexer tokens.
type tokenKind int

const (
	tokEOF tokenKind = iota
	tokIdent
	tokString    // single-quoted string
	tokInteger   // integer literal
	tokFloat     // float literal
	tokUUID      // bare UUID literal (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
	tokLParen    // (
	tokRParen    // )
	tokLBrace    // {
	tokRBrace    // }
	tokComma     // ,
	tokDot       // .
	tokSemicolon // ;
	tokStar      // *
	tokEq        // =
	tokLT        // <
	tokGT        // >
	tokLTEq      // <=
	tokGTEq      // >=
	tokNE        // !=
	tokColon     // :
	tokQMark     // ?
)

// token is a single lexer token.
type token struct {
	kind tokenKind
	val  string // raw text of the token
	pos  int    // byte offset in input
}

// lexer tokenizes a CQL input string. It is intentionally simple: CQL's lexical
// grammar has no string escapes beyond ” inside single-quoted strings.
type lexer struct {
	input  string
	pos    int
	tokens []token
	cur    int
}

func newLexer(input string) (*lexer, error) {
	l := &lexer{input: input}
	if err := l.tokenize(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *lexer) tokenize() error {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]

		// Skip whitespace.
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			l.pos++
			continue
		}

		// Skip line comments (-- ...).
		if ch == '-' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '-' {
			l.pos += 2
			for l.pos < len(l.input) && l.input[l.pos] != '\n' {
				l.pos++
			}
			continue
		}

		// Skip block comments (/* ... */).
		if ch == '/' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '*' {
			l.pos += 2
			for l.pos+1 < len(l.input) {
				if l.input[l.pos] == '*' && l.input[l.pos+1] == '/' {
					l.pos += 2
					break
				}
				l.pos++
			}
			continue
		}

		switch ch {
		case '(':
			l.emit(tokLParen, "(")
		case ')':
			l.emit(tokRParen, ")")
		case '{':
			l.emit(tokLBrace, "{")
		case '}':
			l.emit(tokRBrace, "}")
		case ',':
			l.emit(tokComma, ",")
		case '.':
			l.emit(tokDot, ".")
		case ';':
			l.emit(tokSemicolon, ";")
		case '*':
			l.emit(tokStar, "*")
		case '?':
			l.emit(tokQMark, "?")
		case ':':
			l.emit(tokColon, ":")
		case '=':
			l.emit(tokEq, "=")
		case '<':
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
				l.pos++
				l.emit(tokLTEq, "<=")
			} else {
				l.emit(tokLT, "<")
			}
		case '>':
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
				l.pos++
				l.emit(tokGTEq, ">=")
			} else {
				l.emit(tokGT, ">")
			}
		case '!':
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '=' {
				l.pos++
				l.emit(tokNE, "!=")
			} else {
				return fmt.Errorf("unexpected character '!' at position %d", l.pos)
			}
		case '\'':
			if err := l.lexString(); err != nil {
				return err
			}
			continue // lexString already advanced pos
		case '"':
			if err := l.lexQuotedIdent(); err != nil {
				return err
			}
			continue // lexQuotedIdent already advanced pos
		default:
			if isDigit(ch) || (ch == '-' && l.pos+1 < len(l.input) && isDigit(l.input[l.pos+1])) {
				if isHexDigit(ch) && l.tryLexUUID() {
					continue
				}
				l.lexNumber()
				continue
			}
			if isIdentStart(ch) {
				if isHexDigit(ch) && l.tryLexUUID() {
					continue
				}
				l.lexIdent()
				continue
			}
			return fmt.Errorf("unexpected character %q at position %d", ch, l.pos)
		}
		l.pos++
	}
	l.tokens = append(l.tokens, token{kind: tokEOF, pos: l.pos})
	return nil
}

func (l *lexer) emit(kind tokenKind, val string) {
	l.tokens = append(l.tokens, token{kind: kind, val: val, pos: l.pos})
}

// lexString reads a single-quoted CQL string. CQL escapes single quotes as ”.
func (l *lexer) lexString() error {
	start := l.pos
	l.pos++ // skip opening quote
	var b strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\'' {
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '\'' {
				b.WriteByte('\'')
				l.pos += 2
				continue
			}
			l.pos++ // skip closing quote
			l.tokens = append(l.tokens, token{kind: tokString, val: b.String(), pos: start})
			return nil
		}
		b.WriteByte(ch)
		l.pos++
	}
	return fmt.Errorf("unterminated string starting at position %d", start)
}

// lexNumber reads an integer or float literal. Negative numbers start with '-'.
func (l *lexer) lexNumber() {
	start := l.pos
	if l.input[l.pos] == '-' {
		l.pos++
	}
	for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
		l.pos++
	}
	kind := tokInteger
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		kind = tokFloat
		l.pos++
		for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
			l.pos++
		}
	}
	// Scientific notation (e.g. 1e10, 2.5E-3).
	if l.pos < len(l.input) && (l.input[l.pos] == 'e' || l.input[l.pos] == 'E') {
		kind = tokFloat
		l.pos++
		if l.pos < len(l.input) && (l.input[l.pos] == '+' || l.input[l.pos] == '-') {
			l.pos++
		}
		for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
			l.pos++
		}
	}
	l.tokens = append(l.tokens, token{kind: kind, val: l.input[start:l.pos], pos: start})
}

// lexQuotedIdent reads a double-quoted CQL identifier. Double-quoted
// identifiers preserve case and may contain reserved words. CQL escapes
// embedded double quotes by doubling them ("").
func (l *lexer) lexQuotedIdent() error {
	start := l.pos
	l.pos++ // skip opening "
	var b strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '"' {
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '"' {
				b.WriteByte('"')
				l.pos += 2
				continue
			}
			l.pos++ // skip closing "
			l.tokens = append(l.tokens, token{
				kind: tokIdent, val: b.String(), pos: start,
			})
			return nil
		}
		b.WriteByte(ch)
		l.pos++
	}
	return fmt.Errorf("unterminated quoted identifier starting at position %d", start)
}

// lexIdent reads a keyword or identifier. CQL identifiers are case-insensitive
// so we store the value as-is (comparison is done case-insensitively).
func (l *lexer) lexIdent() {
	start := l.pos
	for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
		l.pos++
	}
	l.tokens = append(l.tokens, token{kind: tokIdent, val: l.input[start:l.pos], pos: start})
}

// peek returns the current token without consuming it.
func (l *lexer) peek() token {
	if l.cur >= len(l.tokens) {
		return token{kind: tokEOF}
	}
	return l.tokens[l.cur]
}

// next consumes and returns the current token.
func (l *lexer) next() token {
	t := l.peek()
	if t.kind != tokEOF {
		l.cur++
	}
	return t
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentPart(ch byte) bool {
	return isIdentStart(ch) || isDigit(ch)
}

func isHexDigit(ch byte) bool {
	return isDigit(ch) || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}

// tryLexUUID attempts to lex a bare UUID at the current position. UUIDs
// have the form xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars, groups
// of 8-4-4-4-12 hex digits separated by dashes). Returns true if a UUID
// was successfully lexed.
func (l *lexer) tryLexUUID() bool {
	if l.pos+36 > len(l.input) {
		return false
	}
	candidate := l.input[l.pos : l.pos+36]
	if !isUUID(candidate) {
		return false
	}
	// Ensure the UUID is not followed by more identifier characters
	// (which would mean it's part of a longer token).
	if l.pos+36 < len(l.input) && isIdentPart(l.input[l.pos+36]) {
		return false
	}
	l.tokens = append(l.tokens, token{kind: tokUUID, val: candidate, pos: l.pos})
	l.pos += 36
	return true
}

// isKeyword returns true if the token is an identifier matching kw
// (case-insensitive).
func isKeyword(t token, kw string) bool {
	return t.kind == tokIdent && strings.EqualFold(t.val, kw)
}

// isUUID checks whether s looks like a UUID (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, ch := range s {
		switch i {
		case 8, 13, 18, 23:
			if ch != '-' {
				return false
			}
		default:
			if !unicode.Is(unicode.ASCII_Hex_Digit, ch) {
				return false
			}
		}
	}
	return true
}
