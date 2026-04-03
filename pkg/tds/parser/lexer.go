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

// tokenType identifies the type of a lexer token.
type tokenType int

const (
	// Special tokens.
	tokenEOF tokenType = iota
	tokenError

	// Literals.
	tokenIdent
	tokenInt
	tokenFloat
	tokenString

	// Punctuation.
	tokenLParen
	tokenRParen
	tokenComma
	tokenDot
	tokenSemicolon
	tokenStar
	tokenPlus
	tokenMinus
	tokenSlash
	tokenPercent
	tokenEq
	tokenNeq // <> or !=
	tokenLT  // <
	tokenGT  // >
	tokenLTE // <=
	tokenGTE // >=

	// Keywords. T-SQL keywords are case-insensitive; the lexer normalizes
	// them to upper case for comparison.
	tokenUSE
	tokenCREATE
	tokenTABLE
	tokenINSERT
	tokenINTO
	tokenVALUES
	tokenSELECT
	tokenFROM
	tokenWHERE
	tokenORDER
	tokenBY
	tokenTOP
	tokenAS
	tokenNOT
	tokenNULL
	tokenAND
	tokenOR
	tokenASC
	tokenDESC
	tokenGO
	tokenIS
	tokenIN
	tokenBETWEEN
	tokenLIKE
	tokenDATABASE
	tokenDELETE
	tokenUPDATE
	tokenSET
	tokenDROP
	tokenDISTINCT
	tokenGROUP
	tokenHAVING
	tokenCASE
	tokenWHEN
	tokenTHEN
	tokenELSE
	tokenEND
	tokenISNULL
	tokenCONVERT
	tokenGETDATE

	// JOIN-related keywords.
	tokenJOIN
	tokenINNER
	tokenLEFT
	tokenRIGHT
	tokenFULL
	tokenOUTER
	tokenCROSS
	tokenON

	// Extended DDL keywords.
	tokenALTER
	tokenCOLUMN
	tokenCONSTRAINT
	tokenINDEX
	tokenVIEW
	tokenPROCEDURE
	tokenFUNCTION
	tokenTRIGGER
	tokenTRUNCATE
	tokenIF
	tokenEXISTS
	tokenUNIQUE
	tokenINCLUDE
	tokenREFERENCES
	tokenPRIMARY
	tokenKEY
	tokenFOREIGN
	tokenCHECK
	tokenADD

	// DML extension keywords.
	tokenMERGE
	tokenUSING
	tokenMATCHED
	tokenOUTPUT

	// Phase 2 keywords: set operations, subqueries, CTEs, window functions,
	// OFFSET-FETCH pagination.
	tokenUNION
	tokenINTERSECT
	tokenEXCEPT
	tokenALL
	tokenWITH
	tokenANY
	tokenSOME
	tokenOVER
	tokenPARTITION
	tokenOFFSET
	tokenFETCH
	tokenNEXT
	tokenFIRST
	tokenONLY
	tokenROWS
	tokenROW

	// Transaction-related keywords.
	tokenBEGIN
	tokenTRAN
	tokenTRANSACTION
	tokenCOMMIT
	tokenROLLBACK
	tokenSAVE

	// Sybase ASE pagination keyword.
	tokenLIMIT

	// Type system keywords.
	tokenIDENTITY
	tokenDEFAULT
)

var keywords = map[string]tokenType{
	"USE":      tokenUSE,
	"CREATE":   tokenCREATE,
	"TABLE":    tokenTABLE,
	"INSERT":   tokenINSERT,
	"INTO":     tokenINTO,
	"DATABASE": tokenDATABASE,
	"VALUES":   tokenVALUES,
	"SELECT":   tokenSELECT,
	"FROM":     tokenFROM,
	"WHERE":    tokenWHERE,
	"ORDER":    tokenORDER,
	"BY":       tokenBY,
	"TOP":      tokenTOP,
	"AS":       tokenAS,
	"NOT":      tokenNOT,
	"NULL":     tokenNULL,
	"AND":      tokenAND,
	"OR":       tokenOR,
	"ASC":      tokenASC,
	"DESC":     tokenDESC,
	"GO":       tokenGO,
	"IS":       tokenIS,
	"IN":       tokenIN,
	"BETWEEN":  tokenBETWEEN,
	"LIKE":     tokenLIKE,
	"DELETE":   tokenDELETE,
	"UPDATE":   tokenUPDATE,
	"SET":      tokenSET,
	"DROP":     tokenDROP,
	"DISTINCT": tokenDISTINCT,
	"GROUP":    tokenGROUP,
	"HAVING":   tokenHAVING,
	"CASE":     tokenCASE,
	"WHEN":     tokenWHEN,
	"THEN":     tokenTHEN,
	"ELSE":     tokenELSE,
	"END":      tokenEND,
	"ISNULL":   tokenISNULL,
	"CONVERT":  tokenCONVERT,
	"GETDATE":  tokenGETDATE,
	"JOIN":     tokenJOIN,
	"INNER":    tokenINNER,
	"LEFT":     tokenLEFT,
	"RIGHT":    tokenRIGHT,
	"FULL":     tokenFULL,
	"OUTER":    tokenOUTER,
	"CROSS":    tokenCROSS,
	"ON":       tokenON,

	// Extended DDL keywords.
	"ALTER":      tokenALTER,
	"COLUMN":     tokenCOLUMN,
	"CONSTRAINT": tokenCONSTRAINT,
	"INDEX":      tokenINDEX,
	"VIEW":       tokenVIEW,
	"PROCEDURE":  tokenPROCEDURE,
	"PROC":       tokenPROCEDURE, // T-SQL alias
	"FUNCTION":   tokenFUNCTION,
	"TRIGGER":    tokenTRIGGER,
	"TRUNCATE":   tokenTRUNCATE,
	"IF":         tokenIF,
	"EXISTS":     tokenEXISTS,
	"UNIQUE":     tokenUNIQUE,
	"INCLUDE":    tokenINCLUDE,
	"REFERENCES": tokenREFERENCES,
	"PRIMARY":    tokenPRIMARY,
	"KEY":        tokenKEY,
	"FOREIGN":    tokenFOREIGN,
	"CHECK":      tokenCHECK,
	"ADD":        tokenADD,

	// DML extension keywords.
	"MERGE":   tokenMERGE,
	"USING":   tokenUSING,
	"MATCHED": tokenMATCHED,
	"OUTPUT":  tokenOUTPUT,

	// Phase 2 keywords.
	"UNION":     tokenUNION,
	"INTERSECT": tokenINTERSECT,
	"EXCEPT":    tokenEXCEPT,
	"ALL":       tokenALL,
	"WITH":      tokenWITH,
	"ANY":       tokenANY,
	"SOME":      tokenSOME,
	"OVER":      tokenOVER,
	"PARTITION": tokenPARTITION,
	"OFFSET":    tokenOFFSET,
	"FETCH":     tokenFETCH,
	"NEXT":      tokenNEXT,
	"FIRST":     tokenFIRST,
	"ONLY":      tokenONLY,
	"ROWS":      tokenROWS,
	"ROW":       tokenROW,

	// Transaction-related keywords.
	"BEGIN":       tokenBEGIN,
	"TRAN":        tokenTRAN,
	"TRANSACTION": tokenTRANSACTION,
	"COMMIT":      tokenCOMMIT,
	"ROLLBACK":    tokenROLLBACK,
	"SAVE":        tokenSAVE,

	// Sybase ASE pagination keyword.
	"LIMIT": tokenLIMIT,

	// Type system keywords.
	"IDENTITY": tokenIDENTITY,
	"DEFAULT":  tokenDEFAULT,
}

// token represents a single lexical token from T-SQL input.
type token struct {
	typ tokenType
	val string
	pos int // byte offset in the input
}

func (t token) String() string {
	switch t.typ {
	case tokenEOF:
		return "EOF"
	case tokenError:
		return fmt.Sprintf("error(%s)", t.val)
	default:
		return t.val
	}
}

// lexer tokenizes T-SQL input. It handles case-insensitive keywords, bracket-
// quoted identifiers ([name]), single-quoted strings with " escaping, and the
// standard T-SQL punctuation.
type lexer struct {
	input string
	pos   int
	// peeked is non-nil when a token has been peeked but not consumed.
	peeked *token
}

func newLexer(input string) *lexer {
	return &lexer{input: input}
}

// next returns the next token, consuming it.
func (l *lexer) next() token {
	if l.peeked != nil {
		t := *l.peeked
		l.peeked = nil
		return t
	}
	return l.scan()
}

// peek returns the next token without consuming it.
func (l *lexer) peek() token {
	if l.peeked != nil {
		return *l.peeked
	}
	t := l.scan()
	l.peeked = &t
	return t
}

// scan reads the next token from the input.
func (l *lexer) scan() token {
	l.skipWhitespaceAndComments()
	if l.pos >= len(l.input) {
		return token{typ: tokenEOF, pos: l.pos}
	}

	start := l.pos
	ch := l.input[l.pos]

	switch {
	case ch == '(':
		l.pos++
		return token{typ: tokenLParen, val: "(", pos: start}
	case ch == ')':
		l.pos++
		return token{typ: tokenRParen, val: ")", pos: start}
	case ch == ',':
		l.pos++
		return token{typ: tokenComma, val: ",", pos: start}
	case ch == '.':
		l.pos++
		return token{typ: tokenDot, val: ".", pos: start}
	case ch == ';':
		l.pos++
		return token{typ: tokenSemicolon, val: ";", pos: start}
	case ch == '*':
		l.pos++
		return token{typ: tokenStar, val: "*", pos: start}
	case ch == '+':
		l.pos++
		return token{typ: tokenPlus, val: "+", pos: start}
	case ch == '-':
		l.pos++
		return token{typ: tokenMinus, val: "-", pos: start}
	case ch == '/':
		l.pos++
		return token{typ: tokenSlash, val: "/", pos: start}
	case ch == '%':
		l.pos++
		return token{typ: tokenPercent, val: "%", pos: start}
	case ch == '=':
		l.pos++
		return token{typ: tokenEq, val: "=", pos: start}
	case ch == '!':
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return token{typ: tokenNeq, val: "!=", pos: start}
		}
		return token{typ: tokenError, val: "unexpected '!'", pos: start}
	case ch == '<':
		l.pos++
		if l.pos < len(l.input) {
			if l.input[l.pos] == '>' {
				l.pos++
				return token{typ: tokenNeq, val: "<>", pos: start}
			}
			if l.input[l.pos] == '=' {
				l.pos++
				return token{typ: tokenLTE, val: "<=", pos: start}
			}
		}
		return token{typ: tokenLT, val: "<", pos: start}
	case ch == '>':
		l.pos++
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return token{typ: tokenGTE, val: ">=", pos: start}
		}
		return token{typ: tokenGT, val: ">", pos: start}

	case ch == '\'':
		return l.scanString()
	case ch == '[':
		return l.scanBracketIdent()
	case isDigit(ch):
		return l.scanNumber()
	case isIdentStart(ch):
		return l.scanIdent()
	default:
		l.pos++
		return token{typ: tokenError, val: fmt.Sprintf("unexpected character %q", ch), pos: start}
	}
}

// scanString scans a single-quoted string literal. T-SQL uses " to escape
// embedded single quotes.
func (l *lexer) scanString() token {
	start := l.pos
	l.pos++ // skip opening '
	var b strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\'' {
			l.pos++
			// Check for escaped quote ('').
			if l.pos < len(l.input) && l.input[l.pos] == '\'' {
				b.WriteByte('\'')
				l.pos++
				continue
			}
			return token{typ: tokenString, val: b.String(), pos: start}
		}
		b.WriteByte(ch)
		l.pos++
	}
	return token{typ: tokenError, val: "unterminated string literal", pos: start}
}

// scanBracketIdent scans a bracket-quoted identifier ([name]).
func (l *lexer) scanBracketIdent() token {
	start := l.pos
	l.pos++ // skip [
	end := strings.IndexByte(l.input[l.pos:], ']')
	if end < 0 {
		return token{typ: tokenError, val: "unterminated bracket identifier", pos: start}
	}
	val := l.input[l.pos : l.pos+end]
	l.pos += end + 1 // skip past ]
	return token{typ: tokenIdent, val: val, pos: start}
}

// scanNumber scans an integer or floating-point literal.
func (l *lexer) scanNumber() token {
	start := l.pos
	for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
		l.pos++
	}
	// Check for decimal point.
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		l.pos++
		for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
			l.pos++
		}
		return token{typ: tokenFloat, val: l.input[start:l.pos], pos: start}
	}
	return token{typ: tokenInt, val: l.input[start:l.pos], pos: start}
}

// scanIdent scans an identifier or keyword. T-SQL identifiers can start with
// a letter, underscore, #, or @.
func (l *lexer) scanIdent() token {
	start := l.pos
	for l.pos < len(l.input) && isIdentByte(l.input[l.pos]) {
		l.pos++
	}
	val := l.input[start:l.pos]
	upper := strings.ToUpper(val)
	if typ, ok := keywords[upper]; ok {
		return token{typ: typ, val: val, pos: start}
	}
	return token{typ: tokenIdent, val: val, pos: start}
}

// skipWhitespaceAndComments skips whitespace and SQL comments (-- line
// comments and /* block comments */).
func (l *lexer) skipWhitespaceAndComments() {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if unicode.IsSpace(rune(ch)) {
			l.pos++
			continue
		}
		// Line comment: -- until end of line
		if ch == '-' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '-' {
			l.pos += 2
			for l.pos < len(l.input) && l.input[l.pos] != '\n' {
				l.pos++
			}
			continue
		}
		// Block comment: /* ... */
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
		break
	}
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || ch == '#' || ch == '@'
}

func isIdentByte(ch byte) bool {
	return isIdentStart(ch) || isDigit(ch)
}
