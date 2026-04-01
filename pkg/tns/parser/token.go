// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser

import "fmt"

// TokenType identifies the type of a lexical token.
type TokenType int

const (
	// Special tokens.

	ILLEGAL TokenType = iota
	EOF
	COMMENT

	// Literals.

	IDENT  // identifier (quoted or unquoted)
	NUMBER // integer or decimal number
	STRING // 'single-quoted string'
	BIND   // :name bind variable

	// Operators and delimiters.

	PLUS     // +
	MINUS    // -
	STAR     // *
	SLASH    // /
	EQ       // =
	NEQ      // <> or !=
	LT       // <
	GT       // >
	LTE      // <=
	GTE      // >=
	CONCAT   // ||
	LPAREN   // (
	RPAREN   // )
	COMMA    // ,
	SEMI     // ;
	DOT      // .
	COLON    // :
	PERCENT  // %

	keywordStart // marks the beginning of keyword token types

	// SQL keywords.

	SELECT
	FROM
	WHERE
	INSERT
	INTO
	VALUES
	UPDATE
	SET
	DELETE
	AND
	OR
	NOT
	NULL
	IS
	IN
	BETWEEN
	LIKE
	EXISTS
	AS
	ON
	JOIN
	LEFT
	RIGHT
	FULL
	INNER
	OUTER
	CROSS
	ORDER
	BY
	GROUP
	HAVING
	UNION
	ALL
	DISTINCT
	ASC
	DESC
	CASE
	WHEN
	THEN
	ELSE
	END
	LIMIT
	OFFSET
	FETCH
	FIRST
	NEXT
	ROWS
	ONLY
	WITH
	RECURSIVE

	// Oracle-specific keywords.

	ROWNUM
	DUAL
	SYSDATE
	SYSTIMESTAMP
	NVL
	NVL2
	DECODE
	TO_CHAR
	TO_DATE
	TO_NUMBER
	NEXTVAL
	CURRVAL
	ROWID
	LEVEL
	CONNECT
	PRIOR
	START
	NOCACHE
	CACHE
	CYCLE
	NOCYCLE
	MINVALUE
	MAXVALUE
	NOMINVALUE
	NOMAXVALUE
	INCREMENT
	SEQUENCE
	CREATE
	TABLE
	ALTER
	DROP
	MINUS_KW // MINUS keyword (set operation, distinct from - operator)
	INTERSECT

	keywordEnd // marks the end of keyword token types
)

// keywords maps uppercase keyword strings to their TokenType.
var keywords map[string]TokenType

func init() {
	keywords = map[string]TokenType{
		"SELECT":       SELECT,
		"FROM":         FROM,
		"WHERE":        WHERE,
		"INSERT":       INSERT,
		"INTO":         INTO,
		"VALUES":       VALUES,
		"UPDATE":       UPDATE,
		"SET":          SET,
		"DELETE":       DELETE,
		"AND":          AND,
		"OR":           OR,
		"NOT":          NOT,
		"NULL":         NULL,
		"IS":           IS,
		"IN":           IN,
		"BETWEEN":      BETWEEN,
		"LIKE":         LIKE,
		"EXISTS":       EXISTS,
		"AS":           AS,
		"ON":           ON,
		"JOIN":         JOIN,
		"LEFT":         LEFT,
		"RIGHT":        RIGHT,
		"FULL":         FULL,
		"INNER":        INNER,
		"OUTER":        OUTER,
		"CROSS":        CROSS,
		"ORDER":        ORDER,
		"BY":           BY,
		"GROUP":        GROUP,
		"HAVING":       HAVING,
		"UNION":        UNION,
		"ALL":          ALL,
		"DISTINCT":     DISTINCT,
		"ASC":          ASC,
		"DESC":         DESC,
		"CASE":         CASE,
		"WHEN":         WHEN,
		"THEN":         THEN,
		"ELSE":         ELSE,
		"END":          END,
		"LIMIT":        LIMIT,
		"OFFSET":       OFFSET,
		"FETCH":        FETCH,
		"FIRST":        FIRST,
		"NEXT":         NEXT,
		"ROWS":         ROWS,
		"ONLY":         ONLY,
		"WITH":         WITH,
		"RECURSIVE":    RECURSIVE,
		"ROWNUM":       ROWNUM,
		"DUAL":         DUAL,
		"SYSDATE":      SYSDATE,
		"SYSTIMESTAMP":  SYSTIMESTAMP,
		"NVL":          NVL,
		"NVL2":         NVL2,
		"DECODE":       DECODE,
		"TO_CHAR":      TO_CHAR,
		"TO_DATE":      TO_DATE,
		"TO_NUMBER":    TO_NUMBER,
		"NEXTVAL":      NEXTVAL,
		"CURRVAL":      CURRVAL,
		"ROWID":        ROWID,
		"LEVEL":        LEVEL,
		"CONNECT":      CONNECT,
		"PRIOR":        PRIOR,
		"START":        START,
		"NOCACHE":      NOCACHE,
		"CACHE":        CACHE,
		"CYCLE":        CYCLE,
		"NOCYCLE":      NOCYCLE,
		"MINVALUE":     MINVALUE,
		"MAXVALUE":     MAXVALUE,
		"NOMINVALUE":   NOMINVALUE,
		"NOMAXVALUE":   NOMAXVALUE,
		"INCREMENT":    INCREMENT,
		"SEQUENCE":     SEQUENCE,
		"CREATE":       CREATE,
		"TABLE":        TABLE,
		"ALTER":        ALTER,
		"DROP":         DROP,
		"MINUS":        MINUS_KW,
		"INTERSECT":    INTERSECT,
	}
}

// LookupKeyword returns the keyword TokenType for s (which must be
// uppercase), or IDENT if s is not a keyword.
func LookupKeyword(s string) TokenType {
	if tok, ok := keywords[s]; ok {
		return tok
	}
	return IDENT
}

// Token represents a lexical token with its type, literal text, and position.
type Token struct {
	Type    TokenType
	Literal string // raw text of the token
	Pos     int    // byte offset in the input
	Line    int    // 1-based line number
	Col     int    // 1-based column number
}

func (t Token) String() string {
	if t.Literal != "" {
		return fmt.Sprintf("%v(%q)", t.Type, t.Literal)
	}
	return t.Type.String()
}

// tokenTypeNames maps TokenType to its display name.
var tokenTypeNames = map[TokenType]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	COMMENT: "COMMENT",
	IDENT:   "IDENT",
	NUMBER:  "NUMBER",
	STRING:  "STRING",
	BIND:    "BIND",
	PLUS:    "+",
	MINUS:   "-",
	STAR:    "*",
	SLASH:   "/",
	EQ:      "=",
	NEQ:     "<>",
	LT:      "<",
	GT:      ">",
	LTE:     "<=",
	GTE:     ">=",
	CONCAT:  "||",
	LPAREN:  "(",
	RPAREN:  ")",
	COMMA:   ",",
	SEMI:    ";",
	DOT:     ".",
	COLON:   ":",
	PERCENT: "%",
}

func (t TokenType) String() string {
	if name, ok := tokenTypeNames[t]; ok {
		return name
	}
	// keywords
	for kw, tt := range keywords {
		if tt == t {
			return kw
		}
	}
	return fmt.Sprintf("TokenType(%d)", int(t))
}
