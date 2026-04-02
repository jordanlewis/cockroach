// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package parser implements a recursive descent parser for a subset of
// the Cassandra Query Language (CQL). It produces an AST that downstream
// phases translate into CockroachDB SQL operations.
package parser

// Statement is the interface implemented by every top-level CQL statement.
type Statement interface {
	statementNode()
}

// UseStatement represents USE <keyspace>.
type UseStatement struct {
	Keyspace string
}

func (*UseStatement) statementNode() {}

// CreateKeyspaceStatement represents
// CREATE KEYSPACE [IF NOT EXISTS] <name> WITH replication = { ... } [AND durable_writes = ...].
type CreateKeyspaceStatement struct {
	Keyspace    string
	IfNotExists bool
	Replication map[string]string
	// DurableWrites stores the AND durable_writes = <bool> option, if present.
	// nil means unset.
	DurableWrites *bool
}

func (*CreateKeyspaceStatement) statementNode() {}

// ColumnDef describes a single column in a CREATE TABLE statement.
type ColumnDef struct {
	Name     string
	DataType DataType
}

// DataType represents a CQL data type.
type DataType struct {
	// Name is the canonical lowercase type name
	// (e.g. "text", "int", "bigint", "uuid", "timestamp").
	Name string
}

// PrimaryKey describes the primary key of a table. PartitionKeys form the
// partition portion; ClusteringKeys follow.
//
//	PRIMARY KEY ((pk1, pk2), ck1, ck2)
//
// A single partition key is often written without the inner parens:
//
//	PRIMARY KEY (pk, ck1, ck2)
type PrimaryKey struct {
	PartitionKeys  []string
	ClusteringKeys []string
}

// CreateTableStatement represents
// CREATE TABLE [IF NOT EXISTS] <table> ( <cols> PRIMARY KEY (...) ).
type CreateTableStatement struct {
	Table       string
	Keyspace    string // empty when unqualified
	IfNotExists bool
	Columns     []ColumnDef
	PrimaryKey  PrimaryKey
}

func (*CreateTableStatement) statementNode() {}

// InsertStatement represents
// INSERT INTO <table> (<cols>) VALUES (<vals>) [IF NOT EXISTS].
type InsertStatement struct {
	Table       string
	Keyspace    string // empty when unqualified
	Columns     []string
	Values      []Expr
	IfNotExists bool
}

func (*InsertStatement) statementNode() {}

// UpdateStatement represents
// UPDATE [<ks>.]<table> SET <col> = <val>, ... WHERE <conds> [IF <conds>|IF EXISTS].
type UpdateStatement struct {
	Table    string
	Keyspace string // empty when unqualified
	// Assignments is the list of SET assignments: col = val.
	Assignments []Assignment
	Where       []WhereClause
	IfExists    bool
	IfConds     []WhereClause // IF col = val conditions (empty when not conditional)
}

func (*UpdateStatement) statementNode() {}

// Assignment represents a SET assignment: <col> = <val>.
type Assignment struct {
	Column string
	Value  Expr
}

// DeleteStatement represents
// DELETE FROM [<ks>.]<table> WHERE <conds> [IF <conds>|IF EXISTS].
type DeleteStatement struct {
	Table    string
	Keyspace string // empty when unqualified
	Where    []WhereClause
	IfExists bool
	IfConds  []WhereClause // IF col = val conditions (empty when not conditional)
}

func (*DeleteStatement) statementNode() {}

// SelectStatement represents
// SELECT [DISTINCT] <cols> FROM <table> [WHERE <conds>]
// [ORDER BY <col> [ASC|DESC], ...] [LIMIT <n>] [ALLOW FILTERING].
type SelectStatement struct {
	Table    string
	Keyspace string // empty when unqualified
	Columns  []Selector
	Distinct bool
	Where    []WhereClause
	OrderBy  []OrderByClause
	Limit    Expr // nil if no LIMIT
}

func (*SelectStatement) statementNode() {}

// Selector is a single item in a SELECT list.
type Selector struct {
	// Column is the column name. "*" represents all columns.
	Column string
}

// OrderByClause specifies a single column ordering in ORDER BY.
type OrderByClause struct {
	Column string
	Desc   bool
}

// WhereClause is a single <col> <op> <val> condition. For the IN operator,
// Value is a *TupleLiteral containing the list of values.
type WhereClause struct {
	Column   string
	Operator string // "=", "<", ">", "<=", ">=", "!=", "IN"
	Value    Expr
}

// Expr is the interface for value expressions in CQL.
type Expr interface {
	exprNode()
}

// StringLiteral is a single-quoted string value.
type StringLiteral struct {
	Value string
}

func (*StringLiteral) exprNode() {}

// IntegerLiteral is an integer constant.
type IntegerLiteral struct {
	Value int64
}

func (*IntegerLiteral) exprNode() {}

// FloatLiteral is a floating-point constant.
type FloatLiteral struct {
	Value float64
}

func (*FloatLiteral) exprNode() {}

// BoolLiteral is a true/false value.
type BoolLiteral struct {
	Value bool
}

func (*BoolLiteral) exprNode() {}

// UUIDLiteral is a UUID value written as a bare hex-dash string.
type UUIDLiteral struct {
	Value string
}

func (*UUIDLiteral) exprNode() {}

// NullLiteral represents the null value.
type NullLiteral struct{}

func (*NullLiteral) exprNode() {}

// BindMarker is a ? positional placeholder.
type BindMarker struct{}

func (*BindMarker) exprNode() {}

// NamedBindMarker is a :name placeholder.
type NamedBindMarker struct {
	Name string
}

func (*NamedBindMarker) exprNode() {}

// TupleLiteral is a parenthesized list of values, used with the IN operator:
// WHERE col IN (val1, val2, ...).
type TupleLiteral struct {
	Values []Expr
}

func (*TupleLiteral) exprNode() {}
