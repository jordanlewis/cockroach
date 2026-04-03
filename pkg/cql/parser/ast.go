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

// AlterKeyspaceStatement represents
// ALTER KEYSPACE <name> WITH replication = { ... } [AND durable_writes = ...].
type AlterKeyspaceStatement struct {
	Keyspace      string
	Replication   map[string]string
	DurableWrites *bool
}

func (*AlterKeyspaceStatement) statementNode() {}

// ColumnDef describes a single column in a CREATE TABLE statement.
type ColumnDef struct {
	Name     string
	DataType DataType
	IsStatic bool // STATIC keyword present (Cassandra per-partition shared column)
}

// DataType represents a CQL data type. Scalar types have an empty Params
// slice; parameterized types (list<T>, map<K,V>, frozen<T>, tuple<...>)
// carry their element types in Params.
type DataType struct {
	// Name is the canonical lowercase type name
	// (e.g. "text", "int", "list", "map", "frozen", "tuple").
	Name string
	// Params holds type parameters for parameterized types.
	// For example, list<text> has Params: [{Name: "text"}],
	// map<text, int> has Params: [{Name: "text"}, {Name: "int"}].
	Params []DataType
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
// CREATE TABLE [IF NOT EXISTS] <table> ( <cols> PRIMARY KEY (...) )
// [WITH <properties> | CLUSTERING ORDER BY (...)].
type CreateTableStatement struct {
	Table           string
	Keyspace        string // empty when unqualified
	IfNotExists     bool
	Columns         []ColumnDef
	PrimaryKey      PrimaryKey
	WithProperties  []TableProperty        // WITH key = value [AND ...]
	ClusteringOrder []ClusteringOrderEntry // WITH CLUSTERING ORDER BY (...)
}

// TableProperty is a single key = value option in a CREATE TABLE WITH clause.
// These are parsed for CQL compatibility but silently ignored during
// translation to CockroachDB SQL.
type TableProperty struct {
	Key      string
	Value    Expr              // non-nil for scalar values (int, string)
	MapValue map[string]string // non-nil for map values ({...})
}

// ClusteringOrderEntry specifies the storage sort order for a single
// clustering column. Parsed from WITH CLUSTERING ORDER BY (...).
type ClusteringOrderEntry struct {
	Column string
	Desc   bool
}

func (*CreateTableStatement) statementNode() {}

// UsingClause holds optional USING TTL and/or USING TIMESTAMP modifiers
// on DML statements (INSERT, UPDATE, DELETE). These are accepted by the
// parser for CQL compatibility but silently ignored during translation —
// CockroachDB does not support per-row TTL or write timestamps.
type UsingClause struct {
	TTL       Expr // USING TTL <seconds>, nil if absent
	Timestamp Expr // USING TIMESTAMP <microseconds>, nil if absent
}

// InsertStatement represents
// INSERT INTO <table> (<cols>) VALUES (<vals>) [IF NOT EXISTS] [USING ...]
// or INSERT INTO <table> JSON '<json>' [DEFAULT UNSET|NULL] [IF NOT EXISTS].
type InsertStatement struct {
	Table       string
	Keyspace    string // empty when unqualified
	Columns     []string
	Values      []Expr
	IfNotExists bool
	Using       *UsingClause // USING TTL/TIMESTAMP, nil if absent
	// JSON is true for INSERT INTO <table> JSON '<json>' syntax.
	JSON         bool
	JSONValue    string // the JSON string for INSERT JSON
	DefaultUnset bool   // DEFAULT UNSET
	DefaultNull  bool   // DEFAULT NULL
}

func (*InsertStatement) statementNode() {}

// UpdateStatement represents
// UPDATE [<ks>.]<table> [USING ...] SET <col> = <val>, ... WHERE <conds>
// [IF <conds>|IF EXISTS].
type UpdateStatement struct {
	Table    string
	Keyspace string       // empty when unqualified
	Using    *UsingClause // USING TTL/TIMESTAMP, nil if absent
	// Assignments is the list of SET assignments: col = val.
	Assignments []Assignment
	Where       []WhereClause
	IfExists    bool
	IfConds     []WhereClause // IF col = val conditions (empty when not conditional)
}

func (*UpdateStatement) statementNode() {}

// Assignment represents a SET assignment: <col> = <val> or <col>[<key>] = <val>.
type Assignment struct {
	Column    string
	Subscript Expr // non-nil for col[key] = val (map element update)
	Value     Expr
}

// DeleteTarget is a single column reference in a column-level DELETE,
// optionally with a subscript for map entry removal:
// DELETE col FROM ... or DELETE col['key'] FROM ....
type DeleteTarget struct {
	Column    string
	Subscript Expr // non-nil for col[key] (map entry or list element removal)
}

// DeleteStatement represents
// DELETE [<cols>] FROM [<ks>.]<table> [USING TIMESTAMP ...] WHERE <conds>
// [IF <conds>|IF EXISTS].
//
// When Columns is non-empty, this is a column-level DELETE (Cassandra
// tombstone semantics): the named columns are set to NULL rather than
// removing the entire row. When a column has a Subscript, only the
// specified map entry or list element is removed.
type DeleteStatement struct {
	Table    string
	Keyspace string         // empty when unqualified
	Columns  []DeleteTarget // column-level DELETE; empty means whole-row DELETE
	Using    *UsingClause   // USING TIMESTAMP, nil if absent
	Where    []WhereClause
	IfExists bool
	IfConds  []WhereClause // IF col = val conditions (empty when not conditional)
}

func (*DeleteStatement) statementNode() {}

// SelectStatement represents
// SELECT [JSON] [DISTINCT] <cols> FROM <table> [WHERE <conds>]
// [GROUP BY <cols>] [ORDER BY <col> [ASC|DESC], ...]
// [PER PARTITION LIMIT <n>] [LIMIT <n>] [ALLOW FILTERING].
type SelectStatement struct {
	Table             string
	Keyspace          string // empty when unqualified
	Columns           []Selector
	Distinct          bool
	JSON              bool // SELECT JSON
	Where             []WhereClause
	GroupBy           []string
	OrderBy           []OrderByClause
	PerPartitionLimit Expr // nil if no PER PARTITION LIMIT
	Limit             Expr // nil if no LIMIT
}

func (*SelectStatement) statementNode() {}

// Selector is a single item in a SELECT list. For simple column
// references, Column is set. For function calls or CAST expressions,
// Expr is non-nil and takes precedence over Column during translation.
type Selector struct {
	// Column is the column name. "*" represents all columns.
	Column string
	// Expr is non-nil when the selector is a function call or CAST.
	Expr Expr
	// Alias is the optional AS alias.
	Alias string
}

// OrderByClause specifies a single column ordering in ORDER BY.
type OrderByClause struct {
	Column string
	Desc   bool
}

// WhereClause is a single <col> <op> <val> condition. For the IN operator,
// Value is a *TupleLiteral containing the list of values. When the left-hand
// side is a function call (e.g. token(pk) > 0), ColumnExpr is non-nil.
// For multi-column IN (e.g. (col1, col2) IN ((1,'a'), (2,'b'))), Columns
// is non-nil and Column is empty.
type WhereClause struct {
	Column     string
	Columns    []string // non-nil for multi-column IN: (col1, col2) IN (...)
	ColumnExpr Expr     // non-nil when left side is a function call
	Operator   string   // "=", "<", ">", "<=", ">=", "!=", "IN", "CONTAINS", "CONTAINS KEY"
	Value      Expr
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

// BigIntLiteral is an integer constant that exceeds int64 range. The value
// is stored as a raw decimal string and passed through to CRDB as a DECIMAL.
type BigIntLiteral struct {
	Value string
}

func (*BigIntLiteral) exprNode() {}

// BlobLiteral is a CQL blob hex literal (0xdeadbeef). The Value is the
// hex string without the 0x prefix.
type BlobLiteral struct {
	Value string // hex digits only, e.g. "deadbeef"
}

func (*BlobLiteral) exprNode() {}

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

// ListLiteral is a square-bracket-delimited list of values: [val1, val2, ...].
type ListLiteral struct {
	Values []Expr
}

func (*ListLiteral) exprNode() {}

// SetLiteral is a brace-delimited set of values: {val1, val2, ...}.
type SetLiteral struct {
	Values []Expr
}

func (*SetLiteral) exprNode() {}

// MapExprLiteral is a brace-delimited map of key-value pairs:
// {key1: val1, key2: val2, ...}.
type MapExprLiteral struct {
	Entries []MapEntry
}

func (*MapExprLiteral) exprNode() {}

// MapEntry is a single key-value pair in a MapExprLiteral.
type MapEntry struct {
	Key   Expr
	Value Expr
}

// CounterExpr represents a counter increment/decrement expression:
// col + val or col - val.
type CounterExpr struct {
	Column string
	Op     string // "+" or "-"
	Value  Expr
}

func (*CounterExpr) exprNode() {}

// FunctionCall represents a function invocation: name(args...).
// For aggregate functions like COUNT(DISTINCT col), Distinct is true.
type FunctionCall struct {
	Name     string
	Args     []Expr
	Distinct bool // COUNT(DISTINCT col)
}

func (*FunctionCall) exprNode() {}

// StarExpr represents * used as a function argument (e.g. COUNT(*)).
type StarExpr struct{}

func (*StarExpr) exprNode() {}

// ColumnRef is a column name used as an expression, typically as a function
// argument (e.g. the pk in token(pk)).
type ColumnRef struct {
	Name string
}

func (*ColumnRef) exprNode() {}

// CollectionOpExpr represents a binary collection operation used in SET
// assignments: <left> + <right> (e.g. [1,2] + col for list prepend) or
// <left> - <right>. Unlike CounterExpr, the left side can be an arbitrary
// expression (not just a column name).
type CollectionOpExpr struct {
	Left  Expr
	Op    string // "+" or "-"
	Right Expr
}

func (*CollectionOpExpr) exprNode() {}

// CastExpr represents CAST(expr AS type).
type CastExpr struct {
	Expr Expr
	Type DataType
}

func (*CastExpr) exprNode() {}

// AlterTableStatement represents ALTER TABLE [IF EXISTS] [<ks>.]<table> <op>.
type AlterTableStatement struct {
	Table    string
	Keyspace string
	IfExists bool
	Op       AlterTableOp
}

func (*AlterTableStatement) statementNode() {}

// AlterTableOp is the interface implemented by ALTER TABLE operation types.
type AlterTableOp interface {
	alterTableOp()
}

// AlterTableAdd represents ALTER TABLE ... ADD <col> <type>.
type AlterTableAdd struct {
	Column   string
	DataType DataType
}

func (*AlterTableAdd) alterTableOp() {}

// AlterTableDrop represents ALTER TABLE ... DROP <col>.
type AlterTableDrop struct {
	Column string
}

func (*AlterTableDrop) alterTableOp() {}

// AlterTableRename represents ALTER TABLE ... RENAME <old> TO <new>.
type AlterTableRename struct {
	OldName string
	NewName string
}

func (*AlterTableRename) alterTableOp() {}

// AlterTableAlterType represents ALTER TABLE ... ALTER <col> TYPE <type>.
type AlterTableAlterType struct {
	Column   string
	DataType DataType
}

func (*AlterTableAlterType) alterTableOp() {}

// AlterTableWith represents ALTER TABLE ... WITH <properties>.
// Properties are stored as raw key-value pairs; the translator
// decides what to do with them.
type AlterTableWith struct {
	Properties []TableProperty
}

func (*AlterTableWith) alterTableOp() {}

// CreateIndexStatement represents CREATE [CUSTOM] INDEX on a table column.
type CreateIndexStatement struct {
	IndexName   string
	IfNotExists bool
	Table       string
	Keyspace    string
	Columns     []IndexColumn
	IsCustom    bool
	UsingClass  string // for CUSTOM INDEX ... USING '<class>'
}

func (*CreateIndexStatement) statementNode() {}

// IndexColumn describes a column reference in a CREATE INDEX statement,
// optionally wrapped in a collection indexing function (KEYS, VALUES,
// ENTRIES, or FULL).
type IndexColumn struct {
	Name     string
	Function string // "KEYS", "VALUES", "ENTRIES", "FULL", or ""
}

// DropStatement represents DROP TABLE/KEYSPACE/INDEX [IF EXISTS] <name>.
type DropStatement struct {
	ObjectType string // "TABLE", "KEYSPACE", "INDEX"
	Name       string
	Keyspace   string // empty when unqualified
	IfExists   bool
}

func (*DropStatement) statementNode() {}

// TruncateStatement represents TRUNCATE [TABLE] [<ks>.]<name>.
type TruncateStatement struct {
	Table    string
	Keyspace string
}

func (*TruncateStatement) statementNode() {}

// BatchStatement represents BEGIN [UNLOGGED|COUNTER] BATCH
// [USING TIMESTAMP <ts>] <stmts> APPLY BATCH.
type BatchStatement struct {
	Type       string // "", "UNLOGGED", "COUNTER"
	Statements []Statement
	Timestamp  *int64
}

func (*BatchStatement) statementNode() {}

// CreateTypeStatement represents CREATE TYPE for user-defined types.
type CreateTypeStatement struct {
	TypeName    string
	Keyspace    string
	IfNotExists bool
	Fields      []ColumnDef
}

func (*CreateTypeStatement) statementNode() {}

// AlterTypeStatement represents ALTER TYPE for user-defined types.
type AlterTypeStatement struct {
	TypeName string
	Keyspace string
	Op       AlterTypeOp
}

func (*AlterTypeStatement) statementNode() {}

// AlterTypeOp is the interface implemented by ALTER TYPE operation types.
type AlterTypeOp interface {
	alterTypeOp()
}

// AlterTypeAddField represents ALTER TYPE ... ADD <field> <type>.
type AlterTypeAddField struct {
	Field    string
	DataType DataType
}

func (*AlterTypeAddField) alterTypeOp() {}

// AlterTypeRenameField represents ALTER TYPE ... RENAME <old> TO <new>.
type AlterTypeRenameField struct {
	OldName string
	NewName string
}

func (*AlterTypeRenameField) alterTypeOp() {}

// AlterTypeAlterField represents ALTER TYPE ... ALTER <field> TYPE <type>.
type AlterTypeAlterField struct {
	Field    string
	DataType DataType
}

func (*AlterTypeAlterField) alterTypeOp() {}

// SubscriptExpr represents element access on a collection: col[index].
// For lists, Index is an integer (positional access); for maps, Index is
// the map key expression (key lookup). Translates to CRDB's JSONB -> operator.
type SubscriptExpr struct {
	Column string
	Index  Expr
}

func (*SubscriptExpr) exprNode() {}

// FieldAccessExpr represents a composite type field access: col.field.
// In CQL this appears as column.field_name in SELECT lists.
type FieldAccessExpr struct {
	Column string
	Field  string
}

func (*FieldAccessExpr) exprNode() {}
