// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser

import "testing"

func TestParseUse(t *testing.T) {
	stmt, err := Parse("USE mykeyspace")
	if err != nil {
		t.Fatal(err)
	}
	u, ok := stmt.(*UseStatement)
	if !ok {
		t.Fatalf("expected *UseStatement, got %T", stmt)
	}
	if u.Keyspace != "mykeyspace" {
		t.Errorf("keyspace = %q, want %q", u.Keyspace, "mykeyspace")
	}
}

func TestParseUseSemicolon(t *testing.T) {
	stmt, err := Parse("USE ks;")
	if err != nil {
		t.Fatal(err)
	}
	u := stmt.(*UseStatement)
	if u.Keyspace != "ks" {
		t.Errorf("keyspace = %q, want %q", u.Keyspace, "ks")
	}
}

func TestParseCreateKeyspace(t *testing.T) {
	input := `CREATE KEYSPACE cycling
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	cs, ok := stmt.(*CreateKeyspaceStatement)
	if !ok {
		t.Fatalf("expected *CreateKeyspaceStatement, got %T", stmt)
	}
	if cs.Keyspace != "cycling" {
		t.Errorf("keyspace = %q, want %q", cs.Keyspace, "cycling")
	}
	if cs.IfNotExists {
		t.Error("IfNotExists should be false")
	}
	if cs.Replication["class"] != "SimpleStrategy" {
		t.Errorf("replication class = %q, want %q", cs.Replication["class"], "SimpleStrategy")
	}
	if cs.Replication["replication_factor"] != "3" {
		t.Errorf("replication_factor = %q, want %q",
			cs.Replication["replication_factor"], "3")
	}
}

func TestParseCreateKeyspaceIfNotExists(t *testing.T) {
	input := `CREATE KEYSPACE IF NOT EXISTS test_ks
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3'}
  AND durable_writes = false`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	cs := stmt.(*CreateKeyspaceStatement)
	if !cs.IfNotExists {
		t.Error("IfNotExists should be true")
	}
	if cs.DurableWrites == nil || *cs.DurableWrites {
		t.Error("DurableWrites should be false")
	}
}

func TestParseCreateTable(t *testing.T) {
	input := `CREATE TABLE cycling.cyclist_name (
    id uuid,
    lastname text,
    firstname text,
    PRIMARY KEY (id)
  )`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	ct, ok := stmt.(*CreateTableStatement)
	if !ok {
		t.Fatalf("expected *CreateTableStatement, got %T", stmt)
	}
	if ct.Keyspace != "cycling" {
		t.Errorf("keyspace = %q, want %q", ct.Keyspace, "cycling")
	}
	if ct.Table != "cyclist_name" {
		t.Errorf("table = %q, want %q", ct.Table, "cyclist_name")
	}
	if len(ct.Columns) != 3 {
		t.Fatalf("got %d columns, want 3", len(ct.Columns))
	}
	if ct.Columns[0].Name != "id" || ct.Columns[0].DataType.Name != "uuid" {
		t.Errorf("col 0: got %v, want id uuid", ct.Columns[0])
	}
	if len(ct.PrimaryKey.PartitionKeys) != 1 || ct.PrimaryKey.PartitionKeys[0] != "id" {
		t.Errorf("partition key = %v, want [id]", ct.PrimaryKey.PartitionKeys)
	}
	if len(ct.PrimaryKey.ClusteringKeys) != 0 {
		t.Errorf("clustering keys should be empty, got %v", ct.PrimaryKey.ClusteringKeys)
	}
}

func TestParseCreateTableCompositePK(t *testing.T) {
	input := `CREATE TABLE IF NOT EXISTS ks.events (
    tenant_id text,
    event_date timestamp,
    event_id uuid,
    data text,
    PRIMARY KEY ((tenant_id, event_date), event_id)
  )`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStatement)
	if !ct.IfNotExists {
		t.Error("IfNotExists should be true")
	}
	if len(ct.PrimaryKey.PartitionKeys) != 2 {
		t.Fatalf("partition keys = %v, want 2", ct.PrimaryKey.PartitionKeys)
	}
	if ct.PrimaryKey.PartitionKeys[0] != "tenant_id" ||
		ct.PrimaryKey.PartitionKeys[1] != "event_date" {
		t.Errorf("partition keys = %v", ct.PrimaryKey.PartitionKeys)
	}
	if len(ct.PrimaryKey.ClusteringKeys) != 1 ||
		ct.PrimaryKey.ClusteringKeys[0] != "event_id" {
		t.Errorf("clustering keys = %v, want [event_id]", ct.PrimaryKey.ClusteringKeys)
	}
}

func TestParseInsert(t *testing.T) {
	input := `INSERT INTO cycling.cyclist_name (id, lastname, firstname)
    VALUES ('123', 'DOE', 'Jane')`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	ins, ok := stmt.(*InsertStatement)
	if !ok {
		t.Fatalf("expected *InsertStatement, got %T", stmt)
	}
	if ins.Keyspace != "cycling" || ins.Table != "cyclist_name" {
		t.Errorf("table = %s.%s", ins.Keyspace, ins.Table)
	}
	if len(ins.Columns) != 3 {
		t.Fatalf("columns = %v, want 3", ins.Columns)
	}
	if len(ins.Values) != 3 {
		t.Fatalf("values count = %d, want 3", len(ins.Values))
	}
	sl, ok := ins.Values[0].(*StringLiteral)
	if !ok || sl.Value != "123" {
		t.Errorf("value 0 = %v, want StringLiteral('123')", ins.Values[0])
	}
}

func TestParseInsertIfNotExists(t *testing.T) {
	input := `INSERT INTO t (a, b) VALUES (1, 'x') IF NOT EXISTS`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStatement)
	if !ins.IfNotExists {
		t.Error("IfNotExists should be true")
	}
	v0 := ins.Values[0].(*IntegerLiteral)
	if v0.Value != 1 {
		t.Errorf("value 0 = %d, want 1", v0.Value)
	}
}

func TestParseInsertBindMarkers(t *testing.T) {
	input := `INSERT INTO t (a, b, c) VALUES (?, :name, 42)`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStatement)
	if _, ok := ins.Values[0].(*BindMarker); !ok {
		t.Errorf("value 0: want BindMarker, got %T", ins.Values[0])
	}
	nm, ok := ins.Values[1].(*NamedBindMarker)
	if !ok || nm.Name != "name" {
		t.Errorf("value 1: want NamedBindMarker(name), got %T(%v)", ins.Values[1], ins.Values[1])
	}
	il, ok := ins.Values[2].(*IntegerLiteral)
	if !ok || il.Value != 42 {
		t.Errorf("value 2: want IntegerLiteral(42), got %T(%v)", ins.Values[2], ins.Values[2])
	}
}

func TestParseSelectStar(t *testing.T) {
	input := `SELECT * FROM users`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected *SelectStatement, got %T", stmt)
	}
	if len(sel.Columns) != 1 || sel.Columns[0].Column != "*" {
		t.Errorf("columns = %v, want [*]", sel.Columns)
	}
	if sel.Table != "users" {
		t.Errorf("table = %q, want %q", sel.Table, "users")
	}
}

func TestParseSelectWithWhere(t *testing.T) {
	input := `SELECT name, age FROM ks.users WHERE id = '123' AND age > 21`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStatement)
	if sel.Keyspace != "ks" || sel.Table != "users" {
		t.Errorf("table = %s.%s", sel.Keyspace, sel.Table)
	}
	if len(sel.Columns) != 2 {
		t.Fatalf("columns count = %d, want 2", len(sel.Columns))
	}
	if sel.Columns[0].Column != "name" || sel.Columns[1].Column != "age" {
		t.Errorf("columns = %v", sel.Columns)
	}
	if len(sel.Where) != 2 {
		t.Fatalf("where clauses = %d, want 2", len(sel.Where))
	}
	if sel.Where[0].Column != "id" || sel.Where[0].Operator != "=" {
		t.Errorf("where[0] = %+v", sel.Where[0])
	}
	if sel.Where[1].Column != "age" || sel.Where[1].Operator != ">" {
		t.Errorf("where[1] = %+v", sel.Where[1])
	}
}

func TestParseSelectWithLimit(t *testing.T) {
	input := `SELECT * FROM events LIMIT 100`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStatement)
	lim, ok := sel.Limit.(*IntegerLiteral)
	if !ok || lim.Value != 100 {
		t.Errorf("limit = %v, want 100", sel.Limit)
	}
}

func TestParseSelectWhereAndLimit(t *testing.T) {
	input := `SELECT col1 FROM ks.tbl WHERE pk = 'x' LIMIT 10`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStatement)
	if len(sel.Where) != 1 || sel.Where[0].Column != "pk" {
		t.Errorf("where = %+v", sel.Where)
	}
	lim := sel.Limit.(*IntegerLiteral)
	if lim.Value != 10 {
		t.Errorf("limit = %d, want 10", lim.Value)
	}
}

func TestParseSelectBindMarkerInWhere(t *testing.T) {
	input := `SELECT * FROM t WHERE id = ?`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStatement)
	if _, ok := sel.Where[0].Value.(*BindMarker); !ok {
		t.Errorf("where value: want BindMarker, got %T", sel.Where[0].Value)
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		input string
		want  string // substring of error message
	}{
		{"", "expected statement keyword"},
		{"GRANT ALL ON t", "unsupported statement"},
		{"CREATE INDEX foo ON", "expected identifier"},
		{"USE", "expected identifier"},
		{"SELECT * FROM", "expected identifier"},
		{"INSERT INTO t (a) VALUES ('x'", "expected ')'"},
	}
	for _, tt := range tests {
		_, err := Parse(tt.input)
		if err == nil {
			t.Errorf("Parse(%q): expected error containing %q, got nil", tt.input, tt.want)
			continue
		}
		if got := err.Error(); !contains(got, tt.want) {
			t.Errorf("Parse(%q): error = %q, want substring %q", tt.input, got, tt.want)
		}
	}
}

func TestParseCreateIndexUnnamed(t *testing.T) {
	stmt, err := Parse("CREATE INDEX ON users (email)")
	if err != nil {
		t.Fatal(err)
	}
	ci, ok := stmt.(*CreateIndexStatement)
	if !ok {
		t.Fatalf("expected *CreateIndexStatement, got %T", stmt)
	}
	if ci.IndexName != "" {
		t.Errorf("IndexName = %q, want empty", ci.IndexName)
	}
	if ci.Table != "users" {
		t.Errorf("Table = %q, want %q", ci.Table, "users")
	}
	if len(ci.Columns) != 1 || ci.Columns[0].Name != "email" {
		t.Errorf("Columns = %+v, want [{Name: email}]", ci.Columns)
	}
}

func TestParseCreateIndexUnnamedIfNotExists(t *testing.T) {
	stmt, err := Parse("CREATE INDEX IF NOT EXISTS ON users (email)")
	if err != nil {
		t.Fatal(err)
	}
	ci := stmt.(*CreateIndexStatement)
	if ci.IndexName != "" {
		t.Errorf("IndexName = %q, want empty", ci.IndexName)
	}
	if !ci.IfNotExists {
		t.Errorf("IfNotExists = false, want true")
	}
	if ci.Table != "users" {
		t.Errorf("Table = %q, want %q", ci.Table, "users")
	}
}

func TestParseBoolAndNullLiterals(t *testing.T) {
	input := `INSERT INTO t (a, b, c) VALUES (true, false, null)`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStatement)
	if b, ok := ins.Values[0].(*BoolLiteral); !ok || !b.Value {
		t.Errorf("value 0: want BoolLiteral(true), got %T", ins.Values[0])
	}
	if b, ok := ins.Values[1].(*BoolLiteral); !ok || b.Value {
		t.Errorf("value 1: want BoolLiteral(false), got %T", ins.Values[1])
	}
	if _, ok := ins.Values[2].(*NullLiteral); !ok {
		t.Errorf("value 2: want NullLiteral, got %T", ins.Values[2])
	}
}

func TestParseFloatLiteral(t *testing.T) {
	input := `INSERT INTO t (a) VALUES (3.14)`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStatement)
	fl, ok := ins.Values[0].(*FloatLiteral)
	if !ok {
		t.Fatalf("value 0: want FloatLiteral, got %T", ins.Values[0])
	}
	if fl.Value != 3.14 {
		t.Errorf("float value = %f, want 3.14", fl.Value)
	}
}

func TestParseNegativeNumber(t *testing.T) {
	input := `INSERT INTO t (a) VALUES (-42)`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStatement)
	il, ok := ins.Values[0].(*IntegerLiteral)
	if !ok {
		t.Fatalf("value 0: want IntegerLiteral, got %T", ins.Values[0])
	}
	if il.Value != -42 {
		t.Errorf("int value = %d, want -42", il.Value)
	}
}

func TestParseAllDataTypes(t *testing.T) {
	types := []string{
		"text", "varchar", "int", "bigint", "float", "double",
		"boolean", "timestamp", "uuid", "timeuuid", "blob", "inet", "counter",
	}
	for _, dt := range types {
		input := "CREATE TABLE t (c " + dt + ", PRIMARY KEY (c))"
		stmt, err := Parse(input)
		if err != nil {
			t.Errorf("type %s: %v", dt, err)
			continue
		}
		ct := stmt.(*CreateTableStatement)
		if ct.Columns[0].DataType.Name != dt {
			t.Errorf("type %s: got %q", dt, ct.Columns[0].DataType.Name)
		}
	}
}

func TestParseAllOperators(t *testing.T) {
	ops := []string{"=", "<", ">", "<=", ">=", "!="}
	for _, op := range ops {
		input := "SELECT * FROM t WHERE c " + op + " 1"
		stmt, err := Parse(input)
		if err != nil {
			t.Errorf("operator %s: %v", op, err)
			continue
		}
		sel := stmt.(*SelectStatement)
		if sel.Where[0].Operator != op {
			t.Errorf("operator: got %q, want %q", sel.Where[0].Operator, op)
		}
	}
}

func TestParseComments(t *testing.T) {
	input := `-- This is a comment
  SELECT * FROM t -- trailing comment`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stmt.(*SelectStatement); !ok {
		t.Fatalf("expected *SelectStatement, got %T", stmt)
	}
}

func TestParseEscapedString(t *testing.T) {
	input := `INSERT INTO t (a) VALUES ('it''s a test')`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStatement)
	sl := ins.Values[0].(*StringLiteral)
	if sl.Value != "it's a test" {
		t.Errorf("string value = %q, want %q", sl.Value, "it's a test")
	}
}

func TestParseCreateTableClusteringKeys(t *testing.T) {
	input := `CREATE TABLE t (
    pk text,
    ck1 int,
    ck2 int,
    v text,
    PRIMARY KEY (pk, ck1, ck2)
  )`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	ct := stmt.(*CreateTableStatement)
	if len(ct.PrimaryKey.PartitionKeys) != 1 ||
		ct.PrimaryKey.PartitionKeys[0] != "pk" {
		t.Errorf("partition keys = %v, want [pk]", ct.PrimaryKey.PartitionKeys)
	}
	if len(ct.PrimaryKey.ClusteringKeys) != 2 {
		t.Fatalf("clustering keys = %v, want [ck1, ck2]", ct.PrimaryKey.ClusteringKeys)
	}
	if ct.PrimaryKey.ClusteringKeys[0] != "ck1" ||
		ct.PrimaryKey.ClusteringKeys[1] != "ck2" {
		t.Errorf("clustering keys = %v", ct.PrimaryKey.ClusteringKeys)
	}
}

func TestParseSelectDistinct(t *testing.T) {
	input := `SELECT DISTINCT pk FROM users`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStatement)
	if !sel.Distinct {
		t.Error("Distinct should be true")
	}
	if len(sel.Columns) != 1 || sel.Columns[0].Column != "pk" {
		t.Errorf("columns = %v, want [pk]", sel.Columns)
	}
}

func TestParseSelectOrderBy(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantCol string
		wantDir bool // true = DESC
	}{
		{
			name:    "implicit ASC",
			input:   "SELECT * FROM t WHERE pk = 1 ORDER BY ck",
			wantCol: "ck",
			wantDir: false,
		},
		{
			name:    "explicit ASC",
			input:   "SELECT * FROM t WHERE pk = 1 ORDER BY ck ASC",
			wantCol: "ck",
			wantDir: false,
		},
		{
			name:    "explicit DESC",
			input:   "SELECT * FROM t WHERE pk = 1 ORDER BY ck DESC",
			wantCol: "ck",
			wantDir: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := Parse(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			sel := stmt.(*SelectStatement)
			if len(sel.OrderBy) != 1 {
				t.Fatalf("orderBy count = %d, want 1", len(sel.OrderBy))
			}
			if sel.OrderBy[0].Column != tt.wantCol {
				t.Errorf("orderBy column = %q, want %q", sel.OrderBy[0].Column, tt.wantCol)
			}
			if sel.OrderBy[0].Desc != tt.wantDir {
				t.Errorf("orderBy desc = %v, want %v", sel.OrderBy[0].Desc, tt.wantDir)
			}
		})
	}
}

func TestParseSelectOrderByMultiple(t *testing.T) {
	input := `SELECT * FROM t ORDER BY ck1 ASC, ck2 DESC`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStatement)
	if len(sel.OrderBy) != 2 {
		t.Fatalf("orderBy count = %d, want 2", len(sel.OrderBy))
	}
	if sel.OrderBy[0].Column != "ck1" || sel.OrderBy[0].Desc {
		t.Errorf("orderBy[0] = %+v, want ck1 ASC", sel.OrderBy[0])
	}
	if sel.OrderBy[1].Column != "ck2" || !sel.OrderBy[1].Desc {
		t.Errorf("orderBy[1] = %+v, want ck2 DESC", sel.OrderBy[1])
	}
}

func TestParseSelectWhereIn(t *testing.T) {
	input := `SELECT * FROM t WHERE pk IN (1, 2, 3)`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStatement)
	if len(sel.Where) != 1 {
		t.Fatalf("where count = %d, want 1", len(sel.Where))
	}
	w := sel.Where[0]
	if w.Column != "pk" || w.Operator != "IN" {
		t.Errorf("where = %+v, want pk IN", w)
	}
	tuple, ok := w.Value.(*TupleLiteral)
	if !ok {
		t.Fatalf("value: want *TupleLiteral, got %T", w.Value)
	}
	if len(tuple.Values) != 3 {
		t.Errorf("tuple values count = %d, want 3", len(tuple.Values))
	}
}

func TestParseSelectAllowFiltering(t *testing.T) {
	input := `SELECT * FROM t WHERE a = 1 ALLOW FILTERING`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStatement)
	if sel.Table != "t" {
		t.Errorf("table = %q, want %q", sel.Table, "t")
	}
	if len(sel.Where) != 1 {
		t.Errorf("where count = %d, want 1", len(sel.Where))
	}
}

func TestParseUpdate(t *testing.T) {
	input := `UPDATE users SET name = 'bob' WHERE id = 1`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	upd, ok := stmt.(*UpdateStatement)
	if !ok {
		t.Fatalf("expected *UpdateStatement, got %T", stmt)
	}
	if upd.Table != "users" {
		t.Errorf("table = %q, want %q", upd.Table, "users")
	}
	if len(upd.Assignments) != 1 {
		t.Fatalf("assignments count = %d, want 1", len(upd.Assignments))
	}
	if upd.Assignments[0].Column != "name" {
		t.Errorf("assignment column = %q, want %q", upd.Assignments[0].Column, "name")
	}
	if len(upd.Where) != 1 || upd.Where[0].Column != "id" {
		t.Errorf("where = %+v, want id = 1", upd.Where)
	}
}

func TestParseUpdateIfExists(t *testing.T) {
	input := `UPDATE t SET val = 42 WHERE pk = 1 IF EXISTS`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	upd := stmt.(*UpdateStatement)
	if !upd.IfExists {
		t.Error("IfExists should be true")
	}
}

func TestParseUpdateMultipleAssignments(t *testing.T) {
	input := `UPDATE t SET name = 'alice', val = 100 WHERE id = 1`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	upd := stmt.(*UpdateStatement)
	if len(upd.Assignments) != 2 {
		t.Fatalf("assignments count = %d, want 2", len(upd.Assignments))
	}
	if upd.Assignments[0].Column != "name" || upd.Assignments[1].Column != "val" {
		t.Errorf("assignments = %v", upd.Assignments)
	}
}

func TestParseDelete(t *testing.T) {
	input := `DELETE FROM users WHERE id = 1`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	del, ok := stmt.(*DeleteStatement)
	if !ok {
		t.Fatalf("expected *DeleteStatement, got %T", stmt)
	}
	if del.Table != "users" {
		t.Errorf("table = %q, want %q", del.Table, "users")
	}
	if len(del.Where) != 1 || del.Where[0].Column != "id" {
		t.Errorf("where = %+v, want id = 1", del.Where)
	}
}

func TestParseDeleteIfExists(t *testing.T) {
	input := `DELETE FROM t WHERE pk = 1 IF EXISTS`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	del := stmt.(*DeleteStatement)
	if !del.IfExists {
		t.Error("IfExists should be true")
	}
}

func TestParseDeleteColumns(t *testing.T) {
	input := `DELETE name, val FROM t WHERE pk = 1`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	del := stmt.(*DeleteStatement)
	if len(del.Columns) != 2 || del.Columns[0] != "name" || del.Columns[1] != "val" {
		t.Errorf("columns = %v, want [name val]", del.Columns)
	}
	if del.Table != "t" {
		t.Errorf("table = %q, want %q", del.Table, "t")
	}
	if len(del.Where) != 1 || del.Where[0].Column != "pk" {
		t.Errorf("where = %+v, want pk = 1", del.Where)
	}
}

func TestParseSelectFullSyntax(t *testing.T) {
	// SELECT with all optional clauses combined.
	input := `SELECT DISTINCT pk FROM t WHERE pk IN (1, 2) ORDER BY pk DESC LIMIT 10 ALLOW FILTERING`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStatement)
	if !sel.Distinct {
		t.Error("Distinct should be true")
	}
	if len(sel.Where) != 1 || sel.Where[0].Operator != "IN" {
		t.Errorf("where = %+v, want IN", sel.Where)
	}
	if len(sel.OrderBy) != 1 || !sel.OrderBy[0].Desc {
		t.Errorf("orderBy = %+v, want DESC", sel.OrderBy)
	}
	lim := sel.Limit.(*IntegerLiteral)
	if lim.Value != 10 {
		t.Errorf("limit = %d, want 10", lim.Value)
	}
}

func TestParseUDTLiteral(t *testing.T) {
	input := `INSERT INTO t (id, addr) VALUES (1, {street: '123 Main', city: 'Anytown'})`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStatement)
	if len(ins.Values) != 2 {
		t.Fatalf("got %d values, want 2", len(ins.Values))
	}
	m, ok := ins.Values[1].(*MapExprLiteral)
	if !ok {
		t.Fatalf("expected *MapExprLiteral, got %T", ins.Values[1])
	}
	if len(m.Entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(m.Entries))
	}
	// Keys should be string literals from the identifier names.
	key0 := m.Entries[0].Key.(*StringLiteral)
	if key0.Value != "street" {
		t.Errorf("key[0] = %q, want %q", key0.Value, "street")
	}
	val0 := m.Entries[0].Value.(*StringLiteral)
	if val0.Value != "123 Main" {
		t.Errorf("val[0] = %q, want %q", val0.Value, "123 Main")
	}
	key1 := m.Entries[1].Key.(*StringLiteral)
	if key1.Value != "city" {
		t.Errorf("key[1] = %q, want %q", key1.Value, "city")
	}
}

func TestParseUDTLiteralMixedValues(t *testing.T) {
	// UDT literal with different value types.
	input := `INSERT INTO t (id, data) VALUES (1, {name: 'test', count: 42, active: true})`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	ins := stmt.(*InsertStatement)
	m := ins.Values[1].(*MapExprLiteral)
	if len(m.Entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(m.Entries))
	}
	// Verify the integer value.
	val1 := m.Entries[1].Value.(*IntegerLiteral)
	if val1.Value != 42 {
		t.Errorf("val[1] = %d, want 42", val1.Value)
	}
	// Verify the boolean value.
	val2 := m.Entries[2].Value.(*BoolLiteral)
	if !val2.Value {
		t.Error("val[2] should be true")
	}
}

func TestParseSelectFieldAccess(t *testing.T) {
	input := `SELECT addr.street, addr.city FROM t`
	stmt, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	sel := stmt.(*SelectStatement)
	if len(sel.Columns) != 2 {
		t.Fatalf("got %d columns, want 2", len(sel.Columns))
	}
	fa0, ok := sel.Columns[0].Expr.(*FieldAccessExpr)
	if !ok {
		t.Fatalf("expected *FieldAccessExpr, got %T", sel.Columns[0].Expr)
	}
	if fa0.Column != "addr" || fa0.Field != "street" {
		t.Errorf("field access = %s.%s, want addr.street", fa0.Column, fa0.Field)
	}
	fa1 := sel.Columns[1].Expr.(*FieldAccessExpr)
	if fa1.Column != "addr" || fa1.Field != "city" {
		t.Errorf("field access = %s.%s, want addr.city", fa1.Column, fa1.Field)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && containsAt(s, substr)
}

func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
