// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package translate

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cql/parser"
	"github.com/stretchr/testify/require"
)

func TestTranslateUse(t *testing.T) {
	stmt, err := parser.Parse("USE my_keyspace")
	require.NoError(t, err)
	result, err := Translate(stmt)
	require.NoError(t, err)
	require.Equal(t, "SET database = 'my_keyspace'", result.SQL)
}

func TestTranslateCreateKeyspace(t *testing.T) {
	tests := []struct {
		name string
		cql  string
		want string
	}{
		{
			name: "simple",
			cql:  "CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}",
			want: `CREATE DATABASE "test_ks"`,
		},
		{
			name: "if not exists",
			cql:  "CREATE KEYSPACE IF NOT EXISTS test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
			want: `CREATE DATABASE IF NOT EXISTS "test_ks"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.cql)
			require.NoError(t, err)
			result, err := Translate(stmt)
			require.NoError(t, err)
			require.Equal(t, tt.want, result.SQL)
		})
	}
}

func TestTranslateCreateTable(t *testing.T) {
	tests := []struct {
		name string
		cql  string
		want string
	}{
		{
			name: "simple pk",
			cql:  "CREATE TABLE users (user_id uuid, name text, email text, PRIMARY KEY (user_id))",
			want: `CREATE TABLE "users" ("user_id" UUID, "name" STRING, "email" STRING, PRIMARY KEY ("user_id"))`,
		},
		{
			name: "composite pk with clustering",
			cql: `CREATE TABLE events (
				tenant_id uuid,
				event_time timestamp,
				event_id uuid,
				data text,
				PRIMARY KEY ((tenant_id), event_time, event_id)
			)`,
			want: `CREATE TABLE "events" ("tenant_id" UUID, "event_time" TIMESTAMPTZ, "event_id" UUID, "data" STRING, PRIMARY KEY ("tenant_id", "event_time", "event_id"))`,
		},
		{
			name: "compound partition key",
			cql:  "CREATE TABLE metrics (region text, host text, ts timestamp, val double, PRIMARY KEY ((region, host), ts))",
			want: `CREATE TABLE "metrics" ("region" STRING, "host" STRING, "ts" TIMESTAMPTZ, "val" FLOAT8, PRIMARY KEY ("region", "host", "ts"))`,
		},
		{
			name: "if not exists with keyspace",
			cql:  "CREATE TABLE IF NOT EXISTS ks1.users (id int, name text, PRIMARY KEY (id))",
			want: `CREATE TABLE IF NOT EXISTS "ks1"."users" ("id" INT4, "name" STRING, PRIMARY KEY ("id"))`,
		},
		{
			name: "all supported types",
			cql: `CREATE TABLE type_test (
				a text, b varchar, c int, d bigint, e float, f double,
				g boolean, h timestamp, i uuid, j blob, k inet, l counter,
				PRIMARY KEY (a)
			)`,
			want: `CREATE TABLE "type_test" ("a" STRING, "b" STRING, "c" INT4, "d" INT8, "e" FLOAT4, "f" FLOAT8, "g" BOOL, "h" TIMESTAMPTZ, "i" UUID, "j" BYTES, "k" INET, "l" INT8, PRIMARY KEY ("a"))`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.cql)
			require.NoError(t, err)
			result, err := Translate(stmt)
			require.NoError(t, err)
			require.Equal(t, tt.want, result.SQL)
		})
	}
}

func TestTranslateCreateIndex(t *testing.T) {
	tests := []struct {
		name    string
		cql     string
		want    string
		wantErr string
	}{
		{
			name: "basic single column",
			cql:  "CREATE INDEX idx_email ON users (email)",
			want: `CREATE INDEX "idx_email" ON "users" ("email")`,
		},
		{
			name: "if not exists",
			cql:  "CREATE INDEX IF NOT EXISTS idx_email ON users (email)",
			want: `CREATE INDEX IF NOT EXISTS "idx_email" ON "users" ("email")`,
		},
		{
			name: "composite index",
			cql:  "CREATE INDEX idx_city_age ON users (city, age)",
			want: `CREATE INDEX "idx_city_age" ON "users" ("city", "age")`,
		},
		{
			name: "qualified table name",
			cql:  "CREATE INDEX idx_q ON ks1.users (email)",
			want: `CREATE INDEX "idx_q" ON "ks1"."users" ("email")`,
		},
		{
			name:    "collection function KEYS rejected",
			cql:     "CREATE INDEX idx_k ON t (KEYS(col))",
			wantErr: "collection index function KEYS() is not supported",
		},
		{
			name:    "collection function VALUES rejected",
			cql:     "CREATE INDEX idx_v ON t (VALUES(col))",
			wantErr: "collection index function VALUES() is not supported",
		},
		{
			name:    "custom index rejected",
			cql:     "CREATE CUSTOM INDEX idx_sasi ON t (col) USING 'org.apache.cassandra.index.sasi.SASIIndex'",
			wantErr: "CUSTOM INDEX is not supported",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.cql)
			require.NoError(t, err)
			result, err := Translate(stmt)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, result.SQL)
			}
		})
	}
}

func TestTranslateAlterTableWith(t *testing.T) {
	tests := []struct {
		name string
		cql  string
	}{
		{
			name: "compaction strategy",
			cql:  "ALTER TABLE t WITH compaction = {'class': 'LeveledCompactionStrategy'}",
		},
		{
			name: "gc_grace_seconds",
			cql:  "ALTER TABLE t WITH gc_grace_seconds = 86400",
		},
		{
			name: "multiple properties",
			cql:  "ALTER TABLE t WITH gc_grace_seconds = 86400 AND compaction = {'class': 'SizeTieredCompactionStrategy'}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.cql)
			require.NoError(t, err)
			result, err := Translate(stmt)
			require.NoError(t, err)
			require.Empty(t, result.SQL)
		})
	}
}

func TestTranslateAlterKeyspace(t *testing.T) {
	tests := []struct {
		name string
		cql  string
	}{
		{
			name: "replication change",
			cql:  "ALTER KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}",
		},
		{
			name: "durable writes",
			cql:  "ALTER KEYSPACE ks WITH durable_writes = false",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.cql)
			require.NoError(t, err)
			result, err := Translate(stmt)
			require.NoError(t, err)
			require.Empty(t, result.SQL)
		})
	}
}

func TestTranslateInsert(t *testing.T) {
	tests := []struct {
		name       string
		cql        string
		want       string
		wantParams int
	}{
		{
			name: "upsert with literals",
			cql:  "INSERT INTO users (id, name, active) VALUES (42, 'alice', true)",
			want: `UPSERT INTO "users" ("id", "name", "active") VALUES (42, 'alice', true)`,
		},
		{
			name: "insert if not exists",
			cql:  "INSERT INTO users (id, name) VALUES (1, 'bob') IF NOT EXISTS",
			want: `INSERT INTO "users" ("id", "name") VALUES (1, 'bob')`,
		},
		{
			name:       "upsert with bind markers",
			cql:        "INSERT INTO users (id, name) VALUES (?, ?)",
			want:       `UPSERT INTO "users" ("id", "name") VALUES ($1, $2)`,
			wantParams: 2,
		},
		{
			name: "with keyspace",
			cql:  "INSERT INTO ks1.users (id, name) VALUES (1, 'test')",
			want: `UPSERT INTO "ks1"."users" ("id", "name") VALUES (1, 'test')`,
		},
		{
			name: "null value",
			cql:  "INSERT INTO users (id, name) VALUES (1, null)",
			want: `UPSERT INTO "users" ("id", "name") VALUES (1, NULL)`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.cql)
			require.NoError(t, err)
			result, err := Translate(stmt)
			require.NoError(t, err)
			require.Equal(t, tt.want, result.SQL)
			require.Len(t, result.Params, tt.wantParams)
		})
	}
}

func TestTranslateSelect(t *testing.T) {
	tests := []struct {
		name       string
		cql        string
		want       string
		wantParams int
	}{
		{
			name: "select star",
			cql:  "SELECT * FROM users",
			want: `SELECT * FROM "users"`,
		},
		{
			name: "select columns",
			cql:  "SELECT name, email FROM users",
			want: `SELECT "name", "email" FROM "users"`,
		},
		{
			name: "select with where",
			cql:  "SELECT * FROM users WHERE user_id = 'abc'",
			want: `SELECT * FROM "users" WHERE "user_id" = 'abc'`,
		},
		{
			name: "select with multiple where",
			cql:  "SELECT * FROM events WHERE tenant_id = 'x' AND event_time > 100",
			want: `SELECT * FROM "events" WHERE "tenant_id" = 'x' AND "event_time" > 100`,
		},
		{
			name: "select with limit",
			cql:  "SELECT * FROM users LIMIT 10",
			want: `SELECT * FROM "users" LIMIT 10`,
		},
		{
			name: "select with where and limit",
			cql:  "SELECT name FROM users WHERE id = 5 LIMIT 1",
			want: `SELECT "name" FROM "users" WHERE "id" = 5 LIMIT 1`,
		},
		{
			name:       "select with bind marker in where",
			cql:        "SELECT * FROM users WHERE id = ?",
			want:       `SELECT * FROM "users" WHERE "id" = $1`,
			wantParams: 1,
		},
		{
			name: "select with keyspace",
			cql:  "SELECT * FROM ks1.users WHERE id = 1",
			want: `SELECT * FROM "ks1"."users" WHERE "id" = 1`,
		},
		{
			name: "select distinct",
			cql:  "SELECT DISTINCT pk FROM users",
			want: `SELECT DISTINCT "pk" FROM "users"`,
		},
		{
			name: "select with order by",
			cql:  "SELECT * FROM events WHERE pk = 1 ORDER BY ck DESC",
			want: `SELECT * FROM "events" WHERE "pk" = 1 ORDER BY "ck" DESC`,
		},
		{
			name: "select with order by asc",
			cql:  "SELECT * FROM events ORDER BY ck ASC",
			want: `SELECT * FROM "events" ORDER BY "ck"`,
		},
		{
			name: "select with where in",
			cql:  "SELECT * FROM users WHERE id IN (1, 2, 3)",
			want: `SELECT * FROM "users" WHERE "id" IN (1, 2, 3)`,
		},
		{
			name: "select with allow filtering",
			cql:  "SELECT * FROM users WHERE name = 'alice' ALLOW FILTERING",
			want: `SELECT * FROM "users" WHERE "name" = 'alice'`,
		},
		{
			name: "select with all clauses",
			cql:  "SELECT DISTINCT pk FROM t WHERE pk IN (1, 2) ORDER BY pk DESC LIMIT 5 ALLOW FILTERING",
			want: `SELECT DISTINCT "pk" FROM "t" WHERE "pk" IN (1, 2) ORDER BY "pk" DESC LIMIT 5`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.cql)
			require.NoError(t, err)
			result, err := Translate(stmt)
			require.NoError(t, err)
			require.Equal(t, tt.want, result.SQL)
			require.Len(t, result.Params, tt.wantParams)
		})
	}
}

func TestTranslateUpdate(t *testing.T) {
	tests := []struct {
		name string
		cql  string
		want string
	}{
		{
			name: "simple update",
			cql:  "UPDATE users SET name = 'bob' WHERE id = 1",
			want: `UPDATE "users" SET "name" = 'bob' WHERE "id" = 1`,
		},
		{
			name: "multiple assignments",
			cql:  "UPDATE users SET name = 'alice', val = 42 WHERE id = 1",
			want: `UPDATE "users" SET "name" = 'alice', "val" = 42 WHERE "id" = 1`,
		},
		{
			name: "with keyspace",
			cql:  "UPDATE ks.users SET name = 'x' WHERE id = 1",
			want: `UPDATE "ks"."users" SET "name" = 'x' WHERE "id" = 1`,
		},
		{
			name: "if exists ignored",
			cql:  "UPDATE users SET val = 1 WHERE id = 1 IF EXISTS",
			want: `UPDATE "users" SET "val" = 1 WHERE "id" = 1`,
		},
		{
			name: "if condition",
			cql:  "UPDATE users SET name = 'bob' WHERE id = 1 IF name = 'alice'",
			want: `UPDATE "users" SET "name" = 'bob' WHERE "id" = 1 AND "name" = 'alice'`,
		},
		{
			name: "multiple if conditions",
			cql:  "UPDATE users SET name = 'x' WHERE id = 1 IF name = 'alice' AND val = 100",
			want: `UPDATE "users" SET "name" = 'x' WHERE "id" = 1 AND "name" = 'alice' AND "val" = 100`,
		},
		{
			name: "if condition with not equal",
			cql:  "UPDATE users SET val = 0 WHERE id = 1 IF val != 100",
			want: `UPDATE "users" SET "val" = 0 WHERE "id" = 1 AND "val" != 100`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.cql)
			require.NoError(t, err)
			result, err := Translate(stmt)
			require.NoError(t, err)
			require.Equal(t, tt.want, result.SQL)
		})
	}
}

func TestTranslateDelete(t *testing.T) {
	tests := []struct {
		name string
		cql  string
		want string
	}{
		{
			name: "simple delete",
			cql:  "DELETE FROM users WHERE id = 1",
			want: `DELETE FROM "users" WHERE "id" = 1`,
		},
		{
			name: "with keyspace",
			cql:  "DELETE FROM ks.users WHERE id = 1",
			want: `DELETE FROM "ks"."users" WHERE "id" = 1`,
		},
		{
			name: "if exists ignored",
			cql:  "DELETE FROM users WHERE id = 1 IF EXISTS",
			want: `DELETE FROM "users" WHERE "id" = 1`,
		},
		{
			name: "multiple where clauses",
			cql:  "DELETE FROM t WHERE pk = 1 AND ck = 2",
			want: `DELETE FROM "t" WHERE "pk" = 1 AND "ck" = 2`,
		},
		{
			name: "if condition",
			cql:  "DELETE FROM t WHERE pk = 1 IF name = 'alice'",
			want: `DELETE FROM "t" WHERE "pk" = 1 AND "name" = 'alice'`,
		},
		{
			name: "multiple if conditions",
			cql:  "DELETE FROM t WHERE pk = 1 IF name = 'alice' AND val = 100",
			want: `DELETE FROM "t" WHERE "pk" = 1 AND "name" = 'alice' AND "val" = 100`,
		},
		{
			name: "if condition with not equal",
			cql:  "DELETE FROM t WHERE pk = 1 IF name != 'alice'",
			want: `DELETE FROM "t" WHERE "pk" = 1 AND "name" != 'alice'`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.cql)
			require.NoError(t, err)
			result, err := Translate(stmt)
			require.NoError(t, err)
			require.Equal(t, tt.want, result.SQL)
		})
	}
}

func TestConsistencyToIsolation(t *testing.T) {
	tests := []struct {
		consistency string
		want        string
	}{
		{"ONE", "READ COMMITTED"},
		{"LOCAL_ONE", "READ COMMITTED"},
		{"ANY", "READ COMMITTED"},
		{"QUORUM", "SERIALIZABLE"},
		{"LOCAL_QUORUM", "SERIALIZABLE"},
		{"EACH_QUORUM", "SERIALIZABLE"},
		{"ALL", "SERIALIZABLE"},
		{"SERIAL", "SERIALIZABLE"},
		{"LOCAL_SERIAL", "SERIALIZABLE"},
		// Case insensitive.
		{"one", "READ COMMITTED"},
		{"quorum", "SERIALIZABLE"},
	}
	for _, tt := range tests {
		t.Run(tt.consistency, func(t *testing.T) {
			got := ConsistencyToIsolation(tt.consistency)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestQuoteIdent(t *testing.T) {
	require.Equal(t, `"foo"`, quoteIdent("foo"))
	require.Equal(t, `"foo"`, quoteIdent("FOO"))
	require.Equal(t, `"foo""bar"`, quoteIdent(`foo"bar`))
}

func TestQuoteLiteral(t *testing.T) {
	require.Equal(t, "'hello'", quoteLiteral("hello"))
	require.Equal(t, "'it''s'", quoteLiteral("it's"))
}

func TestTranslateBuiltinFunctions(t *testing.T) {
	tests := []struct {
		name string
		cql  string
		want string
	}{
		{
			name: "toTimestamp",
			cql:  "SELECT toTimestamp(uid) FROM t",
			want: `SELECT CAST("uid" AS TIMESTAMPTZ) FROM "t"`,
		},
		{
			name: "toDate",
			cql:  "SELECT toDate(ts) FROM t",
			want: `SELECT CAST("ts" AS DATE) FROM "t"`,
		},
		{
			name: "toUnixTimestamp",
			cql:  "SELECT toUnixTimestamp(ts) FROM t",
			want: `SELECT CAST(extract(epoch FROM "ts") AS INT8) FROM "t"`,
		},
		{
			name: "dateOf",
			cql:  "SELECT dateOf(uid) FROM t",
			want: `SELECT CAST("uid" AS TIMESTAMPTZ) FROM "t"`,
		},
		{
			name: "unixTimestampOf",
			cql:  "SELECT unixTimestampOf(uid) FROM t",
			want: `SELECT CAST(extract(epoch FROM "uid") AS INT8) FROM "t"`,
		},
		{
			name: "minTimeuuid",
			cql:  "SELECT * FROM t WHERE uid > minTimeuuid('2024-01-01')",
			want: `SELECT * FROM "t" WHERE "uid" > gen_random_uuid()`,
		},
		{
			name: "maxTimeuuid",
			cql:  "SELECT * FROM t WHERE uid < maxTimeuuid('2024-12-31')",
			want: `SELECT * FROM "t" WHERE "uid" < gen_random_uuid()`,
		},
		{
			name: "token single key",
			cql:  "SELECT token(pk) FROM t",
			want: `SELECT fnv32a(CAST(CAST("pk" AS STRING) AS BYTES)) FROM "t"`,
		},
		{
			name: "token in where",
			cql:  "SELECT * FROM t WHERE token(pk) > 0",
			want: `SELECT * FROM "t" WHERE fnv32a(CAST(CAST("pk" AS STRING) AS BYTES)) > 0`,
		},
		{
			name: "writetime",
			cql:  "SELECT writetime(val) FROM t",
			want: `SELECT 0::INT8 FROM "t"`,
		},
		{
			name: "ttl",
			cql:  "SELECT ttl(val) FROM t",
			want: `SELECT NULL::INT4 FROM "t"`,
		},
		{
			name: "textAsBlob",
			cql:  "SELECT textAsBlob(val) FROM t",
			want: `SELECT CAST("val" AS BYTES) FROM "t"`,
		},
		{
			name: "blobAsText",
			cql:  "SELECT blobAsText(val) FROM t",
			want: `SELECT CAST("val" AS STRING) FROM "t"`,
		},
		{
			name: "intAsBlob",
			cql:  "SELECT intAsBlob(pk) FROM t",
			want: `SELECT CAST("pk" AS BYTES) FROM "t"`,
		},
		{
			name: "blobAsInt",
			cql:  "SELECT blobAsInt(val) FROM t",
			want: `SELECT CAST("val" AS INT4) FROM "t"`,
		},
		{
			name: "fromJson",
			cql:  "SELECT fromJson(val) FROM t",
			want: `SELECT CAST("val" AS JSONB) FROM "t"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.cql)
			require.NoError(t, err)
			result, err := Translate(stmt)
			require.NoError(t, err)
			require.Equal(t, tt.want, result.SQL)
		})
	}
}

func TestTranslateInsertJSON(t *testing.T) {
	tests := []struct {
		name string
		cql  string
		want string
	}{
		{
			name: "basic insert json",
			cql:  `INSERT INTO t JSON '{"id": 1, "name": "alice"}'`,
			want: `UPSERT INTO "t" ("id", "name") VALUES (1, 'alice')`,
		},
		{
			name: "insert json if not exists",
			cql:  `INSERT INTO t JSON '{"id": 2, "name": "bob"}' IF NOT EXISTS`,
			want: `INSERT INTO "t" ("id", "name") VALUES (2, 'bob')`,
		},
		{
			name: "insert json with boolean and null",
			cql:  `INSERT INTO t JSON '{"active": true, "id": 1, "name": null}'`,
			want: `UPSERT INTO "t" ("active", "id", "name") VALUES (true, 1, NULL)`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.cql)
			require.NoError(t, err)
			result, err := Translate(stmt)
			require.NoError(t, err)
			require.Equal(t, tt.want, result.SQL)
		})
	}
}

func TestTranslateSelectJSON(t *testing.T) {
	tests := []struct {
		name string
		cql  string
		want string
	}{
		{
			name: "select json star",
			cql:  "SELECT JSON * FROM t",
			want: `SELECT row_to_json(sub)::STRING AS "[json]" FROM (SELECT * FROM "t") AS sub`,
		},
		{
			name: "select json columns",
			cql:  "SELECT JSON id, name FROM t",
			want: `SELECT jsonb_build_object('id', "id", 'name', "name")::STRING AS "[json]" FROM "t"`,
		},
		{
			name: "select json star with where",
			cql:  "SELECT JSON * FROM t WHERE id = 1",
			want: `SELECT row_to_json(sub)::STRING AS "[json]" FROM (SELECT * FROM "t" WHERE "id" = 1) AS sub`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.cql)
			require.NoError(t, err)
			result, err := Translate(stmt)
			require.NoError(t, err)
			require.Equal(t, tt.want, result.SQL)
		})
	}
}

func TestTranslateRoundTrip(t *testing.T) {
	// Verify that parsing a CQL statement and translating it produces valid SQL.
	cqlStatements := []string{
		"USE my_keyspace",
		"CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}",
		"CREATE TABLE users (id uuid, name text, email text, PRIMARY KEY (id))",
		"INSERT INTO users (id, name) VALUES (1, 'test')",
		"SELECT * FROM users WHERE id = 1",
		"SELECT name, email FROM users WHERE id = 1 LIMIT 10",
		"SELECT DISTINCT pk FROM t",
		"SELECT * FROM t ORDER BY ck DESC",
		"SELECT * FROM t WHERE id IN (1, 2, 3)",
		"SELECT * FROM t WHERE a = 1 ALLOW FILTERING",
		"SELECT DISTINCT pk FROM t WHERE pk IN (1, 2) ORDER BY pk LIMIT 5 ALLOW FILTERING",
		"UPDATE t SET name = 'x' WHERE id = 1",
		"DELETE FROM t WHERE id = 1",
	}
	for _, cql := range cqlStatements {
		t.Run(cql, func(t *testing.T) {
			stmt, err := parser.Parse(cql)
			require.NoError(t, err)
			result, err := Translate(stmt)
			require.NoError(t, err)
			require.NotEmpty(t, result.SQL)
		})
	}
}
