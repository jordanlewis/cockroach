// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package smoketest

// This file implements a comprehensive TDS driver smoke test that simulates
// the connection sequence, query patterns, and metadata operations of the
// jTDS JDBC driver connecting to CockroachDB's TDS frontend. Since jTDS
// is a Java library and the CockroachDB build environment may lack a JDK,
// this test uses Go's raw TDS wire protocol to replicate jTDS's exact
// wire behavior against a real CockroachDB server.
//
// Test progression follows the bead specification (hq-dpxy):
//   1. Basic connectivity (PRELOGIN, LOGIN7, version negotiation)
//   2. Simple queries (SELECT 1, @@VERSION, GETDATE())
//   3. DDL with Sybase types (MONEY, SMALLDATETIME, IMAGE, TEXT, etc.)
//   4. DML (INSERT, UPDATE, DELETE with various types)
//   5. Parameterized queries (sp_executesql)
//   6. Transactions (BEGIN TRAN / COMMIT / ROLLBACK)
//   7. Metadata queries (sp_tables, sp_columns, DatabaseMetaData patterns)
//   8. Cursor operations (sp_cursor*)
//   9. Batch operations (multiple statements)
//  10. Error handling (intentional errors, driver reaction)

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/tds/tdswire"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// finding records a compatibility issue discovered during the smoke test.
type finding struct {
	area      string // test area (e.g. "DDL", "DML", "metadata")
	attempted string // what was attempted
	err       string // error message or wrong result
	category  string // root cause category
}

// TestJTDSSmokeTest runs the comprehensive jTDS JDBC driver compatibility
// smoke test against a real CockroachDB server with TDS frontend.
func TestJTDSSmokeTest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	addr, cleanup := startTDSTestServer(t, ctx)
	defer cleanup()

	var findings []finding

	// Connect and authenticate.
	tc := dialTDSConn(t, addr)
	defer func() { _ = tc.Close() }()

	// ---------------------------------------------------------------
	// 1. Basic connectivity
	// ---------------------------------------------------------------
	t.Run("1_BasicConnectivity", func(t *testing.T) {
		// PRELOGIN with jTDS-style options.
		preLoginResp := doGoMSSQLPreLogin(t, tc)
		require.NotNil(t, preLoginResp)

		// Verify server returns VERSION and ENCRYPTION options.
		var hasVersion, hasEncryption bool
		for _, opt := range preLoginResp.Options {
			if opt.Token == tdswire.PreLoginVersion {
				hasVersion = true
				v, err := tdswire.DecodeVersionData(opt.Data)
				require.NoError(t, err)
				t.Logf("Server TDS version: %d.%d.%d.%d", v.Major, v.Minor, v.Build, v.SubBuild)
			}
			if opt.Token == tdswire.PreLoginEncryption {
				hasEncryption = true
			}
		}
		require.True(t, hasVersion, "server should return VERSION in PRELOGIN")
		require.True(t, hasEncryption, "server should return ENCRYPTION in PRELOGIN")

		// LOGIN7 with jTDS-style parameters.
		loginResp := doGoMSSQLLogin7(t, tc, "root", "", "defaultdb")
		loginResult := parseTokenStream(t, loginResp)
		require.NotNil(t, loginResult.LoginAck, "LOGIN7 should produce LOGINACK")
		require.Equal(t, "CockroachDB", loginResult.LoginAck.ProgName)
		t.Logf("LOGIN7 OK: program=%s", loginResult.LoginAck.ProgName)

		// Verify ENVCHANGE(database).
		var dbChange bool
		for _, ec := range loginResult.EnvChanges {
			if ec.Type == tdswire.EnvDatabase {
				dbChange = true
				require.Equal(t, "defaultdb", ec.NewValue)
			}
		}
		require.True(t, dbChange, "LOGIN7 should include ENVCHANGE(database)")
	})

	// ---------------------------------------------------------------
	// 2. Simple queries
	// ---------------------------------------------------------------
	t.Run("2_SimpleQueries", func(t *testing.T) {
		// SELECT 1
		t.Run("SELECT_1", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "SELECT 1 AS val")
			result := parseTokenStream(t, resp)
			require.Nil(t, result.Error, "SELECT 1 should succeed")
			require.NotNil(t, result.ColMeta)
			require.Len(t, result.ColMeta.Columns, 1)
			require.Len(t, result.Rows, 1)
		})

		// SELECT @@VERSION
		t.Run("SELECT_VERSION", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "SELECT @@VERSION")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "simple-queries",
					attempted: "SELECT @@VERSION",
					err:       result.Error.Message,
					category:  "catalog",
				})
				t.Logf("FINDING: SELECT @@VERSION failed: %s", result.Error.Message)
				return
			}
			require.NotNil(t, result.ColMeta)
			require.Len(t, result.Rows, 1)
			ver := decodeRowString(result.Rows[0].Values[0])
			require.Contains(t, ver, "CockroachDB")
			t.Logf("@@VERSION: %s", ver)
		})

		// SELECT GETDATE()
		t.Run("SELECT_GETDATE", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "SELECT GETDATE() AS now")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "simple-queries",
					attempted: "SELECT GETDATE()",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: SELECT GETDATE() failed: %s", result.Error.Message)
				return
			}
			require.NotNil(t, result.ColMeta)
			require.Len(t, result.Rows, 1)
			t.Logf("GETDATE(): %s", decodeRowString(result.Rows[0].Values[0]))
		})

		// SELECT @@SERVERNAME
		t.Run("SELECT_SERVERNAME", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "SELECT @@SERVERNAME AS srvname")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "simple-queries",
					attempted: "SELECT @@SERVERNAME",
					err:       result.Error.Message,
					category:  "catalog",
				})
				t.Logf("FINDING: SELECT @@SERVERNAME failed: %s", result.Error.Message)
			} else {
				t.Logf("@@SERVERNAME: %s", decodeRowString(result.Rows[0].Values[0]))
			}
		})

		// SELECT DB_NAME()
		t.Run("SELECT_DB_NAME", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "SELECT DB_NAME() AS dbname")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "simple-queries",
					attempted: "SELECT DB_NAME()",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: SELECT DB_NAME() failed: %s", result.Error.Message)
			} else {
				require.Len(t, result.Rows, 1)
				t.Logf("DB_NAME(): %s", decodeRowString(result.Rows[0].Values[0]))
			}
		})

		// SELECT @@SPID (jTDS uses this to get connection ID)
		t.Run("SELECT_SPID", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "SELECT @@SPID AS spid")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "simple-queries",
					attempted: "SELECT @@SPID",
					err:       result.Error.Message,
					category:  "catalog",
				})
				t.Logf("FINDING: SELECT @@SPID failed: %s", result.Error.Message)
			} else {
				t.Logf("@@SPID: %s", decodeRowString(result.Rows[0].Values[0]))
			}
		})
	})

	// ---------------------------------------------------------------
	// 3. DDL with Sybase types
	// ---------------------------------------------------------------
	t.Run("3_DDL_SybaseTypes", func(t *testing.T) {
		// Drop table if exists (cleanup from previous runs).
		sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS jtds_types_test")

		// CREATE TABLE with Sybase/SQL Server types.
		ddl := `CREATE TABLE jtds_types_test (
			id INT NOT NULL,
			name VARCHAR(100),
			amount MONEY,
			small_dt SMALLDATETIME,
			big_dt DATETIME,
			img IMAGE,
			txt TEXT,
			ntext_col NTEXT,
			num_col NUMERIC(18,4),
			dec_col DECIMAL(10,2),
			small_money SMALLMONEY,
			tinyint_col TINYINT,
			smallint_col SMALLINT,
			bigint_col BIGINT,
			bit_col BIT,
			real_col REAL,
			float_col FLOAT,
			varbinary_col VARBINARY(200),
			char_col CHAR(10),
			nchar_col NCHAR(10),
			nvarchar_col NVARCHAR(100)
		)`
		resp := sendBatch(t, tc.pr, tc.pw, ddl)
		result := parseTokenStream(t, resp)
		if result.Error != nil {
			findings = append(findings, finding{
				area:      "DDL",
				attempted: "CREATE TABLE with Sybase types",
				err:       result.Error.Message,
				category:  "parser/type-system",
			})
			t.Logf("FINDING: CREATE TABLE with Sybase types failed: %s", result.Error.Message)

			// Try a simpler table for remaining tests.
			resp = sendBatch(t, tc.pr, tc.pw,
				"CREATE TABLE jtds_types_test (id INT NOT NULL, name VARCHAR(100), amount DECIMAL(19,4))")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				t.Fatalf("Even simple CREATE TABLE failed: %s", result.Error.Message)
			}
		} else {
			t.Log("CREATE TABLE with Sybase types: OK")
		}

		// Test CREATE TABLE with IDENTITY column (jTDS heavily uses this).
		t.Run("IdentityColumn", func(t *testing.T) {
			sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS jtds_identity_test")
			resp := sendBatch(t, tc.pr, tc.pw,
				"CREATE TABLE jtds_identity_test (id INT IDENTITY(1,1) NOT NULL, name VARCHAR(50))")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "DDL",
					attempted: "CREATE TABLE with IDENTITY column",
					err:       result.Error.Message,
					category:  "parser/translator",
				})
				t.Logf("FINDING: IDENTITY column failed: %s", result.Error.Message)
			} else {
				t.Log("CREATE TABLE with IDENTITY: OK")
			}
		})

		// Test ALTER TABLE ADD COLUMN.
		t.Run("AlterTable", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"ALTER TABLE jtds_types_test ADD description VARCHAR(500)")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "DDL",
					attempted: "ALTER TABLE ADD COLUMN",
					err:       result.Error.Message,
					category:  "parser/translator",
				})
				t.Logf("FINDING: ALTER TABLE failed: %s", result.Error.Message)
			} else {
				t.Log("ALTER TABLE ADD COLUMN: OK")
			}
		})

		// Test CREATE INDEX.
		t.Run("CreateIndex", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"CREATE INDEX idx_jtds_name ON jtds_types_test (name)")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "DDL",
					attempted: "CREATE INDEX",
					err:       result.Error.Message,
					category:  "parser/translator",
				})
				t.Logf("FINDING: CREATE INDEX failed: %s", result.Error.Message)
			} else {
				t.Log("CREATE INDEX: OK")
			}
		})
	})

	// ---------------------------------------------------------------
	// 4. DML (INSERT/UPDATE/DELETE)
	// ---------------------------------------------------------------
	t.Run("4_DML", func(t *testing.T) {
		// INSERT with values.
		t.Run("Insert", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO jtds_types_test (id, name) VALUES (1, 'Alice')")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "DML",
					attempted: "INSERT INTO with values",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: INSERT failed: %s", result.Error.Message)
			} else {
				t.Log("INSERT: OK")
			}
		})

		// INSERT multiple rows.
		t.Run("InsertMultiple", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO jtds_types_test (id, name) VALUES (2, 'Bob')")
			result := parseTokenStream(t, resp)
			require.Nil(t, result.Error, "INSERT should succeed")

			resp = sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO jtds_types_test (id, name) VALUES (3, 'Charlie')")
			result = parseTokenStream(t, resp)
			require.Nil(t, result.Error, "INSERT should succeed")
		})

		// SELECT to verify inserts.
		t.Run("SelectVerify", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT id, name FROM jtds_types_test ORDER BY id")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				t.Fatalf("SELECT failed: %s", result.Error.Message)
			}
			require.Len(t, result.Rows, 3, "expected 3 rows after inserts")
			t.Logf("SELECT after INSERT: %d rows", len(result.Rows))
		})

		// UPDATE.
		t.Run("Update", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"UPDATE jtds_types_test SET name = 'Alice Updated' WHERE id = 1")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "DML",
					attempted: "UPDATE",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: UPDATE failed: %s", result.Error.Message)
			} else {
				t.Log("UPDATE: OK")
			}
		})

		// DELETE.
		t.Run("Delete", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"DELETE FROM jtds_types_test WHERE id = 3")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "DML",
					attempted: "DELETE",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: DELETE failed: %s", result.Error.Message)
			} else {
				t.Log("DELETE: OK")
			}
		})

		// Verify remaining rows.
		t.Run("SelectAfterDML", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT COUNT(*) AS cnt FROM jtds_types_test")
			result := parseTokenStream(t, resp)
			require.Nil(t, result.Error)
			require.Len(t, result.Rows, 1)
		})

		// INSERT with IDENTITY column and @@IDENTITY retrieval.
		t.Run("InsertIdentity", func(t *testing.T) {
			// First check if identity table was created.
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'jtds_identity_test'")
			result := parseTokenStream(t, resp)
			if result.Error != nil || len(result.Rows) == 0 {
				t.Skip("jtds_identity_test not created (IDENTITY DDL failed)")
			}

			resp = sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO jtds_identity_test (name) VALUES ('test_identity')")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "DML",
					attempted: "INSERT into IDENTITY table",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: INSERT into IDENTITY table failed: %s", result.Error.Message)
				return
			}

			// jTDS retrieves identity via SELECT @@IDENTITY or SCOPE_IDENTITY().
			resp = sendBatch(t, tc.pr, tc.pw, "SELECT @@IDENTITY AS last_id")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "DML",
					attempted: "SELECT @@IDENTITY after INSERT",
					err:       result.Error.Message,
					category:  "catalog",
				})
				t.Logf("FINDING: @@IDENTITY failed: %s", result.Error.Message)
			} else {
				t.Logf("@@IDENTITY: %s", decodeRowString(result.Rows[0].Values[0]))
			}
		})
	})

	// ---------------------------------------------------------------
	// 5. Parameterized queries (sp_executesql)
	// ---------------------------------------------------------------
	t.Run("5_ParameterizedQueries", func(t *testing.T) {
		// jTDS uses sp_executesql for prepared statements.
		t.Run("sp_executesql_simple", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"EXEC sp_executesql N'SELECT id, name FROM jtds_types_test WHERE id = 1'")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "parameterized",
					attempted: "EXEC sp_executesql (simple)",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: sp_executesql simple failed: %s", result.Error.Message)
			} else {
				require.NotNil(t, result.ColMeta)
				t.Logf("sp_executesql simple: %d rows", len(result.Rows))
			}
		})

		// sp_executesql with parameters.
		t.Run("sp_executesql_params", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"EXEC sp_executesql N'SELECT id, name FROM jtds_types_test WHERE id = @p1', N'@p1 INT', @p1 = 1")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "parameterized",
					attempted: "EXEC sp_executesql with parameters",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: sp_executesql with params failed: %s", result.Error.Message)
			} else {
				t.Logf("sp_executesql with params: %d rows", len(result.Rows))
			}
		})

		// sp_executesql with multiple parameters.
		t.Run("sp_executesql_multi_params", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"EXEC sp_executesql N'SELECT id, name FROM jtds_types_test WHERE id >= @p1 AND id <= @p2', "+
					"N'@p1 INT, @p2 INT', @p1 = 1, @p2 = 10")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "parameterized",
					attempted: "EXEC sp_executesql with multiple parameters",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: sp_executesql multi-params failed: %s", result.Error.Message)
			} else {
				t.Logf("sp_executesql multi-params: %d rows", len(result.Rows))
			}
		})
	})

	// ---------------------------------------------------------------
	// 6. Transactions
	// ---------------------------------------------------------------
	t.Run("6_Transactions", func(t *testing.T) {
		// BEGIN TRAN / COMMIT.
		t.Run("BeginCommit", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "BEGIN TRAN")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "transactions",
					attempted: "BEGIN TRAN",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: BEGIN TRAN failed: %s", result.Error.Message)
				return
			}

			resp = sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO jtds_types_test (id, name) VALUES (10, 'TxnTest')")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				t.Logf("INSERT in txn failed: %s", result.Error.Message)
				sendBatch(t, tc.pr, tc.pw, "ROLLBACK")
				return
			}

			resp = sendBatch(t, tc.pr, tc.pw, "COMMIT")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "transactions",
					attempted: "COMMIT",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: COMMIT failed: %s", result.Error.Message)
			} else {
				t.Log("BEGIN/COMMIT: OK")
			}
		})

		// BEGIN TRAN / ROLLBACK.
		t.Run("BeginRollback", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "BEGIN TRAN")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				t.Skipf("BEGIN TRAN failed: %s", result.Error.Message)
			}

			resp = sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO jtds_types_test (id, name) VALUES (99, 'RollbackTest')")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				t.Logf("INSERT in txn failed: %s", result.Error.Message)
			}

			resp = sendBatch(t, tc.pr, tc.pw, "ROLLBACK")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "transactions",
					attempted: "ROLLBACK",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: ROLLBACK failed: %s", result.Error.Message)
			} else {
				t.Log("BEGIN/ROLLBACK: OK")
			}

			// Verify rollback — row should not exist.
			resp = sendBatch(t, tc.pr, tc.pw,
				"SELECT COUNT(*) AS cnt FROM jtds_types_test WHERE id = 99")
			result = parseTokenStream(t, resp)
			if result.Error == nil && len(result.Rows) > 0 {
				// COUNT(*) is returned as binary INT4/INT8 on the wire.
				// Decode as integer rather than string.
				raw := result.Rows[0].Values[0]
				count := decodeIntValue(raw)
				if count != 0 {
					findings = append(findings, finding{
						area:      "transactions",
						attempted: "ROLLBACK verification",
						err:       fmt.Sprintf("row with id=99 still exists after ROLLBACK (count=%d)", count),
						category:  "executor",
					})
					t.Logf("FINDING: ROLLBACK did not remove row (count=%d)", count)
				} else {
					t.Log("ROLLBACK verified: row correctly absent")
				}
			}
		})

		// @@TRANCOUNT.
		t.Run("TranCount", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "SELECT @@TRANCOUNT AS tc")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "transactions",
					attempted: "SELECT @@TRANCOUNT",
					err:       result.Error.Message,
					category:  "catalog",
				})
				t.Logf("FINDING: @@TRANCOUNT failed: %s", result.Error.Message)
			} else {
				val := decodeRowString(result.Rows[0].Values[0])
				t.Logf("@@TRANCOUNT (outside txn): %s", val)
			}
		})
	})

	// ---------------------------------------------------------------
	// 7. Metadata queries (DatabaseMetaData)
	// ---------------------------------------------------------------
	t.Run("7_MetadataQueries", func(t *testing.T) {
		// sp_tables — jTDS uses this for DatabaseMetaData.getTables().
		t.Run("sp_tables", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "EXEC sp_tables")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "metadata",
					attempted: "EXEC sp_tables",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: sp_tables failed: %s", result.Error.Message)
			} else {
				require.NotNil(t, result.ColMeta)
				t.Logf("sp_tables: %d columns, %d rows",
					len(result.ColMeta.Columns), len(result.Rows))
			}
		})

		// sp_tables with table name filter.
		t.Run("sp_tables_filtered", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"EXEC sp_tables @table_name = 'jtds_types_test'")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "metadata",
					attempted: "EXEC sp_tables with filter",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: sp_tables filtered failed: %s", result.Error.Message)
			} else {
				t.Logf("sp_tables filtered: %d rows", len(result.Rows))
			}
		})

		// sp_columns — jTDS uses this for DatabaseMetaData.getColumns().
		t.Run("sp_columns", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"EXEC sp_columns @table_name = 'jtds_types_test'")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "metadata",
					attempted: "EXEC sp_columns",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: sp_columns failed: %s", result.Error.Message)
			} else {
				require.NotNil(t, result.ColMeta)
				t.Logf("sp_columns: %d columns, %d rows",
					len(result.ColMeta.Columns), len(result.Rows))
			}
		})

		// information_schema queries (jTDS DatabaseMetaData fallbacks).
		t.Run("InfoSchema_Tables", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE "+
					"FROM INFORMATION_SCHEMA.TABLES "+
					"WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "metadata",
					attempted: "INFORMATION_SCHEMA.TABLES query",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: INFORMATION_SCHEMA query failed: %s", result.Error.Message)
			} else {
				t.Logf("INFORMATION_SCHEMA.TABLES: %d rows", len(result.Rows))
			}
		})

		// SELECT @@MAX_PRECISION (jTDS queries this for metadata).
		t.Run("MAX_PRECISION", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "SELECT @@MAX_PRECISION AS mp")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "metadata",
					attempted: "SELECT @@MAX_PRECISION",
					err:       result.Error.Message,
					category:  "catalog",
				})
				t.Logf("FINDING: @@MAX_PRECISION failed: %s", result.Error.Message)
			} else {
				t.Logf("@@MAX_PRECISION: %s", decodeRowString(result.Rows[0].Values[0]))
			}
		})

		// sysobjects query (jTDS uses this for some metadata).
		t.Run("sysobjects", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT name, type FROM sysobjects WHERE type IN ('U', 'V') ORDER BY name")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "metadata",
					attempted: "SELECT FROM sysobjects",
					err:       result.Error.Message,
					category:  "catalog",
				})
				t.Logf("FINDING: sysobjects query failed: %s", result.Error.Message)
			} else {
				t.Logf("sysobjects: %d rows", len(result.Rows))
			}
		})

		// syscolumns query (jTDS alternative metadata path).
		t.Run("syscolumns", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT c.name, c.colid FROM syscolumns c "+
					"INNER JOIN sysobjects o ON c.id = o.id "+
					"WHERE o.name = 'jtds_types_test' ORDER BY c.colid")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "metadata",
					attempted: "syscolumns JOIN sysobjects",
					err:       result.Error.Message,
					category:  "catalog",
				})
				t.Logf("FINDING: syscolumns query failed: %s", result.Error.Message)
			} else {
				t.Logf("syscolumns: %d rows", len(result.Rows))
			}
		})
	})

	// ---------------------------------------------------------------
	// 8. Cursor operations
	// ---------------------------------------------------------------
	t.Run("8_CursorOperations", func(t *testing.T) {
		// jTDS uses DECLARE CURSOR for server-side cursors.
		t.Run("DeclareCursor", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"DECLARE test_cursor CURSOR FOR SELECT id, name FROM jtds_types_test ORDER BY id")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "cursors",
					attempted: "DECLARE CURSOR",
					err:       result.Error.Message,
					category:  "parser",
				})
				t.Logf("FINDING: DECLARE CURSOR failed: %s", result.Error.Message)
				return
			}

			// OPEN CURSOR.
			resp = sendBatch(t, tc.pr, tc.pw, "OPEN test_cursor")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "cursors",
					attempted: "OPEN CURSOR",
					err:       result.Error.Message,
					category:  "parser/executor",
				})
				t.Logf("FINDING: OPEN CURSOR failed: %s", result.Error.Message)
				return
			}

			// FETCH NEXT.
			resp = sendBatch(t, tc.pr, tc.pw,
				"FETCH NEXT FROM test_cursor")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "cursors",
					attempted: "FETCH NEXT FROM CURSOR",
					err:       result.Error.Message,
					category:  "parser/executor",
				})
				t.Logf("FINDING: FETCH NEXT failed: %s", result.Error.Message)
			}

			// CLOSE and DEALLOCATE.
			sendBatch(t, tc.pr, tc.pw, "CLOSE test_cursor")
			sendBatch(t, tc.pr, tc.pw, "DEALLOCATE test_cursor")
		})
	})

	// ---------------------------------------------------------------
	// 9. Batch operations
	// ---------------------------------------------------------------
	t.Run("9_BatchOperations", func(t *testing.T) {
		// Multiple statements in a single batch (separated by semicolons in
		// T-SQL). jTDS sends multi-statement batches.
		t.Run("MultipleSQLBatch", func(t *testing.T) {
			// Send multiple sequential batches on the same connection.
			queries := []string{
				"SELECT 1 AS first_query",
				"SELECT 2 AS second_query",
				"SELECT 'batch_test' AS third_query",
			}
			for _, q := range queries {
				resp := sendBatch(t, tc.pr, tc.pw, q)
				result := parseTokenStream(t, resp)
				if result.Error != nil {
					findings = append(findings, finding{
						area:      "batch",
						attempted: fmt.Sprintf("sequential batch: %s", q),
						err:       result.Error.Message,
						category:  "executor",
					})
					t.Logf("FINDING: batch query failed: %s", result.Error.Message)
				}
			}
			t.Log("Sequential batch queries: OK")
		})

		// jTDS driver SET commands sent on connection init.
		t.Run("jTDS_InitCommands", func(t *testing.T) {
			initCmds := []string{
				"SET QUOTED_IDENTIFIER ON",
				"SET TEXTSIZE 2147483647",
				"SET ANSI_DEFAULTS ON",
				"SET CURSOR_CLOSE_ON_COMMIT OFF",
				"SET IMPLICIT_TRANSACTIONS OFF",
			}
			for _, cmd := range initCmds {
				resp := sendBatch(t, tc.pr, tc.pw, cmd)
				result := parseTokenStream(t, resp)
				if result.Error != nil {
					findings = append(findings, finding{
						area:      "batch",
						attempted: fmt.Sprintf("jTDS init: %s", cmd),
						err:       result.Error.Message,
						category:  "catalog",
					})
					t.Logf("FINDING: jTDS init command failed: %s: %s", cmd, result.Error.Message)
				}
			}
			t.Log("jTDS init commands: OK")
		})
	})

	// ---------------------------------------------------------------
	// 10. Error handling
	// ---------------------------------------------------------------
	t.Run("10_ErrorHandling", func(t *testing.T) {
		// Reference nonexistent table.
		t.Run("NonexistentTable", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT * FROM completely_nonexistent_table_xyz")
			result := parseTokenStream(t, resp)
			if result.Error == nil {
				findings = append(findings, finding{
					area:      "error-handling",
					attempted: "SELECT from nonexistent table",
					err:       "no error returned for nonexistent table",
					category:  "executor",
				})
				t.Log("FINDING: no error for nonexistent table")
			} else {
				t.Logf("Error for nonexistent table: number=%d class=%d msg=%s",
					result.Error.Number, result.Error.Class, result.Error.Message)
				// Verify error has reasonable TDS error fields.
				if result.Error.Number == 0 {
					findings = append(findings, finding{
						area:      "error-handling",
						attempted: "error number for nonexistent table",
						err:       "error number is 0 (should be nonzero like 208)",
						category:  "wire-protocol",
					})
				}
			}
		})

		// Syntax error.
		t.Run("SyntaxError", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECTT INVALID SYNTAX HERE")
			result := parseTokenStream(t, resp)
			if result.Error == nil {
				findings = append(findings, finding{
					area:      "error-handling",
					attempted: "intentional syntax error",
					err:       "no error returned for syntax error",
					category:  "parser",
				})
				t.Log("FINDING: no error for syntax error")
			} else {
				t.Logf("Syntax error: number=%d class=%d msg=%s",
					result.Error.Number, result.Error.Class, result.Error.Message)
			}
		})

		// Division by zero.
		t.Run("DivisionByZero", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "SELECT 1/0 AS divzero")
			result := parseTokenStream(t, resp)
			if result.Error == nil {
				// Some SQL servers return NULL for 1/0 with ANSI_WARNINGS OFF.
				// Check if we got a result.
				if result.ColMeta != nil && len(result.Rows) > 0 {
					val := decodeRowString(result.Rows[0].Values[0])
					t.Logf("1/0 returned: %s (no error)", val)
				} else {
					findings = append(findings, finding{
						area:      "error-handling",
						attempted: "SELECT 1/0",
						err:       "no error or result for division by zero",
						category:  "executor",
					})
				}
			} else {
				t.Logf("Division by zero error: %s", result.Error.Message)
			}
		})

		// Constraint violation.
		t.Run("DuplicateKey", func(t *testing.T) {
			// First ensure the row exists.
			sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO jtds_types_test (id, name) VALUES (999, 'unique_test')")
			// Try to insert duplicate.
			resp := sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO jtds_types_test (id, name) VALUES (999, 'duplicate')")
			result := parseTokenStream(t, resp)
			// Note: without a UNIQUE constraint on id, this may succeed.
			// We're testing the error path behavior.
			if result.Error != nil {
				t.Logf("Duplicate insert error: %s", result.Error.Message)
			} else {
				t.Log("Duplicate insert succeeded (no unique constraint)")
			}
		})

		// Connection survives errors.
		t.Run("ConnectionSurvives", func(t *testing.T) {
			// After errors, the connection should still be usable.
			resp := sendBatch(t, tc.pr, tc.pw, "SELECT 'still alive' AS status")
			result := parseTokenStream(t, resp)
			require.Nil(t, result.Error,
				"connection should be usable after errors")
			require.Len(t, result.Rows, 1)
			val := decodeRowString(result.Rows[0].Values[0])
			require.Equal(t, "still alive", val)
			t.Log("Connection survived all error tests: OK")
		})
	})

	// ---------------------------------------------------------------
	// Additional: jTDS-specific patterns
	// ---------------------------------------------------------------
	t.Run("11_jTDS_Patterns", func(t *testing.T) {
		// jTDS sends SET ROWCOUNT on some operations.
		t.Run("SET_ROWCOUNT", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "SET ROWCOUNT 0")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "jtds-patterns",
					attempted: "SET ROWCOUNT 0",
					err:       result.Error.Message,
					category:  "catalog",
				})
				t.Logf("FINDING: SET ROWCOUNT failed: %s", result.Error.Message)
			}
		})

		// jTDS queries @@ROWCOUNT after DML.
		t.Run("ROWCOUNT_after_DML", func(t *testing.T) {
			sendBatch(t, tc.pr, tc.pw,
				"UPDATE jtds_types_test SET name = name WHERE id = 1")
			resp := sendBatch(t, tc.pr, tc.pw, "SELECT @@ROWCOUNT AS rc")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "jtds-patterns",
					attempted: "SELECT @@ROWCOUNT after DML",
					err:       result.Error.Message,
					category:  "catalog",
				})
				t.Logf("FINDING: @@ROWCOUNT failed: %s", result.Error.Message)
			} else if len(result.Rows) > 0 {
				t.Logf("@@ROWCOUNT after UPDATE: %s", decodeRowString(result.Rows[0].Values[0]))
			}
		})

		// String concatenation with + (Sybase/SQL Server style).
		t.Run("StringConcat", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT 'Hello' + ' ' + 'World' AS greeting")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "jtds-patterns",
					attempted: "string concatenation with +",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: string concatenation failed: %s", result.Error.Message)
			} else {
				val := decodeRowString(result.Rows[0].Values[0])
				if val != "Hello World" {
					findings = append(findings, finding{
						area:      "jtds-patterns",
						attempted: "string concatenation result",
						err:       fmt.Sprintf("expected 'Hello World', got '%s'", val),
						category:  "translator",
					})
					t.Logf("FINDING: string concat wrong result: %s", val)
				} else {
					t.Log("String concatenation: OK")
				}
			}
		})

		// CONVERT function (jTDS uses this extensively).
		t.Run("CONVERT", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT CONVERT(VARCHAR(20), 12345) AS conv")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "jtds-patterns",
					attempted: "CONVERT(VARCHAR, int)",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: CONVERT failed: %s", result.Error.Message)
			} else {
				val := decodeRowString(result.Rows[0].Values[0])
				t.Logf("CONVERT: %s", val)
			}
		})

		// ISNULL function (Sybase equivalent of COALESCE).
		t.Run("ISNULL", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT ISNULL(NULL, 'default') AS val")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "jtds-patterns",
					attempted: "ISNULL(NULL, 'default')",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: ISNULL failed: %s", result.Error.Message)
			} else {
				val := decodeRowString(result.Rows[0].Values[0])
				if val != "default" {
					findings = append(findings, finding{
						area:      "jtds-patterns",
						attempted: "ISNULL result",
						err:       fmt.Sprintf("expected 'default', got '%s'", val),
						category:  "translator",
					})
				}
				t.Logf("ISNULL: %s", val)
			}
		})

		// TOP clause (jTDS uses this for ResultSet.setMaxRows).
		t.Run("TOP", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT TOP 1 id FROM jtds_types_test ORDER BY id")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "jtds-patterns",
					attempted: "SELECT TOP 1",
					err:       result.Error.Message,
					category:  "parser",
				})
				t.Logf("FINDING: TOP failed: %s", result.Error.Message)
			} else {
				require.Len(t, result.Rows, 1)
				t.Log("SELECT TOP: OK")
			}
		})

		// CASE expression.
		t.Run("CASE", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END AS result")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "jtds-patterns",
					attempted: "CASE expression",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: CASE failed: %s", result.Error.Message)
			} else {
				val := decodeRowString(result.Rows[0].Values[0])
				require.Equal(t, "yes", val)
				t.Log("CASE expression: OK")
			}
		})

		// CAST function.
		t.Run("CAST", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT CAST(42 AS VARCHAR(10)) AS casted")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "jtds-patterns",
					attempted: "CAST(42 AS VARCHAR(10))",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: CAST failed: %s", result.Error.Message)
			} else {
				t.Log("CAST: OK")
			}
		})

		// LEN function (T-SQL, not LENGTH).
		t.Run("LEN", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT LEN('hello') AS length")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "jtds-patterns",
					attempted: "LEN('hello')",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: LEN failed: %s", result.Error.Message)
			} else {
				t.Log("LEN: OK")
			}
		})

		// CHARINDEX function.
		t.Run("CHARINDEX", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT CHARINDEX('lo', 'hello') AS pos")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "jtds-patterns",
					attempted: "CHARINDEX",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: CHARINDEX failed: %s", result.Error.Message)
			} else {
				t.Log("CHARINDEX: OK")
			}
		})
	})

	// ---------------------------------------------------------------
	// Cleanup
	// ---------------------------------------------------------------
	t.Run("Cleanup", func(t *testing.T) {
		sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS jtds_types_test")
		sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS jtds_identity_test")
	})

	// ---------------------------------------------------------------
	// Report findings
	// ---------------------------------------------------------------
	if len(findings) > 0 {
		t.Logf("\n========================================")
		t.Logf("SMOKE TEST FINDINGS: %d issues found", len(findings))
		t.Logf("========================================")
		for i, f := range findings {
			t.Logf("[%d] Area: %s | Category: %s", i+1, f.area, f.category)
			t.Logf("    Attempted: %s", f.attempted)
			t.Logf("    Error: %s", f.err)
		}
		t.Logf("========================================")
	} else {
		t.Log("No compatibility issues found!")
	}
}

// parseTokenStreamFull is a variant of parseTokenStream that handles
// multiple result sets (COLMETADATA + ROWS + DONE sequences) for
// batch operations. It is not used in the current test but retained
// for future use.
func parseTokenStreamFull(t *testing.T, data []byte) []parsedResult {
	t.Helper()
	var results []parsedResult
	var current parsedResult
	r := bytes.NewReader(data)
	tr := tdswire.NewTokenReader(r)

	for {
		tok, err := tr.PeekToken()
		if err != nil {
			if current.Done != nil || current.ColMeta != nil || current.Error != nil {
				results = append(results, current)
			}
			break
		}

		switch tok {
		case tdswire.TokenLoginAck:
			la, err := tr.ReadLoginAck()
			if err != nil {
				break
			}
			current.LoginAck = &la

		case tdswire.TokenEnvChange:
			ec, err := tr.ReadEnvChange()
			if err != nil {
				break
			}
			current.EnvChanges = append(current.EnvChanges, ec)

		case tdswire.TokenDone, tdswire.TokenDoneProc, tdswire.TokenDoneInProc:
			d, err := tr.ReadDone(tok)
			if err != nil {
				break
			}
			current.Done = &d
			results = append(results, current)
			current = parsedResult{}

		case tdswire.TokenError, tdswire.TokenInfo:
			e, err := tr.ReadError(tok)
			if err != nil {
				break
			}
			if tok == tdswire.TokenError {
				current.Error = &e
			}

		case tdswire.TokenColMetaData:
			md, err := tr.ReadColMetaData()
			if err != nil {
				break
			}
			current.ColMeta = &md
			for {
				nextTok, err := tr.PeekToken()
				if err != nil {
					results = append(results, current)
					return results
				}
				if nextTok == tdswire.TokenRow {
					row, err := tr.ReadRow(md)
					if err != nil {
						break
					}
					current.Rows = append(current.Rows, row)
				} else {
					break
				}
			}
		default:
			results = append(results, current)
			return results
		}
	}
	return results
}

// decodeIntValue decodes a raw TDS integer value from binary wire
// format. Handles INT4 (4 bytes) and INT8 (8 bytes).
func decodeIntValue(b []byte) int64 {
	switch len(b) {
	case 4:
		return int64(int32(binary.LittleEndian.Uint32(b)))
	case 8:
		return int64(binary.LittleEndian.Uint64(b))
	case 2:
		return int64(int16(binary.LittleEndian.Uint16(b)))
	case 1:
		return int64(b[0])
	default:
		return 0
	}
}
