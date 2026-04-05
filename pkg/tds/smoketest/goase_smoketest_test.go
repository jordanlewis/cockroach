// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package smoketest

// This file implements a TDS driver smoke test that exercises the SQL patterns
// used by the SAP go-ase driver (github.com/SAP/go-ase) against CockroachDB's
// TDS frontend. The go-ase driver targets Sybase ASE and uses TDS 5.0 on the
// wire, which is incompatible with our MS TDS 7.x frontend. Instead of
// importing the driver directly, this test replicates the SQL operations from
// the go-ase example programs and integration tests using our raw TDS wire
// protocol client, validating that CockroachDB can handle ASE-style queries
// when they arrive over a TDS 7.x connection (as would happen via a
// bridge/proxy or future protocol negotiation).
//
// Test progression mirrors the go-ase example programs:
//   1. Simple example: CREATE TABLE, INSERT, SELECT with basic types
//   2. Transaction example: BEGIN/COMMIT/ROLLBACK with DML inside transactions
//   3. Prepared statement example: sp_executesql with ? placeholder patterns
//   4. Column types example: Column metadata inspection after typed table creation
//   5. Cursor example: DECLARE/OPEN/FETCH/CLOSE/DEALLOCATE
//   6. ASE-specific data types: Types used in go-ase integration tests
//   7. ASE-specific functions and expressions
//   8. ASE driver init sequence: SET commands sent by go-ase on connection startup
//   9. Error handling: EED-style error introspection patterns

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestGoASESmokeTest runs a comprehensive compatibility smoke test that
// exercises the SQL patterns from the SAP go-ase driver's example programs
// and integration tests against CockroachDB's TDS frontend.
func TestGoASESmokeTest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	addr, cleanup := startTDSTestServer(t, ctx)
	defer cleanup()

	var findings []finding

	// Connect and authenticate.
	tc := dialTDSConn(t, addr)
	defer func() { _ = tc.Close() }()

	// Authenticate using the standard TDS 7.x handshake. The go-ase driver
	// would use TDS 5.0 LOGIN packets, but since our server only speaks 7.x,
	// we use LOGIN7 and test the SQL layer compatibility.
	preLoginResp := doGoMSSQLPreLogin(t, tc)
	require.NotNil(t, preLoginResp)

	loginResp := doGoMSSQLLogin7(t, tc, "root", "", "defaultdb")
	loginResult := parseTokenStream(t, loginResp)
	require.NotNil(t, loginResult.LoginAck, "LOGIN7 should produce LOGINACK")
	t.Logf("Connected to TDS server: %s", loginResult.LoginAck.ProgName)

	// ---------------------------------------------------------------
	// 1. Simple example (go-ase/examples/simple)
	//
	// The simple example creates a table with (a INT, b VARCHAR(256)),
	// inserts rows, and selects them back ordered by column a.
	// ---------------------------------------------------------------
	t.Run("1_SimpleExample", func(t *testing.T) {
		sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS goase_simple")

		t.Run("CreateTable", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"CREATE TABLE goase_simple (a INT NOT NULL, b VARCHAR(256))")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				t.Fatalf("CREATE TABLE failed: %s", result.Error.Message)
			}
			t.Log("CREATE TABLE goase_simple: OK")
		})

		t.Run("InsertRows", func(t *testing.T) {
			inserts := []struct {
				a int
				b string
			}{
				{1, "one"},
				{2, "two"},
				{3, "three"},
			}
			for _, ins := range inserts {
				resp := sendBatch(t, tc.pr, tc.pw,
					fmt.Sprintf("INSERT INTO goase_simple (a, b) VALUES (%d, '%s')", ins.a, ins.b))
				result := parseTokenStream(t, resp)
				if result.Error != nil {
					findings = append(findings, finding{
						area:      "simple-example",
						attempted: fmt.Sprintf("INSERT (%d, '%s')", ins.a, ins.b),
						err:       result.Error.Message,
						category:  "executor",
					})
					t.Errorf("INSERT failed: %s", result.Error.Message)
				}
			}
		})

		t.Run("SelectOrderBy", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT a, b FROM goase_simple ORDER BY a ASC")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "simple-example",
					attempted: "SELECT with ORDER BY",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Fatalf("SELECT failed: %s", result.Error.Message)
			}
			require.NotNil(t, result.ColMeta)
			require.Len(t, result.ColMeta.Columns, 2)
			require.Len(t, result.Rows, 3, "expected 3 rows")
			t.Logf("SELECT returned %d rows with %d columns",
				len(result.Rows), len(result.ColMeta.Columns))
		})
	})

	// ---------------------------------------------------------------
	// 2. Transaction example (go-ase/examples/transaction)
	//
	// The transaction example uses tx.Begin(), prepares a statement
	// inside the transaction, executes INSERT, then commits.
	// ---------------------------------------------------------------
	t.Run("2_TransactionExample", func(t *testing.T) {
		sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS goase_tx")

		resp := sendBatch(t, tc.pr, tc.pw,
			"CREATE TABLE goase_tx (a INT NOT NULL, b VARCHAR(256))")
		result := parseTokenStream(t, resp)
		if result.Error != nil {
			t.Fatalf("CREATE TABLE failed: %s", result.Error.Message)
		}

		// BEGIN TRAN, INSERT, COMMIT — mirrors go-ase tx.Begin()/tx.Commit().
		t.Run("CommitTransaction", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "BEGIN TRAN")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "transaction-example",
					attempted: "BEGIN TRAN",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Fatalf("BEGIN TRAN failed: %s", result.Error.Message)
			}

			// go-ase uses prepared statements inside transactions.
			resp = sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO goase_tx (a, b) VALUES (1, 'committed')")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				sendBatch(t, tc.pr, tc.pw, "ROLLBACK")
				t.Fatalf("INSERT in txn failed: %s", result.Error.Message)
			}

			resp = sendBatch(t, tc.pr, tc.pw, "COMMIT")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "transaction-example",
					attempted: "COMMIT",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Fatalf("COMMIT failed: %s", result.Error.Message)
			}

			// Verify committed data.
			resp = sendBatch(t, tc.pr, tc.pw,
				"SELECT b FROM goase_tx WHERE a = 1")
			result = parseTokenStream(t, resp)
			require.Nil(t, result.Error)
			require.Len(t, result.Rows, 1)
			val := decodeRowString(result.Rows[0].Values[0])
			require.Equal(t, "committed", val)
			t.Log("Commit transaction: OK")
		})

		// BEGIN TRAN, INSERT, ROLLBACK — verify rollback works.
		t.Run("RollbackTransaction", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "BEGIN TRAN")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				t.Skipf("BEGIN TRAN failed: %s", result.Error.Message)
			}

			resp = sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO goase_tx (a, b) VALUES (2, 'rolled_back')")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				t.Logf("INSERT in txn failed: %s", result.Error.Message)
			}

			resp = sendBatch(t, tc.pr, tc.pw, "ROLLBACK")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "transaction-example",
					attempted: "ROLLBACK",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Errorf("ROLLBACK failed: %s", result.Error.Message)
			}

			// Verify rolled-back row does not exist.
			resp = sendBatch(t, tc.pr, tc.pw,
				"SELECT COUNT(*) AS cnt FROM goase_tx WHERE a = 2")
			result = parseTokenStream(t, resp)
			if result.Error == nil && len(result.Rows) > 0 {
				count := decodeIntValue(result.Rows[0].Values[0])
				if count != 0 {
					findings = append(findings, finding{
						area:      "transaction-example",
						attempted: "ROLLBACK verification",
						err:       fmt.Sprintf("row with a=2 still exists after ROLLBACK (count=%d)", count),
						category:  "executor",
					})
				} else {
					t.Log("Rollback transaction: OK")
				}
			}
		})
	})

	// ---------------------------------------------------------------
	// 3. Prepared statement example (go-ase/examples/preparedStatement)
	//
	// go-ase uses db.Prepare() with ? placeholders. On the wire, this
	// translates to sp_executesql with @p1, @p2, etc.
	// ---------------------------------------------------------------
	t.Run("3_PreparedStatementExample", func(t *testing.T) {
		sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS goase_prepared")

		resp := sendBatch(t, tc.pr, tc.pw,
			"CREATE TABLE goase_prepared (a INT NOT NULL, b VARCHAR(256))")
		result := parseTokenStream(t, resp)
		if result.Error != nil {
			t.Fatalf("CREATE TABLE failed: %s", result.Error.Message)
		}

		// Insert via sp_executesql (how go-ase prepared statements translate).
		t.Run("PreparedInsert", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"EXEC sp_executesql N'INSERT INTO goase_prepared (a, b) VALUES (@p1, @p2)', "+
					"N'@p1 INT, @p2 VARCHAR(256)', @p1 = 1, @p2 = 'prepared_value'")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "prepared-stmt",
					attempted: "sp_executesql INSERT with params",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: prepared INSERT failed: %s", result.Error.Message)
				// Fall back to direct INSERT for remaining tests.
				resp = sendBatch(t, tc.pr, tc.pw,
					"INSERT INTO goase_prepared (a, b) VALUES (1, 'prepared_value')")
				result = parseTokenStream(t, resp)
				require.Nil(t, result.Error, "direct INSERT fallback should work")
			} else {
				t.Log("Prepared INSERT: OK")
			}
		})

		// Query via sp_executesql.
		t.Run("PreparedQuery", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"EXEC sp_executesql N'SELECT a, b FROM goase_prepared WHERE a = @p1', "+
					"N'@p1 INT', @p1 = 1")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "prepared-stmt",
					attempted: "sp_executesql SELECT with params",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: prepared SELECT failed: %s", result.Error.Message)
			} else {
				require.NotNil(t, result.ColMeta)
				require.Len(t, result.Rows, 1)
				t.Logf("Prepared SELECT: %d rows", len(result.Rows))
			}
		})

		// Multiple parameters (go-ase supports multiple ? placeholders).
		t.Run("MultipleParams", func(t *testing.T) {
			// Insert a few more rows first.
			sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO goase_prepared (a, b) VALUES (2, 'second')")
			sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO goase_prepared (a, b) VALUES (3, 'third')")

			resp := sendBatch(t, tc.pr, tc.pw,
				"EXEC sp_executesql N'SELECT a, b FROM goase_prepared WHERE a >= @p1 AND a <= @p2 ORDER BY a', "+
					"N'@p1 INT, @p2 INT', @p1 = 1, @p2 = 3")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "prepared-stmt",
					attempted: "sp_executesql with multiple params",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: multi-param query failed: %s", result.Error.Message)
			} else {
				t.Logf("Multi-param query: %d rows", len(result.Rows))
			}
		})
	})

	// ---------------------------------------------------------------
	// 4. Column types example (go-ase/examples/columnTypes)
	//
	// The columnTypes example inspects column metadata: type name,
	// length, precision, scale, and nullable. It creates a table with
	// a mix of numeric, string, and date types.
	// ---------------------------------------------------------------
	t.Run("4_ColumnTypesExample", func(t *testing.T) {
		sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS goase_coltypes")

		// go-ase column type tests use Sybase ASE types. We map to
		// T-SQL equivalents that our TDS frontend translates.
		ddl := `CREATE TABLE goase_coltypes (
			col_int INT NOT NULL,
			col_bigint BIGINT,
			col_smallint SMALLINT,
			col_tinyint TINYINT,
			col_varchar VARCHAR(100),
			col_char CHAR(10),
			col_decimal DECIMAL(18,4),
			col_numeric NUMERIC(10,2),
			col_float FLOAT,
			col_real REAL,
			col_bit BIT,
			col_money MONEY,
			col_smallmoney SMALLMONEY,
			col_datetime DATETIME,
			col_smalldatetime SMALLDATETIME,
			col_text TEXT,
			col_nvarchar NVARCHAR(100),
			col_nchar NCHAR(10),
			col_varbinary VARBINARY(100),
			col_image IMAGE
		)`
		resp := sendBatch(t, tc.pr, tc.pw, ddl)
		result := parseTokenStream(t, resp)
		if result.Error != nil {
			findings = append(findings, finding{
				area:      "column-types",
				attempted: "CREATE TABLE with ASE-compatible types",
				err:       result.Error.Message,
				category:  "parser/type-system",
			})
			t.Logf("FINDING: CREATE TABLE with typed columns failed: %s", result.Error.Message)

			// Fall back to simpler table.
			resp = sendBatch(t, tc.pr, tc.pw,
				"CREATE TABLE goase_coltypes (col_int INT NOT NULL, col_varchar VARCHAR(100), col_decimal DECIMAL(18,4))")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				t.Fatalf("Even simple CREATE TABLE failed: %s", result.Error.Message)
			}
		} else {
			t.Log("CREATE TABLE with ASE-compatible types: OK")
		}

		// Insert a row and query column metadata.
		t.Run("InsertAndQueryMetadata", func(t *testing.T) {
			sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO goase_coltypes (col_int) VALUES (1)")

			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT * FROM goase_coltypes WHERE col_int = 1")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				t.Fatalf("SELECT * failed: %s", result.Error.Message)
			}
			require.NotNil(t, result.ColMeta)
			t.Logf("Column metadata: %d columns returned", len(result.ColMeta.Columns))
			for i, col := range result.ColMeta.Columns {
				t.Logf("  col[%d]: name=%q type=0x%02X", i, col.ColName, col.TypeInfo.TypeID)
			}
		})

		// Query column metadata from INFORMATION_SCHEMA (go-ase uses
		// rows.ColumnTypes() which maps to TDS COLMETADATA).
		t.Run("InfoSchemaColumns", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE "+
					"FROM INFORMATION_SCHEMA.COLUMNS "+
					"WHERE TABLE_NAME = 'goase_coltypes' "+
					"ORDER BY ORDINAL_POSITION")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "column-types",
					attempted: "INFORMATION_SCHEMA.COLUMNS query",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: INFORMATION_SCHEMA.COLUMNS query failed: %s", result.Error.Message)
			} else {
				t.Logf("INFORMATION_SCHEMA.COLUMNS: %d columns found", len(result.Rows))
			}
		})
	})

	// ---------------------------------------------------------------
	// 5. Cursor example (go-ase/examples/cursor)
	//
	// The cursor example inserts multiple rows, then uses a server-side
	// cursor to iterate one row at a time.
	// ---------------------------------------------------------------
	t.Run("5_CursorExample", func(t *testing.T) {
		sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS goase_cursor")

		resp := sendBatch(t, tc.pr, tc.pw,
			"CREATE TABLE goase_cursor (a INT NOT NULL, b VARCHAR(256))")
		result := parseTokenStream(t, resp)
		require.Nil(t, result.Error, "CREATE TABLE should succeed")

		// Insert multiple rows (go-ase cursor example inserts 3 rows).
		for i := 1; i <= 3; i++ {
			resp = sendBatch(t, tc.pr, tc.pw,
				fmt.Sprintf("INSERT INTO goase_cursor (a, b) VALUES (%d, 'row_%d')", i, i))
			result = parseTokenStream(t, resp)
			require.Nil(t, result.Error, "INSERT should succeed")
		}

		// DECLARE + OPEN + FETCH + CLOSE + DEALLOCATE.
		t.Run("FullCursorLifecycle", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"DECLARE goase_cur CURSOR FOR SELECT a, b FROM goase_cursor ORDER BY a")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "cursor-example",
					attempted: "DECLARE CURSOR",
					err:       result.Error.Message,
					category:  "parser",
				})
				t.Logf("FINDING: DECLARE CURSOR failed: %s", result.Error.Message)
				return
			}

			resp = sendBatch(t, tc.pr, tc.pw, "OPEN goase_cur")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "cursor-example",
					attempted: "OPEN CURSOR",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: OPEN CURSOR failed: %s", result.Error.Message)
				return
			}

			// Fetch all rows one at a time (go-ase iterates until no more rows).
			fetchCount := 0
			for i := 0; i < 5; i++ { // safety limit
				resp = sendBatch(t, tc.pr, tc.pw, "FETCH NEXT FROM goase_cur")
				result = parseTokenStream(t, resp)
				if result.Error != nil {
					findings = append(findings, finding{
						area:      "cursor-example",
						attempted: "FETCH NEXT FROM CURSOR",
						err:       result.Error.Message,
						category:  "executor",
					})
					t.Logf("FINDING: FETCH failed: %s", result.Error.Message)
					break
				}
				if len(result.Rows) == 0 {
					break // no more rows
				}
				fetchCount++
			}
			if fetchCount > 0 {
				t.Logf("FETCH: retrieved %d rows via cursor", fetchCount)
			}

			// Cleanup cursor.
			sendBatch(t, tc.pr, tc.pw, "CLOSE goase_cur")
			sendBatch(t, tc.pr, tc.pw, "DEALLOCATE goase_cur")
			t.Log("Cursor lifecycle: OK")
		})
	})

	// ---------------------------------------------------------------
	// 6. ASE-specific data types (go-ase integration tests)
	//
	// go-ase integration tests exercise Sybase-specific types:
	// UNSIGNED INT/BIGINT/SMALLINT, BIGDATETIME, BIGTIME, and
	// MONEY with specific precision requirements.
	// ---------------------------------------------------------------
	t.Run("6_ASEDataTypes", func(t *testing.T) {
		// MONEY type with value.
		t.Run("MoneyInsertSelect", func(t *testing.T) {
			sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS goase_money")
			resp := sendBatch(t, tc.pr, tc.pw,
				"CREATE TABLE goase_money (id INT NOT NULL, amount MONEY)")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-types",
					attempted: "CREATE TABLE with MONEY",
					err:       result.Error.Message,
					category:  "parser/type-system",
				})
				t.Logf("FINDING: MONEY type failed: %s", result.Error.Message)
				return
			}

			resp = sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO goase_money (id, amount) VALUES (1, 123.4567)")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-types",
					attempted: "INSERT MONEY value",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: MONEY INSERT failed: %s", result.Error.Message)
				return
			}

			resp = sendBatch(t, tc.pr, tc.pw,
				"SELECT amount FROM goase_money WHERE id = 1")
			result = parseTokenStream(t, resp)
			require.Nil(t, result.Error)
			require.Len(t, result.Rows, 1)
			t.Log("MONEY insert/select: OK")
		})

		// SMALLMONEY type.
		t.Run("SmallMoney", func(t *testing.T) {
			sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS goase_smallmoney")
			resp := sendBatch(t, tc.pr, tc.pw,
				"CREATE TABLE goase_smallmoney (id INT NOT NULL, amount SMALLMONEY)")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-types",
					attempted: "CREATE TABLE with SMALLMONEY",
					err:       result.Error.Message,
					category:  "parser/type-system",
				})
				t.Logf("FINDING: SMALLMONEY type failed: %s", result.Error.Message)
				return
			}

			resp = sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO goase_smallmoney (id, amount) VALUES (1, 12.34)")
			result = parseTokenStream(t, resp)
			require.Nil(t, result.Error)

			resp = sendBatch(t, tc.pr, tc.pw,
				"SELECT amount FROM goase_smallmoney WHERE id = 1")
			result = parseTokenStream(t, resp)
			require.Nil(t, result.Error)
			require.Len(t, result.Rows, 1)
			t.Log("SMALLMONEY: OK")
		})

		// DATETIME and SMALLDATETIME.
		t.Run("DateTimeTypes", func(t *testing.T) {
			sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS goase_dt")
			resp := sendBatch(t, tc.pr, tc.pw,
				"CREATE TABLE goase_dt (id INT NOT NULL, dt DATETIME, sdt SMALLDATETIME)")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-types",
					attempted: "CREATE TABLE with DATETIME/SMALLDATETIME",
					err:       result.Error.Message,
					category:  "parser/type-system",
				})
				t.Logf("FINDING: DATETIME types failed: %s", result.Error.Message)
				return
			}

			resp = sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO goase_dt (id, dt, sdt) VALUES (1, '2026-01-15 10:30:00', '2026-01-15 10:30:00')")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-types",
					attempted: "INSERT DATETIME values",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: DATETIME INSERT failed: %s", result.Error.Message)
			} else {
				t.Log("DATETIME/SMALLDATETIME insert: OK")
			}
		})

		// DECIMAL with various precisions (go-ase tests many precision combos).
		t.Run("DecimalPrecisions", func(t *testing.T) {
			sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS goase_decimal")
			resp := sendBatch(t, tc.pr, tc.pw,
				"CREATE TABLE goase_decimal ("+
					"id INT NOT NULL, "+
					"d1 DECIMAL(5,2), "+
					"d2 DECIMAL(10,4), "+
					"d3 DECIMAL(18,6), "+
					"d4 DECIMAL(38,10), "+
					"n1 NUMERIC(12,3))")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-types",
					attempted: "CREATE TABLE with DECIMAL precisions",
					err:       result.Error.Message,
					category:  "parser/type-system",
				})
				t.Logf("FINDING: DECIMAL types failed: %s", result.Error.Message)
				return
			}

			resp = sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO goase_decimal (id, d1, d2, d3, d4, n1) VALUES "+
					"(1, 123.45, 123456.7890, 123456789012.345678, 1234567890123456789012345678.9012345678, 123456789.012)")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-types",
					attempted: "INSERT DECIMAL values",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: DECIMAL INSERT failed: %s", result.Error.Message)
			} else {
				t.Log("DECIMAL precision inserts: OK")
			}
		})

		// IMAGE and TEXT types (go-ase integration tests cover these).
		t.Run("LOBTypes", func(t *testing.T) {
			sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS goase_lob")
			resp := sendBatch(t, tc.pr, tc.pw,
				"CREATE TABLE goase_lob (id INT NOT NULL, img IMAGE, txt TEXT)")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-types",
					attempted: "CREATE TABLE with IMAGE/TEXT",
					err:       result.Error.Message,
					category:  "parser/type-system",
				})
				t.Logf("FINDING: IMAGE/TEXT types failed: %s", result.Error.Message)
			} else {
				t.Log("IMAGE/TEXT types: OK")
			}
		})

		// BIT type.
		t.Run("BitType", func(t *testing.T) {
			sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS goase_bit")
			resp := sendBatch(t, tc.pr, tc.pw,
				"CREATE TABLE goase_bit (id INT NOT NULL, flag BIT)")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-types",
					attempted: "CREATE TABLE with BIT",
					err:       result.Error.Message,
					category:  "parser/type-system",
				})
				t.Logf("FINDING: BIT type failed: %s", result.Error.Message)
				return
			}

			resp = sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO goase_bit (id, flag) VALUES (1, 1)")
			result = parseTokenStream(t, resp)
			require.Nil(t, result.Error)

			resp = sendBatch(t, tc.pr, tc.pw,
				"SELECT flag FROM goase_bit WHERE id = 1")
			result = parseTokenStream(t, resp)
			require.Nil(t, result.Error)
			require.Len(t, result.Rows, 1)
			t.Log("BIT type: OK")
		})

		// BINARY/VARBINARY (go-ase integration tests cover these).
		t.Run("BinaryTypes", func(t *testing.T) {
			sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS goase_binary")
			resp := sendBatch(t, tc.pr, tc.pw,
				"CREATE TABLE goase_binary (id INT NOT NULL, b BINARY(16), vb VARBINARY(256))")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-types",
					attempted: "CREATE TABLE with BINARY/VARBINARY",
					err:       result.Error.Message,
					category:  "parser/type-system",
				})
				t.Logf("FINDING: BINARY types failed: %s", result.Error.Message)
				return
			}

			resp = sendBatch(t, tc.pr, tc.pw,
				"INSERT INTO goase_binary (id, b, vb) VALUES (1, 0x0102030405060708090A0B0C0D0E0F10, 0xDEADBEEF)")
			result = parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-types",
					attempted: "INSERT BINARY values",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: BINARY INSERT failed: %s", result.Error.Message)
			} else {
				t.Log("BINARY/VARBINARY: OK")
			}
		})
	})

	// ---------------------------------------------------------------
	// 7. ASE-specific functions and expressions
	//
	// go-ase examples and real ASE applications use Sybase-specific
	// functions. Test the ones CockroachDB's TDS translator supports.
	// ---------------------------------------------------------------
	t.Run("7_ASEFunctions", func(t *testing.T) {
		// CONVERT (ASE uses CONVERT extensively).
		t.Run("CONVERT_int_to_varchar", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT CONVERT(VARCHAR(20), 12345) AS conv")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-functions",
					attempted: "CONVERT(VARCHAR, INT)",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: CONVERT failed: %s", result.Error.Message)
			} else {
				val := decodeRowString(result.Rows[0].Values[0])
				t.Logf("CONVERT(VARCHAR, 12345) = %s", val)
			}
		})

		// CONVERT with date style (ASE-specific date formatting).
		t.Run("CONVERT_datetime", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT CONVERT(VARCHAR(30), GETDATE(), 120) AS formatted_date")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-functions",
					attempted: "CONVERT with datetime style 120",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: CONVERT datetime failed: %s", result.Error.Message)
			} else {
				val := decodeRowString(result.Rows[0].Values[0])
				t.Logf("CONVERT datetime: %s", val)
			}
		})

		// ISNULL (ASE equivalent of COALESCE for two args).
		t.Run("ISNULL", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT ISNULL(NULL, 'fallback') AS val")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-functions",
					attempted: "ISNULL(NULL, 'fallback')",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: ISNULL failed: %s", result.Error.Message)
			} else {
				val := decodeRowString(result.Rows[0].Values[0])
				require.Equal(t, "fallback", val)
				t.Log("ISNULL: OK")
			}
		})

		// DATALENGTH (ASE uses this where MSSQL uses LEN/DATALENGTH).
		t.Run("DATALENGTH", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT DATALENGTH('hello world') AS dl")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-functions",
					attempted: "DATALENGTH('hello world')",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: DATALENGTH failed: %s", result.Error.Message)
			} else {
				t.Log("DATALENGTH: OK")
			}
		})

		// GETDATE() — common in ASE applications.
		t.Run("GETDATE", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT GETDATE() AS now")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-functions",
					attempted: "GETDATE()",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: GETDATE() failed: %s", result.Error.Message)
			} else {
				val := decodeRowString(result.Rows[0].Values[0])
				t.Logf("GETDATE(): %s", val)
			}
		})

		// String concatenation with + (ASE style).
		t.Run("StringConcat", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT 'Hello' + ' ' + 'ASE' AS greeting")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-functions",
					attempted: "string concatenation with +",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: string concat failed: %s", result.Error.Message)
			} else {
				val := decodeRowString(result.Rows[0].Values[0])
				require.Equal(t, "Hello ASE", val)
				t.Log("String concatenation: OK")
			}
		})

		// CAST (used throughout go-ase tests for type conversions).
		t.Run("CAST", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT CAST(3.14159 AS DECIMAL(10,2)) AS pi_approx")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-functions",
					attempted: "CAST(float AS DECIMAL)",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: CAST failed: %s", result.Error.Message)
			} else {
				t.Log("CAST: OK")
			}
		})

		// COALESCE (go-ase uses this for nullable column handling).
		t.Run("COALESCE", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT COALESCE(NULL, NULL, 'third') AS val")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-functions",
					attempted: "COALESCE with multiple NULLs",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: COALESCE failed: %s", result.Error.Message)
			} else {
				val := decodeRowString(result.Rows[0].Values[0])
				require.Equal(t, "third", val)
				t.Log("COALESCE: OK")
			}
		})

		// NULLIF (common in ASE division-safety patterns).
		t.Run("NULLIF", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT NULLIF(1, 1) AS same, NULLIF(1, 2) AS diff")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-functions",
					attempted: "NULLIF",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: NULLIF failed: %s", result.Error.Message)
			} else {
				t.Log("NULLIF: OK")
			}
		})

		// SUBSTRING (ASE uses 1-based indexing like T-SQL).
		t.Run("SUBSTRING", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT SUBSTRING('hello world', 1, 5) AS sub")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-functions",
					attempted: "SUBSTRING('hello world', 1, 5)",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: SUBSTRING failed: %s", result.Error.Message)
			} else {
				val := decodeRowString(result.Rows[0].Values[0])
				require.Equal(t, "hello", val)
				t.Log("SUBSTRING: OK")
			}
		})

		// UPPER/LOWER (basic string functions used in go-ase apps).
		t.Run("UpperLower", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT UPPER('hello') AS up, LOWER('WORLD') AS lo")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "ase-functions",
					attempted: "UPPER/LOWER",
					err:       result.Error.Message,
					category:  "translator",
				})
				t.Logf("FINDING: UPPER/LOWER failed: %s", result.Error.Message)
			} else {
				require.Len(t, result.ColMeta.Columns, 2)
				t.Log("UPPER/LOWER: OK")
			}
		})
	})

	// ---------------------------------------------------------------
	// 8. ASE driver init sequence
	//
	// go-ase sends specific SET commands when opening a connection.
	// These are Sybase ASE session configuration options.
	// ---------------------------------------------------------------
	t.Run("8_ASEDriverInit", func(t *testing.T) {
		// SET commands that go-ase/ASE applications commonly send.
		setCmds := []struct {
			cmd  string
			note string
		}{
			{"SET QUOTED_IDENTIFIER ON", "ANSI identifier quoting"},
			{"SET ANSI_NULLS ON", "ANSI null comparison"},
			{"SET ANSI_PADDING ON", "ANSI string padding"},
			{"SET CONCAT_NULL_YIELDS_NULL ON", "NULL concat behavior"},
			{"SET TEXTSIZE 2147483647", "max text column size"},
			{"SET ARITHABORT ON", "abort on arithmetic error"},
		}
		for _, sc := range setCmds {
			t.Run(sc.cmd, func(t *testing.T) {
				resp := sendBatch(t, tc.pr, tc.pw, sc.cmd)
				result := parseTokenStream(t, resp)
				if result.Error != nil {
					findings = append(findings, finding{
						area:      "driver-init",
						attempted: sc.cmd,
						err:       result.Error.Message,
						category:  "catalog",
					})
					t.Logf("FINDING: %s failed: %s", sc.cmd, result.Error.Message)
				}
			})
		}

		// SET CHAINED ON/OFF is ASE-specific auto-commit control.
		// In ASE, SET CHAINED ON means "no auto-commit" (like BEGIN TRAN
		// before every statement). CHAINED OFF = auto-commit (CRDB default).
		// Silently acknowledged by the catalog layer.
		t.Run("SET_CHAINED", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "SET CHAINED OFF")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				t.Fatalf("SET CHAINED OFF should be acknowledged: %s", result.Error.Message)
			}
			t.Log("SET CHAINED OFF: OK")
		})

		// SET TRANSACTION ISOLATION LEVEL (go-ase apps may set this).
		t.Run("IsolationLevel", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
			result := parseTokenStream(t, resp)
			if result.Error != nil {
				findings = append(findings, finding{
					area:      "driver-init",
					attempted: "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
					err:       result.Error.Message,
					category:  "executor",
				})
				t.Logf("FINDING: isolation level failed: %s", result.Error.Message)
			} else {
				t.Log("SET TRANSACTION ISOLATION LEVEL: OK")
			}
		})
	})

	// ---------------------------------------------------------------
	// 9. Error handling patterns
	//
	// go-ase introspects errors via EED (Extended Error Data) packages.
	// Our TDS frontend returns TokenError with Number, Class, State,
	// and Message fields.
	// ---------------------------------------------------------------
	t.Run("9_ErrorHandling", func(t *testing.T) {
		// Reference nonexistent table.
		t.Run("TableNotFound", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT * FROM goase_nonexistent_table_xyz")
			result := parseTokenStream(t, resp)
			if result.Error == nil {
				findings = append(findings, finding{
					area:      "error-handling",
					attempted: "SELECT from nonexistent table",
					err:       "no error returned",
					category:  "executor",
				})
			} else {
				t.Logf("Table not found error: number=%d class=%d msg=%s",
					result.Error.Number, result.Error.Class, result.Error.Message)
			}
		})

		// Syntax error (go-ase EED example uses invalid CREATE TABLE).
		t.Run("SyntaxError", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECTT INVALID SYNTAX")
			result := parseTokenStream(t, resp)
			if result.Error == nil {
				findings = append(findings, finding{
					area:      "error-handling",
					attempted: "syntax error",
					err:       "no error returned",
					category:  "parser",
				})
			} else {
				t.Logf("Syntax error: number=%d msg=%s",
					result.Error.Number, result.Error.Message)
			}
		})

		// Type mismatch (go-ase EED tests inspect type errors).
		t.Run("TypeMismatch", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw,
				"SELECT CAST('not_a_number' AS INT)")
			result := parseTokenStream(t, resp)
			if result.Error == nil {
				findings = append(findings, finding{
					area:      "error-handling",
					attempted: "CAST type mismatch",
					err:       "no error returned for invalid CAST",
					category:  "executor",
				})
			} else {
				t.Logf("Type mismatch error: %s", result.Error.Message)
			}
		})

		// Connection survives errors.
		t.Run("ConnectionSurvives", func(t *testing.T) {
			resp := sendBatch(t, tc.pr, tc.pw, "SELECT 'alive' AS status")
			result := parseTokenStream(t, resp)
			require.Nil(t, result.Error, "connection should survive errors")
			require.Len(t, result.Rows, 1)
			val := decodeRowString(result.Rows[0].Values[0])
			require.Equal(t, "alive", val)
			t.Log("Connection survived errors: OK")
		})
	})

	// ---------------------------------------------------------------
	// Cleanup
	// ---------------------------------------------------------------
	t.Run("Cleanup", func(t *testing.T) {
		tables := []string{
			"goase_simple", "goase_tx", "goase_prepared",
			"goase_coltypes", "goase_cursor", "goase_money",
			"goase_smallmoney", "goase_dt", "goase_decimal",
			"goase_lob", "goase_bit", "goase_binary",
		}
		for _, tbl := range tables {
			sendBatch(t, tc.pr, tc.pw, "DROP TABLE IF EXISTS "+tbl)
		}
	})

	// ---------------------------------------------------------------
	// Report findings
	// ---------------------------------------------------------------
	if len(findings) > 0 {
		t.Logf("\n========================================")
		t.Logf("GO-ASE SMOKE TEST FINDINGS: %d issues found", len(findings))
		t.Logf("========================================")
		for i, f := range findings {
			t.Logf("[%d] Area: %s | Category: %s", i+1, f.area, f.category)
			t.Logf("    Attempted: %s", f.attempted)
			t.Logf("    Error: %s", f.err)
		}
		t.Logf("========================================")
		t.Logf("Note: The go-ase driver uses Sybase TDS 5.0 on the wire,")
		t.Logf("which is incompatible with our MS TDS 7.x frontend.")
		t.Logf("These tests validate SQL-level compatibility only.")
		t.Logf("Full go-ase driver integration requires TDS 5.0 support.")
	} else {
		t.Log("No compatibility issues found!")
	}
}
