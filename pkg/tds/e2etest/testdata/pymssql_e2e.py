#!/usr/bin/env python3
"""
pymssql_e2e.py: End-to-end test script for CockroachDB's TDS frontend.

This script is invoked by TestPymssqlE2E in pymssql_test.go. It connects
to the TDS server using pymssql (which bundles FreeTDS) and runs a series
of operations to validate compatibility.

Usage: python3 pymssql_e2e.py <host> <port>

Each test prints PASS or FAIL with a test name. The Go test checks for
these markers in the output.
"""

import os
import sys

# Set TDS version before importing pymssql.
os.environ["TDSVER"] = "7.3"

import pymssql  # noqa: E402


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <host> <port>", file=sys.stderr)
        sys.exit(1)

    host = sys.argv[1]
    port = sys.argv[2]
    failures = 0

    # --- Test: connect ---
    try:
        conn = pymssql.connect(
            server=host,
            port=port,
            user="root",
            password="",
            database="defaultdb",
            tds_version="7.3",
        )
        print("PASS: connect")
    except Exception as e:
        print(f"FAIL: connect — {e}")
        sys.exit(1)

    cursor = conn.cursor()

    # --- Test: select_one ---
    try:
        cursor.execute("SELECT 1 AS val")
        row = cursor.fetchone()
        assert row is not None, "expected a row"
        assert row[0] == 1, f"expected 1, got {row[0]}"
        print("PASS: select_one")
    except Exception as e:
        print(f"FAIL: select_one — {e}")
        failures += 1

    # --- Test: select_version ---
    try:
        cursor.execute("SELECT @@VERSION")
        row = cursor.fetchone()
        assert row is not None
        version = row[0]
        assert "CockroachDB" in version, f"expected CockroachDB in version, got {version}"
        print("PASS: select_version")
    except Exception as e:
        print(f"FAIL: select_version — {e}")
        failures += 1

    # --- Test: create_table ---
    try:
        cursor.execute("DROP TABLE IF EXISTS pymssql_e2e_test")
        cursor.execute(
            "CREATE TABLE pymssql_e2e_test ("
            "  id INT NOT NULL,"
            "  name VARCHAR(100),"
            "  value FLOAT,"
            "  active BIT"
            ")"
        )
        print("PASS: create_table")
    except Exception as e:
        print(f"FAIL: create_table — {e}")
        failures += 1

    # --- Test: insert ---
    try:
        cursor.execute(
            "INSERT INTO pymssql_e2e_test (id, name, value, active) "
            "VALUES (1, 'Alice', 3.14, 1)"
        )
        cursor.execute(
            "INSERT INTO pymssql_e2e_test (id, name, value, active) "
            "VALUES (2, 'Bob', 2.718, 0)"
        )
        cursor.execute(
            "INSERT INTO pymssql_e2e_test (id, name, value, active) "
            "VALUES (3, 'Charlie', 1.414, 1)"
        )
        print("PASS: insert")
    except Exception as e:
        print(f"FAIL: insert — {e}")
        failures += 1

    # --- Test: select_all ---
    try:
        cursor.execute("SELECT id, name, value, active FROM pymssql_e2e_test ORDER BY id")
        rows = cursor.fetchall()
        assert len(rows) == 3, f"expected 3 rows, got {len(rows)}"
        assert rows[0][1] == "Alice", f"expected Alice, got {rows[0][1]}"
        assert rows[1][1] == "Bob", f"expected Bob, got {rows[1][1]}"
        assert rows[2][1] == "Charlie", f"expected Charlie, got {rows[2][1]}"
        print("PASS: select_all")
    except Exception as e:
        print(f"FAIL: select_all — {e}")
        failures += 1

    # --- Test: update ---
    try:
        cursor.execute("UPDATE pymssql_e2e_test SET name = 'Alicia' WHERE id = 1")
        cursor.execute("SELECT name FROM pymssql_e2e_test WHERE id = 1")
        row = cursor.fetchone()
        assert row[0] == "Alicia", f"expected Alicia, got {row[0]}"
        print("PASS: update")
    except Exception as e:
        print(f"FAIL: update — {e}")
        failures += 1

    # --- Test: delete ---
    try:
        cursor.execute("DELETE FROM pymssql_e2e_test WHERE id = 3")
        cursor.execute("SELECT COUNT(*) FROM pymssql_e2e_test")
        row = cursor.fetchone()
        assert row[0] == 2, f"expected 2 rows after delete, got {row[0]}"
        print("PASS: delete")
    except Exception as e:
        print(f"FAIL: delete — {e}")
        failures += 1

    # --- Test: parameterized_query ---
    try:
        cursor.execute(
            "SELECT id, name FROM pymssql_e2e_test WHERE id = %s", (1,)
        )
        row = cursor.fetchone()
        assert row is not None, "expected a row for parameterized query"
        assert row[0] == 1
        print("PASS: parameterized_query")
    except Exception as e:
        print(f"FAIL: parameterized_query — {e}")
        failures += 1

    # --- Test: transaction_commit ---
    try:
        conn.autocommit(False)
        cursor.execute(
            "INSERT INTO pymssql_e2e_test (id, name, value, active) "
            "VALUES (10, 'TxCommit', 0.0, 1)"
        )
        conn.commit()
        cursor.execute("SELECT name FROM pymssql_e2e_test WHERE id = 10")
        row = cursor.fetchone()
        assert row is not None, "committed row should be visible"
        assert row[0] == "TxCommit"
        print("PASS: transaction_commit")
    except Exception as e:
        print(f"FAIL: transaction_commit — {e}")
        failures += 1

    # --- Test: transaction_rollback ---
    try:
        conn.autocommit(False)
        cursor.execute(
            "INSERT INTO pymssql_e2e_test (id, name, value, active) "
            "VALUES (20, 'TxRollback', 0.0, 1)"
        )
        conn.rollback()
        cursor.execute("SELECT name FROM pymssql_e2e_test WHERE id = 20")
        row = cursor.fetchone()
        assert row is None, "rolled-back row should not be visible"
        print("PASS: transaction_rollback")
    except Exception as e:
        print(f"FAIL: transaction_rollback — {e}")
        failures += 1

    # --- Test: null_handling ---
    try:
        conn.autocommit(True)
        cursor.execute(
            "INSERT INTO pymssql_e2e_test (id, name, value, active) "
            "VALUES (30, NULL, NULL, NULL)"
        )
        cursor.execute(
            "SELECT name, value, active FROM pymssql_e2e_test WHERE id = 30"
        )
        row = cursor.fetchone()
        assert row[0] is None, f"expected NULL name, got {row[0]}"
        assert row[1] is None, f"expected NULL value, got {row[1]}"
        assert row[2] is None, f"expected NULL active, got {row[2]}"
        print("PASS: null_handling")
    except Exception as e:
        print(f"FAIL: null_handling — {e}")
        failures += 1

    # --- Test: multiple_result_columns ---
    try:
        cursor.execute(
            "SELECT 'hello' AS greeting, 42 AS answer, CAST(3.14 AS FLOAT) AS pi_val"
        )
        row = cursor.fetchone()
        assert row[0] == "hello", f"expected 'hello', got {row[0]}"
        assert row[1] == 42, f"expected 42, got {row[1]}"
        print("PASS: multiple_result_columns")
    except Exception as e:
        print(f"FAIL: multiple_result_columns — {e}")
        failures += 1

    # --- Test: set_commands ---
    try:
        cursor.execute("SET ANSI_NULLS ON")
        cursor.execute("SET QUOTED_IDENTIFIER ON")
        cursor.execute("SELECT 1 AS after_set")
        row = cursor.fetchone()
        assert row[0] == 1
        print("PASS: set_commands")
    except Exception as e:
        print(f"FAIL: set_commands — {e}")
        failures += 1

    # --- Test: string_functions ---
    try:
        cursor.execute("SELECT LEN('hello') AS len_result")
        row = cursor.fetchone()
        assert row[0] == 5, f"expected 5, got {row[0]}"

        cursor.execute("SELECT UPPER('hello') AS upper_result")
        row = cursor.fetchone()
        assert row[0] == "HELLO", f"expected HELLO, got {row[0]}"

        cursor.execute("SELECT SUBSTRING('hello world', 1, 5) AS sub_result")
        row = cursor.fetchone()
        assert row[0] == "hello", f"expected hello, got {row[0]}"
        print("PASS: string_functions")
    except Exception as e:
        print(f"FAIL: string_functions — {e}")
        failures += 1

    # --- Cleanup ---
    try:
        cursor.execute("DROP TABLE IF EXISTS pymssql_e2e_test")
    except Exception:
        pass

    conn.close()

    # --- Summary ---
    if failures > 0:
        print(f"\n{failures} test(s) FAILED")
        sys.exit(1)
    else:
        print("\nAll tests passed")
        sys.exit(0)


if __name__ == "__main__":
    main()
