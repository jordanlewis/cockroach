// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package e2etest

// This file tests CockroachDB's TDS frontend using pymssql, a Python
// database driver that bundles FreeTDS and speaks native TDS wire protocol.
// pymssql is widely used in the Python ecosystem for connecting to SQL
// Server and Sybase ASE databases, making it an important compatibility
// target for our TDS frontend.
//
// The test starts a CockroachDB cluster with TDS enabled, then invokes a
// Python test script (testdata/pymssql_e2e.py) as a subprocess. The Python
// script runs a series of operations (CRUD, transactions, type handling,
// parameterized queries) and reports results. The Go test validates the
// script exits successfully and checks its output.
//
// Tests skip if python3 or pymssql is not installed.

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestPymssqlE2E runs the pymssql Python test script against CockroachDB's
// TDS frontend. The script exercises connection handling, CRUD operations,
// parameterized queries, transactions, and type handling.
func TestPymssqlE2E(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pythonPath := requireToolInstalled(t, "python3",
		"install Python 3: brew install python3")

	// Verify pymssql is importable.
	out, err := runExternalTool(t, pythonPath,
		[]string{"-c", "import pymssql; print(pymssql.__version__)"},
		"", nil, 10*time.Second)
	if err != nil {
		t.Skipf("pymssql not installed: %v (install: pip3 install pymssql)", err)
	}
	t.Logf("pymssql version: %s", strings.TrimSpace(out))

	env := startTDSTestEnv(t)
	defer env.cleanup()

	// Run the Python test script.
	scriptPath, err := filepath.Abs(filepath.Join("testdata", "pymssql_e2e.py"))
	require.NoError(t, err)
	require.FileExists(t, scriptPath, "Python test script must exist")

	out, err = runExternalTool(t, pythonPath,
		[]string{scriptPath, env.host, env.port},
		"", nil, 60*time.Second)
	t.Logf("pymssql output:\n%s", out)

	if err != nil {
		// Parse output for specific test results before failing.
		if strings.Contains(out, "FAIL:") {
			// Script ran but some tests failed — log failures.
			for _, line := range strings.Split(out, "\n") {
				if strings.HasPrefix(line, "FAIL:") {
					t.Errorf("pymssql: %s", line)
				}
			}
		} else {
			t.Fatalf("pymssql script failed: %v\noutput:\n%s", err, out)
		}
	}

	// Verify key test markers are present in output.
	require.Contains(t, out, "PASS: connect",
		"connection test should pass")

	// Check for unexpected errors (pymssql tracebacks).
	if strings.Contains(out, "Traceback (most recent call last)") {
		// Find and report the traceback.
		lines := strings.Split(out, "\n")
		var tb []string
		var inTB bool
		for _, line := range lines {
			if strings.Contains(line, "Traceback") {
				inTB = true
			}
			if inTB {
				tb = append(tb, line)
			}
		}
		t.Errorf("pymssql script produced a Python traceback:\n%s",
			strings.Join(tb, "\n"))
	}
}

// TestPymssqlConnectionVariants tests different pymssql connection
// scenarios via a simpler inline script.
func TestPymssqlConnectionVariants(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pythonPath := requireToolInstalled(t, "python3",
		"install Python 3: brew install python3")

	// Verify pymssql is importable.
	_, err := runExternalTool(t, pythonPath,
		[]string{"-c", "import pymssql"},
		"", nil, 10*time.Second)
	if err != nil {
		t.Skipf("pymssql not installed: %v", err)
	}

	e := startTDSTestEnv(t)
	defer e.cleanup()

	t.Run("DefaultDatabase", func(t *testing.T) {
		script := pyConnectScript(e.host, e.port, "defaultdb",
			"cursor.execute('SELECT 1 AS val')",
			"row = cursor.fetchone()",
			"assert row[0] == 1, f'expected 1, got {row[0]}'",
			"print('PASS: default_database')",
		)
		out, err := runExternalTool(t, pythonPath,
			[]string{"-c", script}, "", nil, 15*time.Second)
		t.Logf("output: %s", out)
		if err != nil && !strings.Contains(out, "PASS:") {
			t.Fatalf("failed: %v\n%s", err, out)
		}
		require.Contains(t, out, "PASS: default_database")
	})

	t.Run("SelectVersion", func(t *testing.T) {
		script := pyConnectScript(e.host, e.port, "defaultdb",
			"cursor.execute('SELECT @@VERSION')",
			"row = cursor.fetchone()",
			"version = row[0]",
			"assert 'CockroachDB' in version, f'expected CockroachDB in version, got {version}'",
			"print('PASS: select_version')",
		)
		out, err := runExternalTool(t, pythonPath,
			[]string{"-c", script}, "", nil, 15*time.Second)
		t.Logf("output: %s", out)
		if err != nil && !strings.Contains(out, "PASS:") {
			t.Fatalf("failed: %v\n%s", err, out)
		}
		require.Contains(t, out, "PASS: select_version")
	})

	t.Run("MultipleQueries", func(t *testing.T) {
		script := pyConnectScript(e.host, e.port, "defaultdb",
			"cursor.execute('SELECT 1 AS a')",
			"r1 = cursor.fetchone()",
			"cursor.execute('SELECT 2 AS b')",
			"r2 = cursor.fetchone()",
			"cursor.execute('SELECT 3 AS c')",
			"r3 = cursor.fetchone()",
			"assert r1[0] == 1 and r2[0] == 2 and r3[0] == 3",
			"print('PASS: multiple_queries')",
		)
		out, err := runExternalTool(t, pythonPath,
			[]string{"-c", script}, "", nil, 15*time.Second)
		t.Logf("output: %s", out)
		if err != nil && !strings.Contains(out, "PASS:") {
			t.Fatalf("failed: %v\n%s", err, out)
		}
		require.Contains(t, out, "PASS: multiple_queries")
	})
}

// pyConnectScript generates a Python script that connects via pymssql
// and executes the provided lines of Python code.
func pyConnectScript(host, port, database string, lines ...string) string {
	var b strings.Builder
	b.WriteString("import pymssql\n")
	b.WriteString("import os\n")
	// Disable encryption since our test server doesn't support TLS.
	b.WriteString("os.environ['TDSVER'] = '7.3'\n")
	b.WriteString("conn = pymssql.connect(\n")
	b.WriteString("    server='" + host + "',\n")
	b.WriteString("    port='" + port + "',\n")
	b.WriteString("    user='root',\n")
	b.WriteString("    password='',\n")
	b.WriteString("    database='" + database + "',\n")
	b.WriteString("    tds_version='7.3',\n")
	b.WriteString(")\n")
	b.WriteString("cursor = conn.cursor()\n")
	for _, line := range lines {
		b.WriteString(line + "\n")
	}
	b.WriteString("conn.close()\n")
	return b.String()
}
