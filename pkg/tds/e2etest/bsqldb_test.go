// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package e2etest

// This file tests CockroachDB's TDS frontend using FreeTDS's bsqldb, a
// non-interactive batch SQL processor. Unlike tsql (tested in smoketest/),
// bsqldb is designed for scripted batch execution: it reads SQL from stdin
// or a file, executes each batch (delimited by "go"), and writes results
// to stdout in a tab-separated format. This makes it ideal for testing
// multi-statement workflows and verifying output programmatically.
//
// bsqldb uses server names from freetds.conf, so each test creates a
// temporary config pointing at the test TDS server.
//
// Tests skip if bsqldb is not installed (brew install freetds).

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestBsqldbE2E runs end-to-end tests using FreeTDS bsqldb against
// CockroachDB's TDS frontend. Each subtest feeds a SQL script to bsqldb
// and validates the output.
func TestBsqldbE2E(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	bsqldbPath := requireToolInstalled(t, "bsqldb",
		"install FreeTDS: brew install freetds")
	t.Logf("using bsqldb at %s", bsqldbPath)

	env := startTDSTestEnv(t)
	defer env.cleanup()

	confPath := writeFreeTDSConf(t, env.host, env.port)

	bsqldbEnv := []string{"FREETDSCONF=" + confPath}
	bsqldbArgs := []string{"-S", "testserver", "-U", "root", "-P", "", "-D", "defaultdb"}

	runBsqldb := func(t *testing.T, script string) string {
		t.Helper()
		out, err := runExternalTool(t, bsqldbPath,
			bsqldbArgs, script, bsqldbEnv, 30*time.Second,
		)
		if err != nil {
			// bsqldb may return non-zero on SQL errors but still produce
			// useful output. Log the error but don't necessarily fail.
			t.Logf("bsqldb returned error: %v\noutput:\n%s", err, out)
		}
		return out
	}

	t.Run("BasicCRUD", func(t *testing.T) {
		script, err := os.ReadFile(filepath.Join("testdata", "bsqldb_crud.sql"))
		require.NoError(t, err)

		out := runBsqldb(t, string(script))
		t.Logf("bsqldb CRUD output:\n%s", out)

		// Verify SELECT results appear in output.
		require.Contains(t, out, "Alice",
			"SELECT should return inserted name 'Alice'")
		require.Contains(t, out, "Bob",
			"SELECT should return inserted name 'Bob'")
	})

	t.Run("DataTypes", func(t *testing.T) {
		script, err := os.ReadFile(filepath.Join("testdata", "bsqldb_datatypes.sql"))
		require.NoError(t, err)

		out := runBsqldb(t, string(script))
		t.Logf("bsqldb datatypes output:\n%s", out)

		// The script creates a table with various types, inserts data,
		// and selects it back. We verify some values appear.
		require.Contains(t, out, "hello world",
			"VARCHAR value should appear in output")
		require.Contains(t, out, "42",
			"INT value should appear in output")
	})

	t.Run("Transactions", func(t *testing.T) {
		script, err := os.ReadFile(filepath.Join("testdata", "bsqldb_transactions.sql"))
		require.NoError(t, err)

		out := runBsqldb(t, string(script))
		t.Logf("bsqldb transactions output:\n%s", out)

		// After ROLLBACK the data inserted in the rolled-back transaction
		// should not appear, but committed data should.
		require.Contains(t, out, "committed_row",
			"committed row should be visible")
		// The rolled-back row should not appear in the final SELECT.
		lines := strings.Split(out, "\n")
		var finalSelectSeen bool
		for _, line := range lines {
			if strings.Contains(line, "final_select_marker") {
				finalSelectSeen = true
			}
			if finalSelectSeen && strings.Contains(line, "rolled_back_row") {
				t.Error("rolled-back row should not appear after final SELECT")
			}
		}
	})

	t.Run("SystemQueries", func(t *testing.T) {
		script, err := os.ReadFile(filepath.Join("testdata", "bsqldb_system.sql"))
		require.NoError(t, err)

		out := runBsqldb(t, string(script))
		t.Logf("bsqldb system queries output:\n%s", out)

		// @@VERSION should return CockroachDB.
		require.Contains(t, out, "CockroachDB",
			"@@VERSION should contain CockroachDB")
	})
}
