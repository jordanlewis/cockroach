// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package smoketest_test

import (
	"net"
	"os/exec"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// runCqlsh runs a CQL query via the cqlsh binary and returns the
// stdout and stderr output. It does not fail the test on non-zero
// exit — callers inspect the output to determine success.
func runCqlsh(
	t *testing.T, cqlshPath, host, port, query string,
) (stdout, stderr string, exitErr error) {
	t.Helper()
	cmd := exec.Command(cqlshPath, host, port, "-e", query)
	var out, errOut strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	err := cmd.Run()
	t.Logf("cqlsh -e %q\nstdout: %s\nstderr: %s\nerr: %v",
		query, out.String(), errOut.String(), err)
	return out.String(), errOut.String(), err
}

// TestCqlshConnect verifies that the real cqlsh binary can connect to
// a CockroachDB server with CQL enabled and run basic queries:
// CREATE KEYSPACE, CREATE TABLE, INSERT, and SELECT.
func TestCqlshConnect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Check that cqlsh is available.
	cqlshPath, err := exec.LookPath("cqlsh")
	if err != nil {
		t.Skip("cqlsh not found in PATH; install with: pip install cqlsh")
	}
	t.Logf("using cqlsh at %s", cqlshPath)

	addr, cleanup := startCQLServer(t)
	defer cleanup()

	// Parse host and port from the CQL address.
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	// cqlsh doesn't understand IPv6 bracket notation; use 127.0.0.1.
	if host == "::" || host == "[::]" || host == "" {
		host = "127.0.0.1"
	}

	// Step 1: CREATE KEYSPACE.
	stdout, stderr, err := runCqlsh(t, cqlshPath, host, port,
		"CREATE KEYSPACE cqlsh_test_ks WITH replication = "+
			"{'class': 'SimpleStrategy', 'replication_factor': '1'}")
	if err != nil {
		t.Fatalf("CREATE KEYSPACE failed:\nstdout: %s\nstderr: %s",
			stdout, stderr)
	}

	// Step 2: CREATE TABLE.
	stdout, stderr, err = runCqlsh(t, cqlshPath, host, port,
		"CREATE TABLE cqlsh_test_ks.users "+
			"(id uuid PRIMARY KEY, name text, age int)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed:\nstdout: %s\nstderr: %s",
			stdout, stderr)
	}

	// Step 3: INSERT a row.
	stdout, stderr, err = runCqlsh(t, cqlshPath, host, port,
		"INSERT INTO cqlsh_test_ks.users (id, name, age) VALUES "+
			"(550e8400-e29b-41d4-a716-446655440000, 'Alice', 30)")
	if err != nil {
		t.Fatalf("INSERT failed:\nstdout: %s\nstderr: %s",
			stdout, stderr)
	}

	// Step 4: SELECT the row back.
	stdout, stderr, err = runCqlsh(t, cqlshPath, host, port,
		"SELECT * FROM cqlsh_test_ks.users")
	if err != nil {
		t.Fatalf("SELECT failed:\nstdout: %s\nstderr: %s",
			stdout, stderr)
	}
	require.Contains(t, stdout, "Alice",
		"SELECT output should contain the inserted name")
}
