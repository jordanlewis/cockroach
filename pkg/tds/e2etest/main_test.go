// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package e2etest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/tds"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	m.Run()
}

// tdsTestEnv holds a running CockroachDB+TDS server for e2e tests.
type tdsTestEnv struct {
	host    string
	port    string
	cleanup func()
}

// startTDSTestEnv starts a CockroachDB test server with a TDS frontend
// and returns the host, port, and a cleanup function.
func startTDSTestEnv(t *testing.T) *tdsTestEnv {
	t.Helper()

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	internalDB := srv.ApplicationLayer().InternalDB().(isql.DB)

	tdsServer := tds.NewServer(tds.ServerConfig{
		ListenAddr:      "127.0.0.1:0",
		DefaultDatabase: "defaultdb",
		DB:              internalDB,
	})
	require.NoError(t, tdsServer.Start(ctx))

	addr := tdsServer.Addr().String()
	parts := strings.Split(addr, ":")
	require.Len(t, parts, 2, "expected host:port, got %s", addr)

	return &tdsTestEnv{
		host: parts[0],
		port: parts[1],
		cleanup: func() {
			tdsServer.Stop()
			srv.Stopper().Stop(ctx)
		},
	}
}

// writeFreeTDSConf creates a temporary freetds.conf file pointing at the
// test TDS server. This is required for FreeTDS tools like bsqldb that
// use server names rather than host:port directly.
func writeFreeTDSConf(t *testing.T, host, port string) (confPath string) {
	t.Helper()

	dir := t.TempDir()
	confPath = filepath.Join(dir, "freetds.conf")
	conf := fmt.Sprintf(`[global]
	tds version = 7.3
	text size = 64512

[testserver]
	host = %s
	port = %s
	tds version = 7.3
`, host, port)
	require.NoError(t, os.WriteFile(confPath, []byte(conf), 0644))
	return confPath
}

// runExternalTool runs an external command with a timeout, returning its
// combined output. The caller can pass environment variables to override
// the process environment.
func runExternalTool(
	t *testing.T, name string, args []string, stdin string, env []string, timeout time.Duration,
) (string, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}

	out, err := cmd.CombinedOutput()
	return string(out), err
}

// requireToolInstalled checks that a binary is available in PATH and
// skips the test if not. Returns the path to the binary.
func requireToolInstalled(t *testing.T, name, installHint string) string {
	t.Helper()
	path, err := exec.LookPath(name)
	if err != nil {
		t.Skipf("%s not found in PATH; %s", name, installHint)
	}
	return path
}
