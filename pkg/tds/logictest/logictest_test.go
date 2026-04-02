// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logictest

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	m.Run()
}

// TestLogic walks testdata/ and runs each file as a TDS logic test.
func TestLogic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Pick a free port for the TDS server.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tdsPort := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())

	// Start CockroachDB with TDS enabled.
	st := cluster.MakeTestingClusterSettings()
	enabledSetting, ok, _ := settings.LookupForLocalAccess(
		"server.tds.enabled", true, /* forSystemTenant */
	)
	require.True(t, ok)
	enabledSetting.(*settings.BoolSetting).Override(ctx, &st.SV, true)

	portSetting, ok, _ := settings.LookupForLocalAccess(
		"server.tds.port", true, /* forSystemTenant */
	)
	require.True(t, ok)
	portSetting.(*settings.IntSetting).Override(ctx, &st.SV, int64(tdsPort))

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Settings: st,
		// The TDS server binds to a specific port. The shared process tenant
		// would try to bind to the same port, causing a conflict.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)

	tdsAddr := fmt.Sprintf("127.0.0.1:%d", tdsPort)

	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		runner := NewRunner(t, tdsAddr)
		defer runner.Close()
		runner.Run(t, path)
	})
}
