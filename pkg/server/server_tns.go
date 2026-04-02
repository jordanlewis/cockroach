// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/tns"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// TNSEnabled controls whether the TNS (Oracle wire protocol) server is
// started. When false (the default), no TNS listener is created.
var TNSEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"server.tns.enabled",
	"if true, the server listens for TNS (Oracle) protocol connections",
	false,
)

// TNSPort controls the TCP port on which the TNS server listens.
// The default Oracle TNS port is 1521.
var TNSPort = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"server.tns.port",
	"the TCP port on which the TNS (Oracle) protocol server listens",
	1521,
	settings.NonNegativeInt,
)

// startTNSServer creates and starts the TNS server if the
// server.tns.enabled cluster setting is true. It returns the TNS
// server and the address the listener is bound to.
func startTNSServer(
	ctx context.Context, stopper *stop.Stopper, db isql.DB, sv *settings.Values,
) (*tns.Server, string, error) {
	if !TNSEnabled.Get(sv) {
		return nil, "", nil
	}

	port := TNSPort.Get(sv)
	listenAddr := fmt.Sprintf(":%d", port)

	tnsServer := tns.NewServer(tns.ServerConfig{
		ListenAddr:      listenAddr,
		Insecure:        true, // TODO: wire up CRDB auth
		DefaultDatabase: "defaultdb",
		DB:              db,
	})

	if err := tnsServer.Start(ctx); err != nil {
		return nil, "", errors.Wrap(err, "starting TNS server")
	}

	addr := tnsServer.Addr().String()
	log.Ops.Infof(ctx, "TNS server listening on %s", addr)

	// Register a closer to stop the TNS server on shutdown.
	stopper.AddCloser(stop.CloserFn(func() {
		tnsServer.Stop()
	}))

	return tnsServer, addr, nil
}
