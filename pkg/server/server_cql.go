// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"net"

	"github.com/cockroachdb/cockroach/pkg/cql"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// CQLEnabled controls whether the CQL native protocol server is
// started. When false (the default), no CQL listener is created.
var CQLEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"server.cql.enabled",
	"if true, a CQL native protocol (v4) listener is started "+
		"alongside the SQL server",
	false,
	settings.WithPublic,
)

// CQLPort controls the TCP port on which the CQL server listens.
// Set to 0 to bind to a random available port (useful for tests).
var CQLPort = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"server.cql.port",
	"the TCP port for the CQL native protocol listener; "+
		"0 selects a random available port",
	9042,
	settings.NonNegativeInt,
	settings.WithPublic,
)

// startCQLServer creates and starts the CQL server if the
// server.cql.enabled cluster setting is true. It opens a TCP
// listener on the configured port and begins accepting CQL
// connections in a background goroutine. The server is wired into
// the stopper for graceful shutdown.
//
// It returns the CQL server and the address the listener is bound
// to. If CQL is not enabled, both return values are zero values.
func startCQLServer(
	ctx context.Context,
	ambientCtx log.AmbientContext,
	stopper *stop.Stopper,
	insecure bool,
	db isql.DB,
	sv *settings.Values,
) (*cql.Server, string, error) {
	if !CQLEnabled.Get(sv) {
		return nil, "", nil
	}

	port := CQLPort.Get(sv)
	addr := fmt.Sprintf(":%d", port)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, "", err
	}

	cqlServer := cql.MakeServer(cql.ServerConfig{
		AmbientCtx: ambientCtx,
		Insecure:   insecure,
	}, db)

	listenAddr := ln.Addr().String()
	log.Ops.Infof(ctx, "CQL server listening on %s", listenAddr)

	// Wire the listener into the stopper so it is closed on
	// shutdown.
	_ = stopper.RunAsyncTask(
		ctx, "cql-quiesce", func(ctx context.Context) {
			<-stopper.ShouldQuiesce()
			if err := ln.Close(); err != nil {
				log.Ops.Errorf(
					ctx, "closing CQL listener: %v", err,
				)
			}
		},
	)

	// Start accepting CQL connections.
	if err := stopper.RunAsyncTask(
		ctx, "cql-serve", func(ctx context.Context) {
			_ = cqlServer.Serve(ctx, stopper, ln)
		},
	); err != nil {
		_ = ln.Close()
		return nil, "", err
	}

	return cqlServer, listenAddr, nil
}
