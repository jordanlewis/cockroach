// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tds

import (
	"github.com/cockroachdb/cockroach/pkg/tds/tdswire"
)

// authenticate checks the LOGIN7 credentials against the server
// configuration. If the configured username or password is empty, that
// field is not checked (allowing any value). This supports simple
// password-based authentication; more sophisticated auth (e.g. NTLM,
// Kerberos) can be added later.
func authenticate(cfg ServerConfig, login *tdswire.Login7) bool {
	if cfg.Username != "" && login.Username != cfg.Username {
		return false
	}
	if cfg.Password != "" && login.Password != cfg.Password {
		return false
	}
	return true
}
