// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tnswire implements encoding and decoding for Oracle's Transparent
// Network Substrate (TNS) wire protocol packets.
//
// TNS is the network protocol used by Oracle Database clients and servers.
// Each TNS packet begins with an 8-byte header:
//
//	Offset  Size  Field
//	0       2     Packet length (including header)
//	2       2     Packet checksum
//	4       1     Packet type
//	5       1     Reserved byte
//	6       2     Header checksum
//
// The following packet types are defined:
//
//	Type  Value  Description
//	CONNECT   1  Connection request (carries connect string)
//	ACCEPT    2  Connection accepted
//	REFUSE    4  Connection refused
//	REDIRECT  5  Redirect to another address
//	DATA      6  Data transfer (SQL statements and results)
//	MARKER   12  Attention/reset marker
package tnswire
