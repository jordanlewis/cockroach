// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cqlwire

import "github.com/cockroachdb/redact"

// ProtocolVersion identifies the CQL native protocol version.
type ProtocolVersion byte

const (
	// ProtoV4Request is the version byte sent by clients in CQL v4 requests.
	ProtoV4Request ProtocolVersion = 0x04
	// ProtoV4Response is the version byte sent by servers in CQL v4 responses.
	ProtoV4Response ProtocolVersion = 0x84
)

var _ redact.SafeValue = ProtocolVersion(0)

// SafeValue implements the redact.SafeValue interface.
func (v ProtocolVersion) SafeValue() {}

// IsRequest returns true if the version byte indicates a request frame.
func (v ProtocolVersion) IsRequest() bool {
	return v&0x80 == 0
}

// IsResponse returns true if the version byte indicates a response frame.
func (v ProtocolVersion) IsResponse() bool {
	return v&0x80 != 0
}

// HeaderFlag represents flags in the CQL frame header.
type HeaderFlag byte

const (
	// FlagCompressed indicates the frame body is compressed.
	FlagCompressed HeaderFlag = 0x01
	// FlagTracing indicates tracing was requested.
	FlagTracing HeaderFlag = 0x02
	// FlagCustomPayload indicates a custom payload is present.
	FlagCustomPayload HeaderFlag = 0x04
	// FlagWarning indicates warnings are present in the frame.
	FlagWarning HeaderFlag = 0x08
)

var _ redact.SafeValue = HeaderFlag(0)

// SafeValue implements the redact.SafeValue interface.
func (f HeaderFlag) SafeValue() {}

// Opcode identifies the type of a CQL frame.
type Opcode byte

var _ redact.SafeValue = Opcode(0)

// SafeValue implements the redact.SafeValue interface.
func (o Opcode) SafeValue() {}

// Request opcodes.
const (
	OpStartup  Opcode = 0x01
	OpOptions  Opcode = 0x05
	OpQuery    Opcode = 0x07
	OpPrepare  Opcode = 0x09
	OpExecute  Opcode = 0x0A
	OpRegister Opcode = 0x0B
)

// Response opcodes.
const (
	OpError        Opcode = 0x00
	OpReady        Opcode = 0x02
	OpAuthenticate Opcode = 0x03
	OpSupported    Opcode = 0x06
	OpResult       Opcode = 0x08
	OpEvent        Opcode = 0x0C
)

// String returns a human-readable name for the opcode.
func (o Opcode) String() string {
	switch o {
	case OpStartup:
		return "STARTUP"
	case OpOptions:
		return "OPTIONS"
	case OpQuery:
		return "QUERY"
	case OpPrepare:
		return "PREPARE"
	case OpExecute:
		return "EXECUTE"
	case OpRegister:
		return "REGISTER"
	case OpError:
		return "ERROR"
	case OpReady:
		return "READY"
	case OpAuthenticate:
		return "AUTHENTICATE"
	case OpSupported:
		return "SUPPORTED"
	case OpResult:
		return "RESULT"
	case OpEvent:
		return "EVENT"
	default:
		return "UNKNOWN"
	}
}

// IsRequest returns true if the opcode is a request opcode.
func (o Opcode) IsRequest() bool {
	switch o {
	case OpStartup, OpOptions, OpQuery, OpPrepare, OpExecute, OpRegister:
		return true
	default:
		return false
	}
}

// Consistency represents a CQL consistency level, encoded as a [short].
type Consistency uint16

var _ redact.SafeValue = Consistency(0)

// SafeValue implements the redact.SafeValue interface.
func (c Consistency) SafeValue() {}

const (
	ConsistencyAny         Consistency = 0x0000
	ConsistencyOne         Consistency = 0x0001
	ConsistencyTwo         Consistency = 0x0002
	ConsistencyThree       Consistency = 0x0003
	ConsistencyQuorum      Consistency = 0x0004
	ConsistencyAll         Consistency = 0x0005
	ConsistencyLocalQuorum Consistency = 0x0006
	ConsistencyEachQuorum  Consistency = 0x0007
	ConsistencySerial      Consistency = 0x0008
	ConsistencyLocalSerial Consistency = 0x0009
	ConsistencyLocalOne    Consistency = 0x000A
)

// String returns a human-readable name for the consistency level.
func (c Consistency) String() string {
	switch c {
	case ConsistencyAny:
		return "ANY"
	case ConsistencyOne:
		return "ONE"
	case ConsistencyTwo:
		return "TWO"
	case ConsistencyThree:
		return "THREE"
	case ConsistencyQuorum:
		return "QUORUM"
	case ConsistencyAll:
		return "ALL"
	case ConsistencyLocalQuorum:
		return "LOCAL_QUORUM"
	case ConsistencyEachQuorum:
		return "EACH_QUORUM"
	case ConsistencySerial:
		return "SERIAL"
	case ConsistencyLocalSerial:
		return "LOCAL_SERIAL"
	case ConsistencyLocalOne:
		return "LOCAL_ONE"
	default:
		return "UNKNOWN"
	}
}

// HeaderSize is the fixed size of a CQL v4 frame header in bytes.
const HeaderSize = 9
