// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package types provides CQL (Cassandra Query Language) type definitions and
// bidirectional mappings between CQL types and CockroachDB's internal type
// system. It also provides binary encoding of CRDB datum values into the CQL
// native protocol wire format used in RESULT Rows messages.
//
// CQL types map to CockroachDB types as follows:
//
//	CQL text/varchar  -> types.String
//	CQL int           -> types.Int4
//	CQL bigint        -> types.Int       (64-bit)
//	CQL float         -> types.Float4
//	CQL double        -> types.Float
//	CQL boolean       -> types.Bool
//	CQL timestamp     -> types.TimestampTZ
//	CQL uuid          -> types.Uuid
//	CQL timeuuid      -> types.Uuid      (version-1 validation deferred)
//	CQL blob          -> types.Bytes
//	CQL inet          -> types.INet
//	CQL counter       -> types.Int       (increment semantics deferred)
package types

import (
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// CQLType represents a CQL data type option ID as defined in the Cassandra
// native protocol v4 specification. These 16-bit identifiers appear in
// RESULT Rows metadata to describe column types.
type CQLType uint16

const (
	CQLCustom    CQLType = 0x0000
	CQLAscii     CQLType = 0x0001
	CQLBigint    CQLType = 0x0002
	CQLBlob      CQLType = 0x0003
	CQLBoolean   CQLType = 0x0004
	CQLCounter   CQLType = 0x0005
	CQLDecimal   CQLType = 0x0006
	CQLDouble    CQLType = 0x0007
	CQLFloat     CQLType = 0x0008
	CQLInt       CQLType = 0x0009
	CQLTimestamp CQLType = 0x000B
	CQLUuid      CQLType = 0x000C
	CQLVarchar   CQLType = 0x000D
	CQLVarint    CQLType = 0x000E
	CQLTimeuuid  CQLType = 0x000F
	CQLInet      CQLType = 0x0010
	CQLDate      CQLType = 0x0011
	CQLTime      CQLType = 0x0012
	CQLSmallint  CQLType = 0x0013
	CQLTinyint   CQLType = 0x0014
	CQLDuration  CQLType = 0x0015
	CQLList      CQLType = 0x0020
	CQLMap       CQLType = 0x0021
	CQLSet       CQLType = 0x0022
	CQLTuple     CQLType = 0x0031
	CQLText      CQLType = CQLVarchar // text and varchar are identical in CQL
)

// String returns the CQL type name.
func (t CQLType) String() string {
	switch t {
	case CQLAscii:
		return "ascii"
	case CQLBigint:
		return "bigint"
	case CQLBlob:
		return "blob"
	case CQLBoolean:
		return "boolean"
	case CQLCounter:
		return "counter"
	case CQLDecimal:
		return "decimal"
	case CQLDouble:
		return "double"
	case CQLFloat:
		return "float"
	case CQLInt:
		return "int"
	case CQLTimestamp:
		return "timestamp"
	case CQLUuid:
		return "uuid"
	case CQLVarchar:
		return "varchar"
	case CQLVarint:
		return "varint"
	case CQLTimeuuid:
		return "timeuuid"
	case CQLInet:
		return "inet"
	case CQLDate:
		return "date"
	case CQLTime:
		return "time"
	case CQLSmallint:
		return "smallint"
	case CQLTinyint:
		return "tinyint"
	case CQLDuration:
		return "duration"
	case CQLList:
		return "list"
	case CQLMap:
		return "map"
	case CQLSet:
		return "set"
	case CQLTuple:
		return "tuple"
	case CQLCustom:
		return "custom"
	default:
		return "unknown"
	}
}

// CRDBType returns the CockroachDB type corresponding to this CQL type.
func (t CQLType) CRDBType() (*types.T, error) {
	switch t {
	case CQLVarchar, CQLAscii: // CQLText == CQLVarchar
		return types.String, nil
	case CQLInt:
		return types.Int4, nil
	case CQLSmallint, CQLTinyint:
		return types.Int2, nil
	case CQLBigint, CQLCounter:
		return types.Int, nil
	case CQLFloat:
		return types.Float4, nil
	case CQLDouble:
		return types.Float, nil
	case CQLBoolean:
		return types.Bool, nil
	case CQLTimestamp:
		return types.TimestampTZ, nil
	case CQLUuid, CQLTimeuuid:
		return types.Uuid, nil
	case CQLBlob:
		return types.Bytes, nil
	case CQLDate:
		return types.Date, nil
	case CQLTime:
		return types.Time, nil
	case CQLDuration:
		return types.Interval, nil
	case CQLInet:
		return types.INet, nil
	case CQLDecimal:
		return types.Decimal, nil
	case CQLVarint:
		return types.Int, nil
	case CQLTuple, CQLList, CQLMap, CQLSet:
		return types.Jsonb, nil
	default:
		return nil, errors.Newf("unsupported CQL type: %s (0x%04x)", t, uint16(t))
	}
}

// CQLTypeFromCRDB returns the CQL type that best represents the given
// CockroachDB type. When multiple CQL types could apply (e.g. bigint vs
// counter for 64-bit integers), the non-special variant is returned.
func CQLTypeFromCRDB(t *types.T) (CQLType, error) {
	switch t.Family() {
	case types.BoolFamily:
		return CQLBoolean, nil
	case types.IntFamily:
		switch t.Width() {
		case 16:
			return CQLSmallint, nil
		case 32:
			return CQLInt, nil
		default:
			return CQLBigint, nil
		}
	case types.FloatFamily:
		if t.Width() <= 32 {
			return CQLFloat, nil
		}
		return CQLDouble, nil
	case types.StringFamily:
		return CQLVarchar, nil
	case types.BytesFamily:
		return CQLBlob, nil
	case types.TimestampTZFamily, types.TimestampFamily:
		return CQLTimestamp, nil
	case types.DateFamily:
		return CQLDate, nil
	case types.TimeFamily:
		return CQLTime, nil
	case types.IntervalFamily:
		return CQLDuration, nil
	case types.UuidFamily:
		return CQLUuid, nil
	case types.INetFamily:
		return CQLInet, nil
	case types.JsonFamily:
		// JSONB columns are used to store CQL collection types (list,
		// set, map). On the wire they are encoded as varchar (JSON text).
		return CQLVarchar, nil
	case types.DecimalFamily:
		return CQLDecimal, nil
	default:
		return 0, errors.Newf(
			"no CQL type mapping for CRDB type %s (family %d)", t.SQLString(), t.Family(),
		)
	}
}
