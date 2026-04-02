// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// OracleType represents an Oracle data type name as used in TNS protocol
// interactions and Oracle catalog metadata.
type OracleType int

const (
	// Number is the Oracle NUMBER type, a variable-length decimal type that
	// can represent integers and fixed/floating-point values.
	Number OracleType = iota + 1
	// Varchar2 is the Oracle VARCHAR2 type, a variable-length character string.
	Varchar2
	// Date is the Oracle DATE type. Unlike SQL-standard DATE, Oracle DATE
	// includes both date and time components (year, month, day, hour, minute,
	// second).
	Date
	// Clob is the Oracle CLOB (Character Large Object) type for large text data.
	Clob
	// Blob is the Oracle BLOB (Binary Large Object) type for large binary data.
	Blob
	// Raw is the Oracle RAW type for raw binary data (up to 2000 bytes).
	Raw
	// BinaryFloat is the Oracle BINARY_FLOAT type, a 32-bit IEEE 754
	// floating-point number.
	BinaryFloat
	// BinaryDouble is the Oracle BINARY_DOUBLE type, a 64-bit IEEE 754
	// floating-point number.
	BinaryDouble
	// Rowid is the Oracle ROWID type, a pseudocolumn that uniquely identifies
	// a row in a table. Represented as a base-64 encoded string.
	Rowid
)

// oracleTypeNames maps OracleType constants to their canonical Oracle names.
var oracleTypeNames = map[OracleType]string{
	Number:       "NUMBER",
	Varchar2:     "VARCHAR2",
	Date:         "DATE",
	Clob:         "CLOB",
	Blob:         "BLOB",
	Raw:          "RAW",
	BinaryFloat:  "BINARY_FLOAT",
	BinaryDouble: "BINARY_DOUBLE",
	Rowid:        "ROWID",
}

// String returns the canonical Oracle name for the type.
func (o OracleType) String() string {
	if name, ok := oracleTypeNames[o]; ok {
		return name
	}
	return "UNKNOWN"
}

// oracleToCRDB maps each Oracle type to its corresponding CockroachDB type.
//
// The mapping preserves data fidelity:
//   - NUMBER -> DECIMAL: both are arbitrary-precision decimal types.
//   - VARCHAR2 -> VARCHAR: variable-length character strings.
//   - DATE -> TIMESTAMP: Oracle DATE includes time (HH:MM:SS), unlike
//     SQL-standard DATE which is date-only. TIMESTAMP is the correct target.
//   - CLOB -> TEXT (STRING): unbounded text.
//   - BLOB -> BYTEA (BYTES): unbounded binary.
//   - RAW -> BYTEA (BYTES): fixed-length binary maps to variable-length binary.
//   - BINARY_FLOAT -> FLOAT4: 32-bit IEEE 754.
//   - BINARY_DOUBLE -> FLOAT8 (FLOAT): 64-bit IEEE 754.
//   - ROWID -> STRING: the base-64 encoded row identifier has no native
//     CockroachDB equivalent; STRING preserves the value.
var oracleToCRDB = map[OracleType]*types.T{
	Number:       types.Decimal,
	Varchar2:     types.VarChar,
	Date:         types.Timestamp,
	Clob:         types.String,
	Blob:         types.Bytes,
	Raw:          types.Bytes,
	BinaryFloat:  types.Float4,
	BinaryDouble: types.Float,
	Rowid:        types.String,
}

// nameToOracleType maps uppercase Oracle type name strings to OracleType
// constants. Built from oracleTypeNames for a single source of truth.
var nameToOracleType map[string]OracleType

func init() {
	nameToOracleType = make(map[string]OracleType, len(oracleTypeNames))
	for ot, name := range oracleTypeNames {
		nameToOracleType[name] = ot
	}
}

// OracleTypeToCRDB returns the CockroachDB type corresponding to the given
// Oracle type.
func OracleTypeToCRDB(o OracleType) (*types.T, error) {
	if t, ok := oracleToCRDB[o]; ok {
		return t, nil
	}
	return nil, errors.Newf("unsupported Oracle type: %s", o)
}

// OracleTypeFromName parses an Oracle type name string (case-insensitive) and
// returns the corresponding OracleType. Returns an error if the name is not
// recognized.
func OracleTypeFromName(name string) (OracleType, error) {
	if ot, ok := nameToOracleType[strings.ToUpper(name)]; ok {
		return ot, nil
	}
	return 0, errors.Newf("unknown Oracle type name: %q", name)
}

// MapOracleTypeName is a convenience function that maps an Oracle type name
// string directly to the corresponding CockroachDB type. It combines
// OracleTypeFromName and OracleTypeToCRDB.
func MapOracleTypeName(name string) (*types.T, error) {
	ot, err := OracleTypeFromName(name)
	if err != nil {
		return nil, err
	}
	return OracleTypeToCRDB(ot)
}
