// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tdswire

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"unicode/utf16"
)

// TDS token type constants as defined by the TDS protocol specification.
// These identify the type of each token in the token stream.
const (
	TokenReturnStatus byte = 0x79
	TokenColMetaData  byte = 0x81
	TokenColInfo      byte = 0xA5
	TokenOrder        byte = 0xA9
	TokenError        byte = 0xAA
	TokenInfo         byte = 0xAB
	TokenLoginAck     byte = 0xAD
	TokenRow          byte = 0xD1
	TokenEnvChange    byte = 0xE3
	TokenDone         byte = 0xFD
	TokenDoneProc     byte = 0xFE
	TokenDoneInProc   byte = 0xFF
)

// Done status flags indicate the state of the completed operation.
const (
	DoneFinal uint16 = 0x00
	DoneMore  uint16 = 0x01
	DoneError uint16 = 0x02
	DoneCount uint16 = 0x10
)

// EnvChange type constants identify what environment property changed.
const (
	EnvDatabase   byte = 1
	EnvLanguage   byte = 2
	EnvCharset    byte = 3
	EnvPacketSize byte = 4
)

// TypeID constants for TDS data types. Fixed-length types have a known
// size; variable-length types require an accompanying maximum length.
const (
	// Fixed-length types.
	TypeInt1     byte = 0x30 // tinyint, 1 byte
	TypeBit      byte = 0x32 // bit, 1 byte
	TypeInt2     byte = 0x34 // smallint, 2 bytes
	TypeInt4     byte = 0x38 // int, 4 bytes
	TypeFloat4   byte = 0x3B // real, 4 bytes
	TypeFloat8   byte = 0x3E // float, 8 bytes
	TypeDateTime byte = 0x3D // datetime, 8 bytes
	TypeInt8     byte = 0x7F // bigint, 8 bytes

	// Variable-length types.
	TypeBigVarChar   byte = 0xA7 // varchar
	TypeBigChar      byte = 0xAF // char
	TypeBigVarBin    byte = 0xA5 // varbinary
	TypeNVarChar     byte = 0xE7 // nvarchar
	TypeNChar        byte = 0xEF // nchar
	TypeBigBinary    byte = 0xAD // binary
	TypeDecimalN     byte = 0x6A // decimal
	TypeNumericN     byte = 0x6C // numeric
	TypeIntN         byte = 0x26 // nullable int
	TypeBitN         byte = 0x68 // nullable bit
	TypeFloatN       byte = 0x6D // nullable float
	TypeDateTimeN    byte = 0x6F // nullable datetime
	TypeMoney4       byte = 0x7A // smallmoney
	TypeMoney8       byte = 0x3C // money
	TypeMoneyN       byte = 0x6E // nullable money
	TypeGuid         byte = 0x24 // uniqueidentifier
	TypeDateTimeOffN byte = 0x2B // datetimeoffset
)

// fixedTypeLen returns the byte length of a fixed-length type, or 0 if
// the type is not fixed-length.
func fixedTypeLen(typeID byte) int {
	switch typeID {
	case TypeInt1, TypeBit:
		return 1
	case TypeInt2:
		return 2
	case TypeInt4, TypeFloat4:
		return 4
	case TypeFloat8, TypeInt8, TypeDateTime:
		return 8
	default:
		return 0
	}
}

// isVariableLenType reports whether typeID is a variable-length type
// that uses a 2-byte length prefix for its maximum length in metadata.
func isVariableLenType(typeID byte) bool {
	switch typeID {
	case TypeBigVarChar, TypeBigChar, TypeBigVarBin,
		TypeNVarChar, TypeNChar, TypeBigBinary:
		return true
	default:
		return false
	}
}

// isPrecisionScaleType reports whether typeID requires precision and
// scale fields in addition to its length.
func isPrecisionScaleType(typeID byte) bool {
	return typeID == TypeDecimalN || typeID == TypeNumericN
}

// isByteLenType reports whether typeID is a variable-length type that
// uses a 1-byte length prefix for its maximum length in metadata.
func isByteLenType(typeID byte) bool {
	switch typeID {
	case TypeIntN, TypeBitN, TypeFloatN, TypeDateTimeN,
		TypeMoneyN, TypeGuid, TypeDateTimeOffN:
		return true
	default:
		return false
	}
}

// TypeInfo describes the type of a column, including its size and
// optional precision/scale for decimal types.
type TypeInfo struct {
	TypeID    byte
	MaxLen    uint16 // For variable-length types.
	ByteLen   byte   // For byte-length types (IntN, etc.).
	Precision byte   // For decimal/numeric.
	Scale     byte   // For decimal/numeric.
}

// ColMetaData describes the columns in a result set. Each Column entry
// carries the user type, flags, type information, and column name.
type ColMetaData struct {
	Columns []Column
}

// Column holds metadata for a single column in a result set.
type Column struct {
	UserType uint32
	Flags    uint16
	TypeInfo TypeInfo
	ColName  string
}

// Row holds the encoded values for a single data row. The values
// correspond positionally to the columns described by ColMetaData.
// A nil entry in Values represents a SQL NULL.
type Row struct {
	Values [][]byte
}

// DoneToken represents a DONE, DONEPROC, or DONEINPROC token. It
// signals completion of a SQL statement or batch.
type DoneToken struct {
	TokenType byte // TokenDone, TokenDoneProc, or TokenDoneInProc.
	Status    uint16
	CurCmd    uint16
	RowCount  uint64
}

// ErrorToken represents a TDS ERROR or INFO token. Both share the same
// wire format; the token type byte distinguishes them.
type ErrorToken struct {
	TokenType byte // TokenError or TokenInfo.
	Number    int32
	State     uint8
	Class     uint8
	Message   string
	Server    string
	Proc      string
	Line      int32
}

// EnvChangeToken represents a TDS ENVCHANGE token that notifies the
// client of a server-side environment change (database, language, etc.).
type EnvChangeToken struct {
	Type     byte
	NewValue string
	OldValue string
}

// LoginAckToken represents the server's acknowledgment of a successful
// login, including the negotiated TDS version and server program name.
type LoginAckToken struct {
	Interface   byte
	TDSVersion  uint32
	ProgName    string
	ProgVersion [4]byte
}

// ReturnStatusToken holds the return value from a stored procedure.
type ReturnStatusToken struct {
	Value int32
}

// OrderToken specifies the column ordering of a result set.
type OrderToken struct {
	Columns []uint16
}

// ColInfoToken provides additional column information.
type ColInfoToken struct {
	Data []byte
}

// TokenWriter writes TDS tokens to an underlying io.Writer using
// little-endian byte order as required by the TDS protocol.
type TokenWriter struct {
	w   io.Writer
	buf []byte
}

// NewTokenWriter creates a TokenWriter that writes to w.
func NewTokenWriter(w io.Writer) *TokenWriter {
	return &TokenWriter{w: w, buf: make([]byte, 8)}
}

func (tw *TokenWriter) writeU8(v byte) error {
	tw.buf[0] = v
	_, err := tw.w.Write(tw.buf[:1])
	return err
}

func (tw *TokenWriter) writeU16(v uint16) error {
	binary.LittleEndian.PutUint16(tw.buf, v)
	_, err := tw.w.Write(tw.buf[:2])
	return err
}

func (tw *TokenWriter) writeU32(v uint32) error {
	binary.LittleEndian.PutUint32(tw.buf, v)
	_, err := tw.w.Write(tw.buf[:4])
	return err
}

func (tw *TokenWriter) writeU32BE(v uint32) error {
	binary.BigEndian.PutUint32(tw.buf, v)
	_, err := tw.w.Write(tw.buf[:4])
	return err
}

func (tw *TokenWriter) writeI32(v int32) error {
	binary.LittleEndian.PutUint32(tw.buf, uint32(v))
	_, err := tw.w.Write(tw.buf[:4])
	return err
}

func (tw *TokenWriter) writeU64(v uint64) error {
	binary.LittleEndian.PutUint64(tw.buf, v)
	_, err := tw.w.Write(tw.buf[:8])
	return err
}

func (tw *TokenWriter) writeBytes(b []byte) error {
	_, err := tw.w.Write(b)
	return err
}

// writeBVarchar writes a string using the TDS 7.x B_VARCHAR format:
// a 1-byte length prefix (in UCS-2 characters) followed by UTF-16LE
// encoded string data.
func (tw *TokenWriter) writeBVarchar(s string) error {
	u16 := utf16.Encode([]rune(s))
	if len(u16) > math.MaxUint8 {
		return fmt.Errorf("BVarchar string too long: %d chars", len(u16))
	}
	if err := tw.writeU8(byte(len(u16))); err != nil {
		return err
	}
	return tw.writeUTF16LE(u16)
}

// writeUsVarchar writes a string using the TDS 7.x US_VARCHAR format:
// a 2-byte length prefix (in UCS-2 characters) followed by UTF-16LE
// encoded string data.
func (tw *TokenWriter) writeUsVarchar(s string) error {
	u16 := utf16.Encode([]rune(s))
	if len(u16) > math.MaxUint16 {
		return fmt.Errorf("UsVarchar string too long: %d chars", len(u16))
	}
	if err := tw.writeU16(uint16(len(u16))); err != nil {
		return err
	}
	return tw.writeUTF16LE(u16)
}

// writeUTF16LE writes a slice of UTF-16 code units as little-endian bytes.
func (tw *TokenWriter) writeUTF16LE(u16 []uint16) error {
	b := make([]byte, len(u16)*2)
	for i, v := range u16 {
		binary.LittleEndian.PutUint16(b[i*2:i*2+2], v)
	}
	return tw.writeBytes(b)
}

// collationSize is the size of the collation field appended after the
// max-length for string types in COLMETADATA (5 bytes: LCID + sort flags).
const collationSize = 5

// defaultCollation is the default SQL collation bytes: LCID 0x0409
// (us_english) with no sort flags, sort ID 52 (binary sort).
var defaultCollation = [collationSize]byte{0x09, 0x04, 0x00, 0x00, 0x34}

// isStringType reports whether typeID is a character-based type that
// requires collation data in COLMETADATA.
func isStringType(typeID byte) bool {
	switch typeID {
	case TypeBigVarChar, TypeBigChar, TypeNVarChar, TypeNChar:
		return true
	default:
		return false
	}
}

// writeTypeInfo encodes a TypeInfo to the wire.
func (tw *TokenWriter) writeTypeInfo(ti TypeInfo) error {
	if err := tw.writeU8(ti.TypeID); err != nil {
		return err
	}
	if fixedTypeLen(ti.TypeID) > 0 {
		// Fixed-length types only need the type ID.
		return nil
	}
	if isByteLenType(ti.TypeID) {
		return tw.writeU8(ti.ByteLen)
	}
	if isVariableLenType(ti.TypeID) {
		if err := tw.writeU16(ti.MaxLen); err != nil {
			return err
		}
		// String types require 5 bytes of collation data.
		if isStringType(ti.TypeID) {
			if err := tw.writeBytes(defaultCollation[:]); err != nil {
				return err
			}
		}
		return nil
	}
	if isPrecisionScaleType(ti.TypeID) {
		if err := tw.writeU8(ti.ByteLen); err != nil {
			return err
		}
		if err := tw.writeU8(ti.Precision); err != nil {
			return err
		}
		return tw.writeU8(ti.Scale)
	}
	return fmt.Errorf("unsupported type ID: 0x%02X", ti.TypeID)
}

// WriteColMetaData writes a COLMETADATA token.
func (tw *TokenWriter) WriteColMetaData(md ColMetaData) error {
	if err := tw.writeU8(TokenColMetaData); err != nil {
		return err
	}
	if err := tw.writeU16(uint16(len(md.Columns))); err != nil {
		return err
	}
	for _, col := range md.Columns {
		if err := tw.writeU32(col.UserType); err != nil {
			return err
		}
		if err := tw.writeU16(col.Flags); err != nil {
			return err
		}
		if err := tw.writeTypeInfo(col.TypeInfo); err != nil {
			return err
		}
		if err := tw.writeBVarchar(col.ColName); err != nil {
			return err
		}
	}
	return nil
}

// writeRowValue writes a single column value for a ROW token. The
// encoding depends on the type described by ti.
func (tw *TokenWriter) writeRowValue(ti TypeInfo, val []byte) error {
	n := fixedTypeLen(ti.TypeID)
	if n > 0 {
		// Fixed-length types: write the raw bytes directly.
		if len(val) != n {
			return fmt.Errorf(
				"fixed-length type 0x%02X expects %d bytes, got %d",
				ti.TypeID, n, len(val),
			)
		}
		return tw.writeBytes(val)
	}
	if isByteLenType(ti.TypeID) {
		if val == nil {
			// NULL sentinel for 1-byte length prefix types.
			return tw.writeU8(0)
		}
		if err := tw.writeU8(byte(len(val))); err != nil {
			return err
		}
		return tw.writeBytes(val)
	}
	if isVariableLenType(ti.TypeID) {
		if val == nil {
			// NULL sentinel for 2-byte length prefix types.
			return tw.writeU16(0xFFFF)
		}
		if err := tw.writeU16(uint16(len(val))); err != nil {
			return err
		}
		return tw.writeBytes(val)
	}
	if isPrecisionScaleType(ti.TypeID) {
		if val == nil {
			return tw.writeU8(0)
		}
		if err := tw.writeU8(byte(len(val))); err != nil {
			return err
		}
		return tw.writeBytes(val)
	}
	return fmt.Errorf("unsupported type ID for row value: 0x%02X", ti.TypeID)
}

// WriteRow writes a ROW token using the column types from md.
func (tw *TokenWriter) WriteRow(md ColMetaData, row Row) error {
	if len(row.Values) != len(md.Columns) {
		return fmt.Errorf(
			"row has %d values but metadata has %d columns",
			len(row.Values), len(md.Columns),
		)
	}
	if err := tw.writeU8(TokenRow); err != nil {
		return err
	}
	for i, col := range md.Columns {
		if err := tw.writeRowValue(col.TypeInfo, row.Values[i]); err != nil {
			return err
		}
	}
	return nil
}

// WriteDone writes a DONE, DONEPROC, or DONEINPROC token.
func (tw *TokenWriter) WriteDone(d DoneToken) error {
	if err := tw.writeU8(d.TokenType); err != nil {
		return err
	}
	if err := tw.writeU16(d.Status); err != nil {
		return err
	}
	if err := tw.writeU16(d.CurCmd); err != nil {
		return err
	}
	return tw.writeU64(d.RowCount)
}

// WriteError writes an ERROR or INFO token. In TDS 7.x, all string
// fields are UTF-16LE encoded with character-count length prefixes.
func (tw *TokenWriter) WriteError(e ErrorToken) error {
	// Compute the payload length in bytes:
	// number(4) + state(1) + class(1) +
	// message: US_VARCHAR(2-byte charcount + chars*2) +
	// server: B_VARCHAR(1-byte charcount + chars*2) +
	// proc: B_VARCHAR(1-byte charcount + chars*2) +
	// line(4).
	msgChars := len(utf16.Encode([]rune(e.Message)))
	srvChars := len(utf16.Encode([]rune(e.Server)))
	procChars := len(utf16.Encode([]rune(e.Proc)))
	totalLen := 4 + 1 + 1 + (2 + msgChars*2) + (1 + srvChars*2) + (1 + procChars*2) + 4

	if err := tw.writeU8(e.TokenType); err != nil {
		return err
	}
	if err := tw.writeU16(uint16(totalLen)); err != nil {
		return err
	}
	if err := tw.writeI32(e.Number); err != nil {
		return err
	}
	if err := tw.writeU8(e.State); err != nil {
		return err
	}
	if err := tw.writeU8(e.Class); err != nil {
		return err
	}
	if err := tw.writeUsVarchar(e.Message); err != nil {
		return err
	}
	if err := tw.writeBVarchar(e.Server); err != nil {
		return err
	}
	if err := tw.writeBVarchar(e.Proc); err != nil {
		return err
	}
	return tw.writeI32(e.Line)
}

// WriteEnvChange writes an ENVCHANGE token. In TDS 7.x, string values
// are UTF-16LE encoded with character-count length prefixes.
func (tw *TokenWriter) WriteEnvChange(ec EnvChangeToken) error {
	// Payload: type(1) + newValue B_VARCHAR(1 + chars*2) +
	// oldValue B_VARCHAR(1 + chars*2).
	newChars := len(utf16.Encode([]rune(ec.NewValue)))
	oldChars := len(utf16.Encode([]rune(ec.OldValue)))
	totalLen := 1 + (1 + newChars*2) + (1 + oldChars*2)

	if err := tw.writeU8(TokenEnvChange); err != nil {
		return err
	}
	if err := tw.writeU16(uint16(totalLen)); err != nil {
		return err
	}
	if err := tw.writeU8(ec.Type); err != nil {
		return err
	}
	if err := tw.writeBVarchar(ec.NewValue); err != nil {
		return err
	}
	return tw.writeBVarchar(ec.OldValue)
}

// WriteLoginAck writes a LOGINACK token. In TDS 7.x, the program name
// is UTF-16LE encoded with a character-count length prefix.
func (tw *TokenWriter) WriteLoginAck(la LoginAckToken) error {
	// Payload: interface(1) + tdsVersion(4) +
	// progName B_VARCHAR(1 + chars*2) + progVersion(4).
	nameChars := len(utf16.Encode([]rune(la.ProgName)))
	totalLen := 1 + 4 + (1 + nameChars*2) + 4

	if err := tw.writeU8(TokenLoginAck); err != nil {
		return err
	}
	if err := tw.writeU16(uint16(totalLen)); err != nil {
		return err
	}
	if err := tw.writeU8(la.Interface); err != nil {
		return err
	}
	// The TDS version in LOGINACK is written big-endian. Clients
	// (FreeTDS, go-mssqldb) read the 4 bytes as a version identifier
	// in byte order (major.minor.build-hi.build-lo). The LOGIN7
	// packet stores the version as a LE DWORD, so we byte-swap here.
	if err := tw.writeU32BE(la.TDSVersion); err != nil {
		return err
	}
	if err := tw.writeBVarchar(la.ProgName); err != nil {
		return err
	}
	return tw.writeBytes(la.ProgVersion[:])
}

// WriteReturnStatus writes a RETURNSTATUS token.
func (tw *TokenWriter) WriteReturnStatus(rs ReturnStatusToken) error {
	if err := tw.writeU8(TokenReturnStatus); err != nil {
		return err
	}
	return tw.writeI32(rs.Value)
}

// WriteOrder writes an ORDER token.
func (tw *TokenWriter) WriteOrder(o OrderToken) error {
	if err := tw.writeU8(TokenOrder); err != nil {
		return err
	}
	// Length is the number of columns * 2 bytes each.
	if err := tw.writeU16(uint16(len(o.Columns) * 2)); err != nil {
		return err
	}
	for _, col := range o.Columns {
		if err := tw.writeU16(col); err != nil {
			return err
		}
	}
	return nil
}

// TokenReader reads TDS tokens from an underlying io.Reader.
type TokenReader struct {
	r   io.Reader
	buf []byte
}

// NewTokenReader creates a TokenReader that reads from r.
func NewTokenReader(r io.Reader) *TokenReader {
	return &TokenReader{r: r, buf: make([]byte, 8)}
}

func (tr *TokenReader) readU8() (byte, error) {
	_, err := io.ReadFull(tr.r, tr.buf[:1])
	return tr.buf[0], err
}

func (tr *TokenReader) readU16() (uint16, error) {
	_, err := io.ReadFull(tr.r, tr.buf[:2])
	return binary.LittleEndian.Uint16(tr.buf[:2]), err
}

func (tr *TokenReader) readU32() (uint32, error) {
	_, err := io.ReadFull(tr.r, tr.buf[:4])
	return binary.LittleEndian.Uint32(tr.buf[:4]), err
}

func (tr *TokenReader) readU32BE() (uint32, error) {
	_, err := io.ReadFull(tr.r, tr.buf[:4])
	return binary.BigEndian.Uint32(tr.buf[:4]), err
}

func (tr *TokenReader) readI32() (int32, error) {
	v, err := tr.readU32()
	return int32(v), err
}

func (tr *TokenReader) readU64() (uint64, error) {
	_, err := io.ReadFull(tr.r, tr.buf[:8])
	return binary.LittleEndian.Uint64(tr.buf[:8]), err
}

func (tr *TokenReader) readBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := io.ReadFull(tr.r, b)
	return b, err
}

// readBVarchar reads a TDS 7.x B_VARCHAR: 1-byte character count +
// UTF-16LE encoded string data.
func (tr *TokenReader) readBVarchar() (string, error) {
	n, err := tr.readU8()
	if err != nil {
		return "", err
	}
	b, err := tr.readBytes(int(n) * 2) // n is in UCS-2 chars
	if err != nil {
		return "", err
	}
	return decodeUTF16LEBytes(b), nil
}

// readUsVarchar reads a TDS 7.x US_VARCHAR: 2-byte character count +
// UTF-16LE encoded string data.
func (tr *TokenReader) readUsVarchar() (string, error) {
	n, err := tr.readU16()
	if err != nil {
		return "", err
	}
	b, err := tr.readBytes(int(n) * 2) // n is in UCS-2 chars
	if err != nil {
		return "", err
	}
	return decodeUTF16LEBytes(b), nil
}

// decodeUTF16LEBytes decodes a little-endian UTF-16 byte slice into a
// Go string.
func decodeUTF16LEBytes(b []byte) string {
	if len(b)%2 != 0 {
		b = b[:len(b)-1]
	}
	u16 := make([]uint16, len(b)/2)
	for i := range u16 {
		u16[i] = binary.LittleEndian.Uint16(b[i*2 : i*2+2])
	}
	return string(utf16.Decode(u16))
}

// readTypeInfo decodes a TypeInfo from the wire.
func (tr *TokenReader) readTypeInfo() (TypeInfo, error) {
	var ti TypeInfo
	var err error
	ti.TypeID, err = tr.readU8()
	if err != nil {
		return ti, err
	}
	if fixedTypeLen(ti.TypeID) > 0 {
		return ti, nil
	}
	if isByteLenType(ti.TypeID) {
		ti.ByteLen, err = tr.readU8()
		return ti, err
	}
	if isVariableLenType(ti.TypeID) {
		ti.MaxLen, err = tr.readU16()
		if err != nil {
			return ti, err
		}
		// String types have 5 bytes of collation data after max-length.
		if isStringType(ti.TypeID) {
			_, err = tr.readBytes(collationSize)
			if err != nil {
				return ti, err
			}
		}
		return ti, nil
	}
	if isPrecisionScaleType(ti.TypeID) {
		ti.ByteLen, err = tr.readU8()
		if err != nil {
			return ti, err
		}
		ti.Precision, err = tr.readU8()
		if err != nil {
			return ti, err
		}
		ti.Scale, err = tr.readU8()
		return ti, err
	}
	return ti, fmt.Errorf("unsupported type ID: 0x%02X", ti.TypeID)
}

// PeekToken reads the next token type byte without consuming it.
// The returned byte is the token type. Subsequent calls to Read*
// methods should handle that token. The caller must handle the token
// type byte themselves; it has already been consumed from the reader.
func (tr *TokenReader) PeekToken() (byte, error) {
	return tr.readU8()
}

// ReadColMetaData reads a COLMETADATA token. The token type byte must
// already have been consumed.
func (tr *TokenReader) ReadColMetaData() (ColMetaData, error) {
	var md ColMetaData
	count, err := tr.readU16()
	if err != nil {
		return md, err
	}
	md.Columns = make([]Column, count)
	for i := range md.Columns {
		md.Columns[i].UserType, err = tr.readU32()
		if err != nil {
			return md, err
		}
		md.Columns[i].Flags, err = tr.readU16()
		if err != nil {
			return md, err
		}
		md.Columns[i].TypeInfo, err = tr.readTypeInfo()
		if err != nil {
			return md, err
		}
		md.Columns[i].ColName, err = tr.readBVarchar()
		if err != nil {
			return md, err
		}
	}
	return md, nil
}

// readRowValue reads a single column value based on the type info.
func (tr *TokenReader) readRowValue(ti TypeInfo) ([]byte, error) {
	n := fixedTypeLen(ti.TypeID)
	if n > 0 {
		return tr.readBytes(n)
	}
	if isByteLenType(ti.TypeID) {
		length, err := tr.readU8()
		if err != nil {
			return nil, err
		}
		if length == 0 {
			// NULL for byte-length types.
			return nil, nil
		}
		return tr.readBytes(int(length))
	}
	if isVariableLenType(ti.TypeID) {
		length, err := tr.readU16()
		if err != nil {
			return nil, err
		}
		if length == 0xFFFF {
			// NULL sentinel for 2-byte length prefix types.
			return nil, nil
		}
		return tr.readBytes(int(length))
	}
	if isPrecisionScaleType(ti.TypeID) {
		length, err := tr.readU8()
		if err != nil {
			return nil, err
		}
		if length == 0 {
			return nil, nil
		}
		return tr.readBytes(int(length))
	}
	return nil, fmt.Errorf(
		"unsupported type ID for row value: 0x%02X", ti.TypeID,
	)
}

// ReadRow reads a ROW token using the column types from md. The token
// type byte must already have been consumed.
func (tr *TokenReader) ReadRow(md ColMetaData) (Row, error) {
	var row Row
	row.Values = make([][]byte, len(md.Columns))
	for i, col := range md.Columns {
		var err error
		row.Values[i], err = tr.readRowValue(col.TypeInfo)
		if err != nil {
			return row, err
		}
	}
	return row, nil
}

// ReadDone reads a DONE, DONEPROC, or DONEINPROC token. The token type
// byte must already have been consumed; pass it as tokenType.
func (tr *TokenReader) ReadDone(tokenType byte) (DoneToken, error) {
	d := DoneToken{TokenType: tokenType}
	var err error
	d.Status, err = tr.readU16()
	if err != nil {
		return d, err
	}
	d.CurCmd, err = tr.readU16()
	if err != nil {
		return d, err
	}
	d.RowCount, err = tr.readU64()
	return d, err
}

// ReadError reads an ERROR or INFO token. The token type byte must
// already have been consumed; pass it as tokenType.
func (tr *TokenReader) ReadError(tokenType byte) (ErrorToken, error) {
	e := ErrorToken{TokenType: tokenType}
	// Read and discard the length field; we parse by structure.
	_, err := tr.readU16()
	if err != nil {
		return e, err
	}
	e.Number, err = tr.readI32()
	if err != nil {
		return e, err
	}
	e.State, err = tr.readU8()
	if err != nil {
		return e, err
	}
	e.Class, err = tr.readU8()
	if err != nil {
		return e, err
	}
	e.Message, err = tr.readUsVarchar()
	if err != nil {
		return e, err
	}
	e.Server, err = tr.readBVarchar()
	if err != nil {
		return e, err
	}
	e.Proc, err = tr.readBVarchar()
	if err != nil {
		return e, err
	}
	e.Line, err = tr.readI32()
	return e, err
}

// ReadEnvChange reads an ENVCHANGE token. The token type byte must
// already have been consumed.
func (tr *TokenReader) ReadEnvChange() (EnvChangeToken, error) {
	var ec EnvChangeToken
	// Read and discard the length field.
	_, err := tr.readU16()
	if err != nil {
		return ec, err
	}
	ec.Type, err = tr.readU8()
	if err != nil {
		return ec, err
	}
	ec.NewValue, err = tr.readBVarchar()
	if err != nil {
		return ec, err
	}
	ec.OldValue, err = tr.readBVarchar()
	return ec, err
}

// ReadLoginAck reads a LOGINACK token. The token type byte must
// already have been consumed.
func (tr *TokenReader) ReadLoginAck() (LoginAckToken, error) {
	var la LoginAckToken
	// Read and discard the length field.
	_, err := tr.readU16()
	if err != nil {
		return la, err
	}
	la.Interface, err = tr.readU8()
	if err != nil {
		return la, err
	}
	// TDSVersion uses big-endian byte order (see WriteLoginAck).
	la.TDSVersion, err = tr.readU32BE()
	if err != nil {
		return la, err
	}
	la.ProgName, err = tr.readBVarchar()
	if err != nil {
		return la, err
	}
	b, err := tr.readBytes(4)
	if err != nil {
		return la, err
	}
	copy(la.ProgVersion[:], b)
	return la, nil
}

// ReadReturnStatus reads a RETURNSTATUS token. The token type byte
// must already have been consumed.
func (tr *TokenReader) ReadReturnStatus() (ReturnStatusToken, error) {
	var rs ReturnStatusToken
	var err error
	rs.Value, err = tr.readI32()
	return rs, err
}

// ReadOrder reads an ORDER token. The token type byte must already
// have been consumed.
func (tr *TokenReader) ReadOrder() (OrderToken, error) {
	var o OrderToken
	length, err := tr.readU16()
	if err != nil {
		return o, err
	}
	count := int(length) / 2
	o.Columns = make([]uint16, count)
	for i := range o.Columns {
		o.Columns[i], err = tr.readU16()
		if err != nil {
			return o, err
		}
	}
	return o, nil
}
