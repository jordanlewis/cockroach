// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tdswire

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// roundTrip is a helper that writes using write, then reads back with
// read, and returns the reader for assertions.
func roundTripBuf(t *testing.T, write func(tw *TokenWriter) error) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	tw := NewTokenWriter(&buf)
	if err := write(tw); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	return &buf
}

func TestColMetaDataRoundTrip(t *testing.T) {
	md := ColMetaData{
		Columns: []Column{
			{
				UserType: 0,
				Flags:    0x08,
				TypeInfo: TypeInfo{TypeID: TypeInt4},
				ColName:  "id",
			},
			{
				UserType: 0,
				Flags:    0x08,
				TypeInfo: TypeInfo{
					TypeID: TypeBigVarChar,
					MaxLen: 255,
				},
				ColName: "name",
			},
		},
	}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteColMetaData(md)
	})

	tr := NewTokenReader(buf)
	tok, err := tr.PeekToken()
	if err != nil {
		t.Fatalf("PeekToken: %v", err)
	}
	if tok != TokenColMetaData {
		t.Fatalf("expected token 0x%02X, got 0x%02X", TokenColMetaData, tok)
	}

	got, err := tr.ReadColMetaData()
	if err != nil {
		t.Fatalf("ReadColMetaData: %v", err)
	}
	if len(got.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(got.Columns))
	}
	if got.Columns[0].ColName != "id" {
		t.Errorf("col 0 name: got %q, want %q", got.Columns[0].ColName, "id")
	}
	if got.Columns[0].TypeInfo.TypeID != TypeInt4 {
		t.Errorf(
			"col 0 type: got 0x%02X, want 0x%02X",
			got.Columns[0].TypeInfo.TypeID, TypeInt4,
		)
	}
	if got.Columns[1].ColName != "name" {
		t.Errorf("col 1 name: got %q, want %q", got.Columns[1].ColName, "name")
	}
	if got.Columns[1].TypeInfo.MaxLen != 255 {
		t.Errorf(
			"col 1 maxlen: got %d, want %d",
			got.Columns[1].TypeInfo.MaxLen, 255,
		)
	}
}

func TestColMetaDataDecimalType(t *testing.T) {
	md := ColMetaData{
		Columns: []Column{
			{
				UserType: 0,
				Flags:    0,
				TypeInfo: TypeInfo{
					TypeID:    TypeDecimalN,
					ByteLen:   17,
					Precision: 38,
					Scale:     10,
				},
				ColName: "amount",
			},
		},
	}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteColMetaData(md)
	})

	tr := NewTokenReader(buf)
	if _, err := tr.PeekToken(); err != nil {
		t.Fatal(err)
	}
	got, err := tr.ReadColMetaData()
	if err != nil {
		t.Fatalf("ReadColMetaData: %v", err)
	}
	col := got.Columns[0]
	if col.TypeInfo.TypeID != TypeDecimalN {
		t.Errorf("type: got 0x%02X, want 0x%02X", col.TypeInfo.TypeID, TypeDecimalN)
	}
	if col.TypeInfo.ByteLen != 17 {
		t.Errorf("byteLen: got %d, want 17", col.TypeInfo.ByteLen)
	}
	if col.TypeInfo.Precision != 38 {
		t.Errorf("precision: got %d, want 38", col.TypeInfo.Precision)
	}
	if col.TypeInfo.Scale != 10 {
		t.Errorf("scale: got %d, want 10", col.TypeInfo.Scale)
	}
}

func TestColMetaDataByteLenType(t *testing.T) {
	md := ColMetaData{
		Columns: []Column{
			{
				UserType: 0,
				Flags:    0,
				TypeInfo: TypeInfo{TypeID: TypeIntN, ByteLen: 4},
				ColName:  "nullable_int",
			},
		},
	}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteColMetaData(md)
	})

	tr := NewTokenReader(buf)
	if _, err := tr.PeekToken(); err != nil {
		t.Fatal(err)
	}
	got, err := tr.ReadColMetaData()
	if err != nil {
		t.Fatalf("ReadColMetaData: %v", err)
	}
	if got.Columns[0].TypeInfo.ByteLen != 4 {
		t.Errorf(
			"byteLen: got %d, want 4", got.Columns[0].TypeInfo.ByteLen,
		)
	}
}

func TestRowFixedLenRoundTrip(t *testing.T) {
	md := ColMetaData{
		Columns: []Column{
			{TypeInfo: TypeInfo{TypeID: TypeInt4}},
			{TypeInfo: TypeInfo{TypeID: TypeInt8}},
		},
	}

	val4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(val4, 42)
	val8 := make([]byte, 8)
	binary.LittleEndian.PutUint64(val8, 9999)

	row := Row{Values: [][]byte{val4, val8}}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteRow(md, row)
	})

	tr := NewTokenReader(buf)
	tok, err := tr.PeekToken()
	if err != nil {
		t.Fatal(err)
	}
	if tok != TokenRow {
		t.Fatalf("expected ROW token, got 0x%02X", tok)
	}

	got, err := tr.ReadRow(md)
	if err != nil {
		t.Fatalf("ReadRow: %v", err)
	}
	gotVal4 := binary.LittleEndian.Uint32(got.Values[0])
	if gotVal4 != 42 {
		t.Errorf("col 0: got %d, want 42", gotVal4)
	}
	gotVal8 := binary.LittleEndian.Uint64(got.Values[1])
	if gotVal8 != 9999 {
		t.Errorf("col 1: got %d, want 9999", gotVal8)
	}
}

func TestRowVarLenRoundTrip(t *testing.T) {
	md := ColMetaData{
		Columns: []Column{
			{TypeInfo: TypeInfo{TypeID: TypeBigVarChar, MaxLen: 100}},
		},
	}

	row := Row{Values: [][]byte{[]byte("hello")}}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteRow(md, row)
	})

	tr := NewTokenReader(buf)
	if _, err := tr.PeekToken(); err != nil {
		t.Fatal(err)
	}
	got, err := tr.ReadRow(md)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Values[0]) != "hello" {
		t.Errorf("got %q, want %q", got.Values[0], "hello")
	}
}

func TestRowNullVarLen(t *testing.T) {
	md := ColMetaData{
		Columns: []Column{
			{TypeInfo: TypeInfo{TypeID: TypeBigVarChar, MaxLen: 100}},
		},
	}

	row := Row{Values: [][]byte{nil}}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteRow(md, row)
	})

	tr := NewTokenReader(buf)
	if _, err := tr.PeekToken(); err != nil {
		t.Fatal(err)
	}
	got, err := tr.ReadRow(md)
	if err != nil {
		t.Fatal(err)
	}
	if got.Values[0] != nil {
		t.Errorf("expected nil for NULL, got %v", got.Values[0])
	}
}

func TestRowNullByteLen(t *testing.T) {
	md := ColMetaData{
		Columns: []Column{
			{TypeInfo: TypeInfo{TypeID: TypeIntN, ByteLen: 4}},
		},
	}

	row := Row{Values: [][]byte{nil}}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteRow(md, row)
	})

	tr := NewTokenReader(buf)
	if _, err := tr.PeekToken(); err != nil {
		t.Fatal(err)
	}
	got, err := tr.ReadRow(md)
	if err != nil {
		t.Fatal(err)
	}
	if got.Values[0] != nil {
		t.Errorf("expected nil for NULL byte-len type, got %v", got.Values[0])
	}
}

func TestRowNullDecimal(t *testing.T) {
	md := ColMetaData{
		Columns: []Column{
			{TypeInfo: TypeInfo{
				TypeID:    TypeDecimalN,
				ByteLen:   17,
				Precision: 18,
				Scale:     2,
			}},
		},
	}

	row := Row{Values: [][]byte{nil}}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteRow(md, row)
	})

	tr := NewTokenReader(buf)
	if _, err := tr.PeekToken(); err != nil {
		t.Fatal(err)
	}
	got, err := tr.ReadRow(md)
	if err != nil {
		t.Fatal(err)
	}
	if got.Values[0] != nil {
		t.Errorf("expected nil for NULL decimal, got %v", got.Values[0])
	}
}

func TestDoneRoundTrip(t *testing.T) {
	for _, tt := range []struct {
		name      string
		tokenType byte
	}{
		{"Done", TokenDone},
		{"DoneProc", TokenDoneProc},
		{"DoneInProc", TokenDoneInProc},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := DoneToken{
				TokenType: tt.tokenType,
				Status:    DoneMore | DoneCount,
				CurCmd:    0xC1,
				RowCount:  12345,
			}

			buf := roundTripBuf(t, func(tw *TokenWriter) error {
				return tw.WriteDone(d)
			})

			tr := NewTokenReader(buf)
			tok, err := tr.PeekToken()
			if err != nil {
				t.Fatal(err)
			}
			if tok != tt.tokenType {
				t.Fatalf(
					"expected token 0x%02X, got 0x%02X",
					tt.tokenType, tok,
				)
			}

			got, err := tr.ReadDone(tok)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != d.Status {
				t.Errorf("status: got %d, want %d", got.Status, d.Status)
			}
			if got.CurCmd != d.CurCmd {
				t.Errorf("curcmd: got %d, want %d", got.CurCmd, d.CurCmd)
			}
			if got.RowCount != d.RowCount {
				t.Errorf(
					"rowcount: got %d, want %d",
					got.RowCount, d.RowCount,
				)
			}
		})
	}
}

func TestDoneFinalStatus(t *testing.T) {
	d := DoneToken{
		TokenType: TokenDone,
		Status:    DoneFinal,
		CurCmd:    0,
		RowCount:  0,
	}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteDone(d)
	})

	tr := NewTokenReader(buf)
	if _, err := tr.PeekToken(); err != nil {
		t.Fatal(err)
	}
	got, err := tr.ReadDone(TokenDone)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != DoneFinal {
		t.Errorf("status: got 0x%04X, want 0x%04X", got.Status, DoneFinal)
	}
}

func TestErrorRoundTrip(t *testing.T) {
	e := ErrorToken{
		TokenType: TokenError,
		Number:    8134,
		State:     1,
		Class:     16,
		Message:   "Divide by zero error encountered.",
		Server:    "TESTSERVER",
		Proc:      "sp_test",
		Line:      42,
	}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteError(e)
	})

	tr := NewTokenReader(buf)
	tok, err := tr.PeekToken()
	if err != nil {
		t.Fatal(err)
	}
	if tok != TokenError {
		t.Fatalf("expected ERROR token, got 0x%02X", tok)
	}

	got, err := tr.ReadError(tok)
	if err != nil {
		t.Fatal(err)
	}
	if got.Number != e.Number {
		t.Errorf("number: got %d, want %d", got.Number, e.Number)
	}
	if got.State != e.State {
		t.Errorf("state: got %d, want %d", got.State, e.State)
	}
	if got.Class != e.Class {
		t.Errorf("class: got %d, want %d", got.Class, e.Class)
	}
	if got.Message != e.Message {
		t.Errorf("message: got %q, want %q", got.Message, e.Message)
	}
	if got.Server != e.Server {
		t.Errorf("server: got %q, want %q", got.Server, e.Server)
	}
	if got.Proc != e.Proc {
		t.Errorf("proc: got %q, want %q", got.Proc, e.Proc)
	}
	if got.Line != e.Line {
		t.Errorf("line: got %d, want %d", got.Line, e.Line)
	}
}

func TestInfoRoundTrip(t *testing.T) {
	e := ErrorToken{
		TokenType: TokenInfo,
		Number:    5701,
		State:     2,
		Class:     0,
		Message:   "Changed database context to 'master'.",
		Server:    "PROD01",
		Proc:      "",
		Line:      1,
	}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteError(e)
	})

	tr := NewTokenReader(buf)
	tok, err := tr.PeekToken()
	if err != nil {
		t.Fatal(err)
	}
	if tok != TokenInfo {
		t.Fatalf("expected INFO token, got 0x%02X", tok)
	}

	got, err := tr.ReadError(tok)
	if err != nil {
		t.Fatal(err)
	}
	if got.TokenType != TokenInfo {
		t.Errorf(
			"tokenType: got 0x%02X, want 0x%02X",
			got.TokenType, TokenInfo,
		)
	}
	if got.Message != e.Message {
		t.Errorf("message: got %q, want %q", got.Message, e.Message)
	}
}

func TestEnvChangeRoundTrip(t *testing.T) {
	for _, tt := range []struct {
		name     string
		envType  byte
		newValue string
		oldValue string
	}{
		{"Database", EnvDatabase, "mydb", "master"},
		{"Language", EnvLanguage, "us_english", ""},
		{"Charset", EnvCharset, "utf8", "iso_1"},
		{"PacketSize", EnvPacketSize, "4096", "512"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ec := EnvChangeToken{
				Type:     tt.envType,
				NewValue: tt.newValue,
				OldValue: tt.oldValue,
			}

			buf := roundTripBuf(t, func(tw *TokenWriter) error {
				return tw.WriteEnvChange(ec)
			})

			tr := NewTokenReader(buf)
			tok, err := tr.PeekToken()
			if err != nil {
				t.Fatal(err)
			}
			if tok != TokenEnvChange {
				t.Fatalf(
					"expected ENVCHANGE token, got 0x%02X", tok,
				)
			}

			got, err := tr.ReadEnvChange()
			if err != nil {
				t.Fatal(err)
			}
			if got.Type != tt.envType {
				t.Errorf(
					"type: got %d, want %d", got.Type, tt.envType,
				)
			}
			if got.NewValue != tt.newValue {
				t.Errorf(
					"newValue: got %q, want %q",
					got.NewValue, tt.newValue,
				)
			}
			if got.OldValue != tt.oldValue {
				t.Errorf(
					"oldValue: got %q, want %q",
					got.OldValue, tt.oldValue,
				)
			}
		})
	}
}

func TestLoginAckRoundTrip(t *testing.T) {
	la := LoginAckToken{
		Interface:   1,
		TDSVersion:  0x74000004,
		ProgName:    "CockroachDB",
		ProgVersion: [4]byte{24, 1, 0, 0},
	}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteLoginAck(la)
	})

	tr := NewTokenReader(buf)
	tok, err := tr.PeekToken()
	if err != nil {
		t.Fatal(err)
	}
	if tok != TokenLoginAck {
		t.Fatalf("expected LOGINACK token, got 0x%02X", tok)
	}

	got, err := tr.ReadLoginAck()
	if err != nil {
		t.Fatal(err)
	}
	if got.Interface != la.Interface {
		t.Errorf(
			"interface: got %d, want %d",
			got.Interface, la.Interface,
		)
	}
	if got.TDSVersion != la.TDSVersion {
		t.Errorf(
			"tdsVersion: got 0x%08X, want 0x%08X",
			got.TDSVersion, la.TDSVersion,
		)
	}
	if got.ProgName != la.ProgName {
		t.Errorf(
			"progName: got %q, want %q", got.ProgName, la.ProgName,
		)
	}
	if got.ProgVersion != la.ProgVersion {
		t.Errorf(
			"progVersion: got %v, want %v",
			got.ProgVersion, la.ProgVersion,
		)
	}
}

func TestReturnStatusRoundTrip(t *testing.T) {
	rs := ReturnStatusToken{Value: -1}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteReturnStatus(rs)
	})

	tr := NewTokenReader(buf)
	tok, err := tr.PeekToken()
	if err != nil {
		t.Fatal(err)
	}
	if tok != TokenReturnStatus {
		t.Fatalf("expected RETURNSTATUS token, got 0x%02X", tok)
	}

	got, err := tr.ReadReturnStatus()
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != -1 {
		t.Errorf("value: got %d, want -1", got.Value)
	}
}

func TestOrderRoundTrip(t *testing.T) {
	o := OrderToken{Columns: []uint16{1, 3, 2}}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteOrder(o)
	})

	tr := NewTokenReader(buf)
	tok, err := tr.PeekToken()
	if err != nil {
		t.Fatal(err)
	}
	if tok != TokenOrder {
		t.Fatalf("expected ORDER token, got 0x%02X", tok)
	}

	got, err := tr.ReadOrder()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(got.Columns))
	}
	for i, want := range []uint16{1, 3, 2} {
		if got.Columns[i] != want {
			t.Errorf(
				"column %d: got %d, want %d",
				i, got.Columns[i], want,
			)
		}
	}
}

func TestFullResultSetRoundTrip(t *testing.T) {
	// Simulate a complete result set: COLMETADATA + ROW + ROW + DONE.
	md := ColMetaData{
		Columns: []Column{
			{
				UserType: 0,
				Flags:    0,
				TypeInfo: TypeInfo{TypeID: TypeInt4},
				ColName:  "id",
			},
			{
				UserType: 0,
				Flags:    0x08,
				TypeInfo: TypeInfo{
					TypeID: TypeBigVarChar,
					MaxLen: 50,
				},
				ColName: "val",
			},
		},
	}

	intBytes := func(v uint32) []byte {
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, v)
		return b
	}

	rows := []Row{
		{Values: [][]byte{intBytes(1), []byte("alpha")}},
		{Values: [][]byte{intBytes(2), nil}},
	}

	done := DoneToken{
		TokenType: TokenDone,
		Status:    DoneCount,
		CurCmd:    0xC1,
		RowCount:  2,
	}

	var buf bytes.Buffer
	tw := NewTokenWriter(&buf)
	if err := tw.WriteColMetaData(md); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := tw.WriteRow(md, row); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.WriteDone(done); err != nil {
		t.Fatal(err)
	}

	// Read it all back.
	tr := NewTokenReader(&buf)

	// COLMETADATA
	tok, err := tr.PeekToken()
	if err != nil {
		t.Fatal(err)
	}
	if tok != TokenColMetaData {
		t.Fatalf("expected COLMETADATA, got 0x%02X", tok)
	}
	gotMD, err := tr.ReadColMetaData()
	if err != nil {
		t.Fatal(err)
	}

	// ROW 1
	tok, err = tr.PeekToken()
	if err != nil {
		t.Fatal(err)
	}
	if tok != TokenRow {
		t.Fatalf("expected ROW, got 0x%02X", tok)
	}
	gotRow1, err := tr.ReadRow(gotMD)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(gotRow1.Values[0]) != 1 {
		t.Error("row 1 col 0: expected 1")
	}
	if string(gotRow1.Values[1]) != "alpha" {
		t.Errorf(
			"row 1 col 1: got %q, want %q",
			gotRow1.Values[1], "alpha",
		)
	}

	// ROW 2 (with NULL)
	tok, err = tr.PeekToken()
	if err != nil {
		t.Fatal(err)
	}
	gotRow2, err := tr.ReadRow(gotMD)
	if err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(gotRow2.Values[0]) != 2 {
		t.Error("row 2 col 0: expected 2")
	}
	if gotRow2.Values[1] != nil {
		t.Errorf("row 2 col 1: expected nil, got %v", gotRow2.Values[1])
	}

	// DONE
	tok, err = tr.PeekToken()
	if err != nil {
		t.Fatal(err)
	}
	if tok != TokenDone {
		t.Fatalf("expected DONE, got 0x%02X", tok)
	}
	gotDone, err := tr.ReadDone(tok)
	if err != nil {
		t.Fatal(err)
	}
	if gotDone.RowCount != 2 {
		t.Errorf("done rowcount: got %d, want 2", gotDone.RowCount)
	}
}

func TestRowValueCountMismatch(t *testing.T) {
	md := ColMetaData{
		Columns: []Column{
			{TypeInfo: TypeInfo{TypeID: TypeInt4}},
			{TypeInfo: TypeInfo{TypeID: TypeInt4}},
		},
	}
	row := Row{Values: [][]byte{make([]byte, 4)}}

	var buf bytes.Buffer
	tw := NewTokenWriter(&buf)
	err := tw.WriteRow(md, row)
	if err == nil {
		t.Fatal("expected error for mismatched column count")
	}
}

func TestRowFixedLenSizeMismatch(t *testing.T) {
	md := ColMetaData{
		Columns: []Column{
			{TypeInfo: TypeInfo{TypeID: TypeInt4}},
		},
	}
	// Provide 2 bytes instead of 4.
	row := Row{Values: [][]byte{make([]byte, 2)}}

	var buf bytes.Buffer
	tw := NewTokenWriter(&buf)
	err := tw.WriteRow(md, row)
	if err == nil {
		t.Fatal("expected error for wrong fixed-length size")
	}
}

func TestDoneErrorStatus(t *testing.T) {
	d := DoneToken{
		TokenType: TokenDone,
		Status:    DoneError | DoneMore,
		CurCmd:    0,
		RowCount:  0,
	}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteDone(d)
	})

	tr := NewTokenReader(buf)
	if _, err := tr.PeekToken(); err != nil {
		t.Fatal(err)
	}
	got, err := tr.ReadDone(TokenDone)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != (DoneError | DoneMore) {
		t.Errorf(
			"status: got 0x%04X, want 0x%04X",
			got.Status, DoneError|DoneMore,
		)
	}
}

func TestRowNonNullByteLen(t *testing.T) {
	md := ColMetaData{
		Columns: []Column{
			{TypeInfo: TypeInfo{TypeID: TypeIntN, ByteLen: 4}},
		},
	}

	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val, 100)
	row := Row{Values: [][]byte{val}}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteRow(md, row)
	})

	tr := NewTokenReader(buf)
	if _, err := tr.PeekToken(); err != nil {
		t.Fatal(err)
	}
	got, err := tr.ReadRow(md)
	if err != nil {
		t.Fatal(err)
	}
	if got.Values[0] == nil {
		t.Fatal("expected non-nil value")
	}
	v := binary.LittleEndian.Uint32(got.Values[0])
	if v != 100 {
		t.Errorf("got %d, want 100", v)
	}
}

func TestDecimalNonNullRoundTrip(t *testing.T) {
	md := ColMetaData{
		Columns: []Column{
			{TypeInfo: TypeInfo{
				TypeID:    TypeNumericN,
				ByteLen:   9,
				Precision: 18,
				Scale:     2,
			}},
		},
	}

	// Arbitrary decimal payload.
	val := []byte{0x01, 0xE8, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	row := Row{Values: [][]byte{val}}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteRow(md, row)
	})

	tr := NewTokenReader(buf)
	if _, err := tr.PeekToken(); err != nil {
		t.Fatal(err)
	}
	got, err := tr.ReadRow(md)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Values[0], val) {
		t.Errorf("got %v, want %v", got.Values[0], val)
	}
}

func TestEmptyColMetaData(t *testing.T) {
	md := ColMetaData{Columns: nil}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteColMetaData(md)
	})

	tr := NewTokenReader(buf)
	if _, err := tr.PeekToken(); err != nil {
		t.Fatal(err)
	}
	got, err := tr.ReadColMetaData()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Columns) != 0 {
		t.Errorf("expected 0 columns, got %d", len(got.Columns))
	}
}

func TestOrderEmpty(t *testing.T) {
	o := OrderToken{Columns: nil}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteOrder(o)
	})

	tr := NewTokenReader(buf)
	if _, err := tr.PeekToken(); err != nil {
		t.Fatal(err)
	}
	got, err := tr.ReadOrder()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Columns) != 0 {
		t.Errorf("expected 0 columns, got %d", len(got.Columns))
	}
}

func TestReturnStatusZero(t *testing.T) {
	rs := ReturnStatusToken{Value: 0}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteReturnStatus(rs)
	})

	tr := NewTokenReader(buf)
	if _, err := tr.PeekToken(); err != nil {
		t.Fatal(err)
	}
	got, err := tr.ReadReturnStatus()
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != 0 {
		t.Errorf("value: got %d, want 0", got.Value)
	}
}

func TestErrorEmptyStrings(t *testing.T) {
	e := ErrorToken{
		TokenType: TokenError,
		Number:    100,
		State:     1,
		Class:     11,
		Message:   "",
		Server:    "",
		Proc:      "",
		Line:      0,
	}

	buf := roundTripBuf(t, func(tw *TokenWriter) error {
		return tw.WriteError(e)
	})

	tr := NewTokenReader(buf)
	if _, err := tr.PeekToken(); err != nil {
		t.Fatal(err)
	}
	got, err := tr.ReadError(TokenError)
	if err != nil {
		t.Fatal(err)
	}
	if got.Message != "" {
		t.Errorf("message: got %q, want empty", got.Message)
	}
	if got.Server != "" {
		t.Errorf("server: got %q, want empty", got.Server)
	}
	if got.Proc != "" {
		t.Errorf("proc: got %q, want empty", got.Proc)
	}
}
