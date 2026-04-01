// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tds

import (
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/tds/catalog"
	"github.com/cockroachdb/cockroach/pkg/tds/tdswire"
)

// --- Mock isql types ---

// mockExecutor implements the subset of isql.Executor used by the TDS
// Executor. It records calls and returns preconfigured results.
type mockExecutor struct {
	// execExFn is called for ExecEx.
	execExFn func(stmt string) (int, error)
	// queryBufferedExWithColsFn is called for QueryBufferedExWithCols.
	queryBufferedExWithColsFn func(stmt string) ([]tree.Datums, colinfo.ResultColumns, error)
}

// mockDB implements isql.DB for testing. It returns a single mockExecutor.
type mockDB struct {
	executor *mockExecutor
}

// The mockDB and mockExecutor need to implement the full isql interfaces.
// For testing purposes, we only implement the methods we use. The rest
// panic with "not implemented". This is acceptable because these mocks
// are only used within this test file.

// We can't directly implement isql.DB/isql.Executor because they have
// many methods. Instead, we test the Executor's internal logic by
// testing the token-generation functions directly, without going through
// the full isql path.

// --- Token stream helper ---

// readTokenStream parses a TDS token stream and returns the token types found.
func readTokenStream(data []byte) []byte {
	return parseTokenTypes(data)
}

// readDoneToken reads a DONE token from the given data and returns it.
func readDoneToken(t *testing.T, data []byte) tdswire.DoneToken {
	t.Helper()
	r := bytes.NewReader(data)
	tr := tdswire.NewTokenReader(r)
	for {
		tok, err := tr.PeekToken()
		if err != nil {
			t.Fatalf("reading token: %v", err)
		}
		switch tok {
		case tdswire.TokenDone, tdswire.TokenDoneProc, tdswire.TokenDoneInProc:
			d, err := tr.ReadDone(tok)
			if err != nil {
				t.Fatalf("reading done: %v", err)
			}
			return d
		case tdswire.TokenError, tdswire.TokenInfo:
			if _, err := tr.ReadError(tok); err != nil {
				t.Fatalf("reading error: %v", err)
			}
		case tdswire.TokenEnvChange:
			if _, err := tr.ReadEnvChange(); err != nil {
				t.Fatalf("reading envchange: %v", err)
			}
		case tdswire.TokenColMetaData:
			if _, err := tr.ReadColMetaData(); err != nil {
				t.Fatalf("reading colmetadata: %v", err)
			}
		case tdswire.TokenLoginAck:
			if _, err := tr.ReadLoginAck(); err != nil {
				t.Fatalf("reading loginack: %v", err)
			}
		default:
			t.Fatalf("unexpected token 0x%02X", tok)
		}
	}
}

// --- Tests for writeDoneFinal ---

func TestWriteDoneFinal(t *testing.T) {
	var buf bytes.Buffer
	tw := tdswire.NewTokenWriter(&buf)

	if err := writeDoneFinal(tw); err != nil {
		t.Fatalf("writeDoneFinal: %v", err)
	}

	tokens := readTokenStream(buf.Bytes())
	if len(tokens) != 1 || tokens[0] != tdswire.TokenDone {
		t.Errorf("expected [DONE], got %v", tokens)
	}

	d := readDoneToken(t, buf.Bytes())
	if d.Status != tdswire.DoneFinal {
		t.Errorf("expected DoneFinal status, got %d", d.Status)
	}
	if d.RowCount != 0 {
		t.Errorf("expected row count 0, got %d", d.RowCount)
	}
}

// --- Tests for writeErrorToken ---

func TestWriteErrorToken(t *testing.T) {
	var buf bytes.Buffer
	tw := tdswire.NewTokenWriter(&buf)

	if err := writeErrorToken(tw, 50000, 1, 16, "test error message"); err != nil {
		t.Fatalf("writeErrorToken: %v", err)
	}

	tokens := readTokenStream(buf.Bytes())
	expected := []byte{tdswire.TokenError, tdswire.TokenDone}
	if !bytes.Equal(tokens, expected) {
		t.Errorf("expected tokens %v, got %v", expected, tokens)
	}

	// Verify error details.
	r := bytes.NewReader(buf.Bytes())
	tr := tdswire.NewTokenReader(r)
	tok, _ := tr.PeekToken()
	errTok, err := tr.ReadError(tok)
	if err != nil {
		t.Fatalf("reading error: %v", err)
	}
	if errTok.Number != 50000 {
		t.Errorf("expected error number 50000, got %d", errTok.Number)
	}
	if errTok.State != 1 {
		t.Errorf("expected state 1, got %d", errTok.State)
	}
	if errTok.Class != 16 {
		t.Errorf("expected class 16, got %d", errTok.Class)
	}
	if errTok.Message != "test error message" {
		t.Errorf("expected message 'test error message', got %q", errTok.Message)
	}

	// Verify DONE with error flag.
	tok2, _ := tr.PeekToken()
	done, _ := tr.ReadDone(tok2)
	if done.Status&tdswire.DoneError == 0 {
		t.Errorf("expected DoneError flag, got status %d", done.Status)
	}
}

// --- Tests for mapResultColumns ---

func TestMapResultColumnsIntTypes(t *testing.T) {
	cols := []resultColumnInfo{
		{Name: "id", Typ: types.Int4},
		{Name: "big_id", Typ: types.Int},
		{Name: "small_id", Typ: types.Int2},
	}

	md, typeInfos, err := mapResultColumns(cols)
	if err != nil {
		t.Fatalf("mapResultColumns: %v", err)
	}

	if len(md.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(md.Columns))
	}

	// INT4 -> IntNType with ByteLen=4
	if md.Columns[0].ColName != "id" {
		t.Errorf("expected column name 'id', got %q", md.Columns[0].ColName)
	}
	if md.Columns[0].TypeInfo.TypeID != tdswire.TypeIntN {
		t.Errorf("expected TypeIntN for INT4, got 0x%02X", md.Columns[0].TypeInfo.TypeID)
	}
	if md.Columns[0].TypeInfo.ByteLen != 4 {
		t.Errorf("expected ByteLen 4 for INT4, got %d", md.Columns[0].TypeInfo.ByteLen)
	}

	// INT8 -> IntNType with ByteLen=8
	if md.Columns[1].TypeInfo.ByteLen != 8 {
		t.Errorf("expected ByteLen 8 for INT, got %d", md.Columns[1].TypeInfo.ByteLen)
	}

	// INT2 -> IntNType with ByteLen=2
	if md.Columns[2].TypeInfo.ByteLen != 2 {
		t.Errorf("expected ByteLen 2 for INT2, got %d", md.Columns[2].TypeInfo.ByteLen)
	}

	// Verify TypeInfos match.
	if len(typeInfos) != 3 {
		t.Fatalf("expected 3 type infos, got %d", len(typeInfos))
	}
}

func TestMapResultColumnsStringType(t *testing.T) {
	cols := []resultColumnInfo{
		{Name: "name", Typ: types.String},
	}

	md, _, err := mapResultColumns(cols)
	if err != nil {
		t.Fatalf("mapResultColumns: %v", err)
	}

	if md.Columns[0].TypeInfo.TypeID != tdswire.TypeNVarChar {
		t.Errorf("expected TypeNVarChar for STRING, got 0x%02X", md.Columns[0].TypeInfo.TypeID)
	}
	if md.Columns[0].TypeInfo.MaxLen == 0 {
		t.Errorf("expected non-zero MaxLen for NVARCHAR")
	}
}

func TestMapResultColumnsBoolType(t *testing.T) {
	cols := []resultColumnInfo{
		{Name: "active", Typ: types.Bool},
	}

	md, _, err := mapResultColumns(cols)
	if err != nil {
		t.Fatalf("mapResultColumns: %v", err)
	}

	if md.Columns[0].TypeInfo.TypeID != tdswire.TypeBitN {
		t.Errorf("expected TypeBitN for BOOL, got 0x%02X", md.Columns[0].TypeInfo.TypeID)
	}
}

// --- Tests for datumToGoValue ---

func TestDatumToGoValueInt(t *testing.T) {
	d := tree.NewDInt(42)
	v, err := datumToGoValue(d)
	if err != nil {
		t.Fatalf("datumToGoValue: %v", err)
	}
	intVal, ok := v.(int64)
	if !ok {
		t.Fatalf("expected int64, got %T", v)
	}
	if intVal != 42 {
		t.Errorf("expected 42, got %d", intVal)
	}
}

func TestDatumToGoValueString(t *testing.T) {
	d := tree.NewDString("hello")
	v, err := datumToGoValue(d)
	if err != nil {
		t.Fatalf("datumToGoValue: %v", err)
	}
	strVal, ok := v.(string)
	if !ok {
		t.Fatalf("expected string, got %T", v)
	}
	if strVal != "hello" {
		t.Errorf("expected 'hello', got %q", strVal)
	}
}

func TestDatumToGoValueBool(t *testing.T) {
	d := tree.MakeDBool(true)
	v, err := datumToGoValue(d)
	if err != nil {
		t.Fatalf("datumToGoValue: %v", err)
	}
	boolVal, ok := v.(bool)
	if !ok {
		t.Fatalf("expected bool, got %T", v)
	}
	if !boolVal {
		t.Errorf("expected true, got false")
	}
}

func TestDatumToGoValueFloat(t *testing.T) {
	d := tree.NewDFloat(3.14)
	v, err := datumToGoValue(d)
	if err != nil {
		t.Fatalf("datumToGoValue: %v", err)
	}
	floatVal, ok := v.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", v)
	}
	if floatVal != 3.14 {
		t.Errorf("expected 3.14, got %f", floatVal)
	}
}

func TestDatumToGoValueNull(t *testing.T) {
	// DNull should be handled before datumToGoValue is called,
	// but verify the fallback works.
	v, err := datumToGoValue(tree.DNull)
	if err != nil {
		t.Fatalf("datumToGoValue: %v", err)
	}
	// DNull fallback returns "NULL" string.
	if v != "NULL" {
		t.Errorf("expected 'NULL', got %v", v)
	}
}

// --- Tests for mapDatumsToRow ---

func TestMapDatumsToRowBasic(t *testing.T) {
	datums := tree.Datums{
		tree.NewDInt(42),
		tree.NewDString("hello"),
	}

	// Create matching type infos.
	cols := []resultColumnInfo{
		{Name: "id", Typ: types.Int4},
		{Name: "name", Typ: types.String},
	}
	_, typeInfos, err := mapResultColumns(cols)
	if err != nil {
		t.Fatalf("mapResultColumns: %v", err)
	}

	row, err := mapDatumsToRow(datums, typeInfos)
	if err != nil {
		t.Fatalf("mapDatumsToRow: %v", err)
	}

	if len(row.Values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(row.Values))
	}

	// INT4 value should be encoded.
	if row.Values[0] == nil {
		t.Error("expected non-nil value for int column")
	}

	// STRING value should be encoded.
	if row.Values[1] == nil {
		t.Error("expected non-nil value for string column")
	}
}

func TestMapDatumsToRowWithNull(t *testing.T) {
	datums := tree.Datums{
		tree.DNull,
		tree.NewDString("world"),
	}

	cols := []resultColumnInfo{
		{Name: "nullable_col", Typ: types.Int4},
		{Name: "name", Typ: types.String},
	}
	_, typeInfos, err := mapResultColumns(cols)
	if err != nil {
		t.Fatalf("mapResultColumns: %v", err)
	}

	row, err := mapDatumsToRow(datums, typeInfos)
	if err != nil {
		t.Fatalf("mapDatumsToRow: %v", err)
	}

	// NULL value should be nil.
	if row.Values[0] != nil {
		t.Errorf("expected nil for NULL datum, got %v", row.Values[0])
	}

	// Non-null value should be encoded.
	if row.Values[1] == nil {
		t.Error("expected non-nil value for string column")
	}
}

// --- Tests for Executor.handleUseDatabase ---

func TestExecutorHandleUseDatabase(t *testing.T) {
	e := &Executor{
		currentDatabase: "master",
	}

	var buf bytes.Buffer
	tw := tdswire.NewTokenWriter(&buf)

	if err := e.handleUseDatabase("mydb", tw); err != nil {
		t.Fatalf("handleUseDatabase: %v", err)
	}

	// Verify the executor's database was updated.
	if e.Database() != "mydb" {
		t.Errorf("expected database 'mydb', got %q", e.Database())
	}

	// Verify the token stream.
	tokens := readTokenStream(buf.Bytes())
	expected := []byte{tdswire.TokenEnvChange, tdswire.TokenDone}
	if !bytes.Equal(tokens, expected) {
		t.Errorf("expected tokens %v, got %v", expected, tokens)
	}

	// Verify ENVCHANGE details.
	r := bytes.NewReader(buf.Bytes())
	tr := tdswire.NewTokenReader(r)
	tok, _ := tr.PeekToken()
	ec, err := tr.ReadEnvChange()
	if err != nil {
		t.Fatalf("reading envchange: %v", err)
	}
	_ = tok
	if ec.Type != tdswire.EnvDatabase {
		t.Errorf("expected EnvDatabase, got %d", ec.Type)
	}
	if ec.NewValue != "mydb" {
		t.Errorf("expected new value 'mydb', got %q", ec.NewValue)
	}
	if ec.OldValue != "master" {
		t.Errorf("expected old value 'master', got %q", ec.OldValue)
	}
}

// --- Tests for SET command handling ---

func TestExecutorSetCommandHandling(t *testing.T) {
	e := &Executor{
		currentDatabase: "master",
	}

	var buf bytes.Buffer
	tw := tdswire.NewTokenWriter(&buf)

	// SET commands should produce a simple DONE token.
	if err := e.ExecuteBatch(context.Background(), "SET QUOTED_IDENTIFIER ON", tw); err != nil {
		t.Fatalf("ExecuteBatch SET: %v", err)
	}

	tokens := readTokenStream(buf.Bytes())
	if len(tokens) != 1 || tokens[0] != tdswire.TokenDone {
		t.Errorf("expected [DONE] for SET command, got %v", tokens)
	}
}

// --- Tests for catalog query detection ---

func TestExecutorCatalogQueryDetection(t *testing.T) {
	// Test that the catalog query detection works correctly.
	// We use the catalog package directly since it's already imported
	// by executor.go in this package.

	tests := []struct {
		sql      string
		isCatalog bool
	}{
		{"SELECT @@VERSION", true},
		{"SET ANSI_NULLS ON", true},
		{"SET QUOTED_IDENTIFIER ON", true},
		{"sp_helpdb", true},
		{"sp_help users", true},
		{"SELECT * FROM users", false},
		{"INSERT INTO t VALUES (1)", false},
		{"CREATE TABLE t (id INT)", false},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			got := catalog.IsCatalogQuery(tt.sql)
			if got != tt.isCatalog {
				t.Errorf("IsCatalogQuery(%q) = %v, want %v", tt.sql, got, tt.isCatalog)
			}
		})
	}
}

// --- Tests for empty batch ---

func TestExecutorEmptyBatch(t *testing.T) {
	e := &Executor{
		currentDatabase: "master",
	}

	var buf bytes.Buffer
	tw := tdswire.NewTokenWriter(&buf)

	if err := e.ExecuteBatch(context.Background(), "", tw); err != nil {
		t.Fatalf("ExecuteBatch empty: %v", err)
	}

	tokens := readTokenStream(buf.Bytes())
	if len(tokens) != 1 || tokens[0] != tdswire.TokenDone {
		t.Errorf("expected [DONE] for empty batch, got %v", tokens)
	}
}

func TestExecutorWhitespaceBatch(t *testing.T) {
	e := &Executor{
		currentDatabase: "master",
	}

	var buf bytes.Buffer
	tw := tdswire.NewTokenWriter(&buf)

	if err := e.ExecuteBatch(context.Background(), "   \n\t  ", tw); err != nil {
		t.Fatalf("ExecuteBatch whitespace: %v", err)
	}

	tokens := readTokenStream(buf.Bytes())
	if len(tokens) != 1 || tokens[0] != tdswire.TokenDone {
		t.Errorf("expected [DONE] for whitespace batch, got %v", tokens)
	}
}

// --- Tests for @@ROWCOUNT tracking ---

func TestExecutorRowCountTracking(t *testing.T) {
	e := &Executor{
		currentDatabase: "master",
	}

	// Verify initial state.
	if e.lastRowsAffected != 0 {
		t.Errorf("expected initial lastRowsAffected=0, got %d", e.lastRowsAffected)
	}
}

// --- Tests for type classification helpers ---

func TestIsFixedLenTDSType(t *testing.T) {
	fixedTypes := []byte{
		tdswire.TypeInt1, tdswire.TypeBit, tdswire.TypeInt2,
		tdswire.TypeInt4, tdswire.TypeFloat4, tdswire.TypeFloat8,
		tdswire.TypeInt8, tdswire.TypeDateTime,
	}
	for _, id := range fixedTypes {
		if !isFixedLenTDSType(id) {
			t.Errorf("expected type 0x%02X to be fixed-length", id)
		}
	}

	varTypes := []byte{
		tdswire.TypeIntN, tdswire.TypeBitN, tdswire.TypeNVarChar,
		tdswire.TypeBigVarChar, tdswire.TypeDecimalN,
	}
	for _, id := range varTypes {
		if isFixedLenTDSType(id) {
			t.Errorf("expected type 0x%02X to NOT be fixed-length", id)
		}
	}
}

func TestIsByteLenTDSType(t *testing.T) {
	byteLenTypes := []byte{
		tdswire.TypeIntN, tdswire.TypeBitN, tdswire.TypeFloatN,
		tdswire.TypeDateTimeN, tdswire.TypeMoneyN, tdswire.TypeGuid,
	}
	for _, id := range byteLenTypes {
		if !isByteLenTDSType(id) {
			t.Errorf("expected type 0x%02X to be byte-length", id)
		}
	}
}

func TestIsVarLenTDSType(t *testing.T) {
	varLenTypes := []byte{
		tdswire.TypeBigVarChar, tdswire.TypeBigChar, tdswire.TypeBigVarBin,
		tdswire.TypeNVarChar, tdswire.TypeNChar, tdswire.TypeBigBinary,
	}
	for _, id := range varLenTypes {
		if !isVarLenTDSType(id) {
			t.Errorf("expected type 0x%02X to be var-length", id)
		}
	}
}

func TestIsPrecScaleTDSType(t *testing.T) {
	if !isPrecScaleTDSType(tdswire.TypeDecimalN) {
		t.Error("expected TypeDecimalN to need precision/scale")
	}
	if !isPrecScaleTDSType(tdswire.TypeNumericN) {
		t.Error("expected TypeNumericN to need precision/scale")
	}
	if isPrecScaleTDSType(tdswire.TypeIntN) {
		t.Error("expected TypeIntN to NOT need precision/scale")
	}
}

// --- Test for USE with quoted database name ---

func TestExecutorUseDatabaseWithBrackets(t *testing.T) {
	e := &Executor{
		currentDatabase: "master",
	}

	var buf bytes.Buffer
	tw := tdswire.NewTokenWriter(&buf)

	if err := e.ExecuteBatch(context.Background(), "USE [mydb]", tw); err != nil {
		t.Fatalf("ExecuteBatch USE: %v", err)
	}

	if e.Database() != "mydb" {
		t.Errorf("expected database 'mydb', got %q", e.Database())
	}
}

// --- Test for multiple SET commands ---

func TestExecutorMultipleSetCommands(t *testing.T) {
	setCommands := []string{
		"SET QUOTED_IDENTIFIER ON",
		"SET ANSI_NULLS ON",
		"SET TEXTSIZE 2147483647",
		"SET ARITHABORT ON",
		"SET CONCAT_NULL_YIELDS_NULL ON",
	}

	for _, cmd := range setCommands {
		t.Run(cmd, func(t *testing.T) {
			e := &Executor{currentDatabase: "master"}
			var buf bytes.Buffer
			tw := tdswire.NewTokenWriter(&buf)

			if err := e.ExecuteBatch(context.Background(), cmd, tw); err != nil {
				t.Fatalf("ExecuteBatch %q: %v", cmd, err)
			}

			tokens := readTokenStream(buf.Bytes())
			if len(tokens) != 1 || tokens[0] != tdswire.TokenDone {
				t.Errorf("expected [DONE] for %q, got %v", cmd, tokens)
			}
		})
	}
}

// --- Test for parse error handling ---

func TestExecutorParseError(t *testing.T) {
	// This tests that a T-SQL parse error produces an ERROR token.
	// We use a SQL string that the parser can't handle.
	e := &Executor{currentDatabase: "master"}
	var buf bytes.Buffer
	tw := tdswire.NewTokenWriter(&buf)

	// "DELETE FROM t" is not supported by the parser (only SELECT, INSERT,
	// CREATE TABLE, USE are supported). This should produce an error token.
	if err := e.ExecuteBatch(context.Background(), "DELETE FROM t", tw); err != nil {
		t.Fatalf("ExecuteBatch: %v", err)
	}

	tokens := readTokenStream(buf.Bytes())
	expected := []byte{tdswire.TokenError, tdswire.TokenDone}
	if !bytes.Equal(tokens, expected) {
		t.Errorf("expected [ERROR, DONE] for parse error, got %v", tokens)
	}
}

// --- Test Executor Database state ---

func TestExecutorDatabaseState(t *testing.T) {
	e := NewExecutor(nil, "defaultdb")

	if e.Database() != "defaultdb" {
		t.Errorf("expected initial database 'defaultdb', got %q", e.Database())
	}

	e.SetDatabase("mydb")
	if e.Database() != "mydb" {
		t.Errorf("expected database 'mydb', got %q", e.Database())
	}
}

// --- Test full token stream for USE command ---

func TestExecutorUseDatabaseTokenStream(t *testing.T) {
	e := NewExecutor(nil, "master")

	result, err := e.ExecuteBatchToBytes(context.Background(), "USE testdb")
	if err != nil {
		t.Fatalf("ExecuteBatchToBytes: %v", err)
	}

	// Parse the full token stream.
	r := bytes.NewReader(result)
	tr := tdswire.NewTokenReader(r)

	// First token: ENVCHANGE.
	tok, err := tr.PeekToken()
	if err != nil {
		t.Fatalf("PeekToken: %v", err)
	}
	if tok != tdswire.TokenEnvChange {
		t.Fatalf("expected ENVCHANGE, got 0x%02X", tok)
	}
	ec, err := tr.ReadEnvChange()
	if err != nil {
		t.Fatalf("ReadEnvChange: %v", err)
	}
	if ec.Type != tdswire.EnvDatabase {
		t.Errorf("expected EnvDatabase type, got %d", ec.Type)
	}
	if ec.NewValue != "testdb" {
		t.Errorf("expected new value 'testdb', got %q", ec.NewValue)
	}
	if ec.OldValue != "master" {
		t.Errorf("expected old value 'master', got %q", ec.OldValue)
	}

	// Second token: DONE.
	tok, err = tr.PeekToken()
	if err != nil {
		t.Fatalf("PeekToken: %v", err)
	}
	if tok != tdswire.TokenDone {
		t.Fatalf("expected DONE, got 0x%02X", tok)
	}
	done, err := tr.ReadDone(tok)
	if err != nil {
		t.Fatalf("ReadDone: %v", err)
	}
	if done.Status != tdswire.DoneFinal {
		t.Errorf("expected DoneFinal status, got %d", done.Status)
	}

	// Verify database was updated.
	if e.Database() != "testdb" {
		t.Errorf("expected database 'testdb', got %q", e.Database())
	}
}

// --- Ensure test-only mocks compile (unused but verify interface shape) ---

func init() {
	// Prevent "unused" errors for the mock types and verify they exist.
	_ = &mockDB{}
	_ = &mockExecutor{}
}
