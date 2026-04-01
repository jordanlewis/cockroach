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

func TestHeaderMarshalUnmarshal(t *testing.T) {
	original := Header{
		Type:     PacketTypeLogin7,
		Status:   PacketStatus(StatusEOM),
		Length:   512,
		SPID:     1234,
		PacketID: 7,
		Window:   0,
	}

	buf := original.MarshalBinary()
	if len(buf) != HeaderSize {
		t.Fatalf("expected header size %d, got %d", HeaderSize, len(buf))
	}

	var decoded Header
	if err := decoded.UnmarshalBinary(buf); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	if decoded != original {
		t.Fatalf("round-trip mismatch:\n  original: %+v\n  decoded:  %+v", original, decoded)
	}
}

func TestHeaderBigEndian(t *testing.T) {
	hdr := Header{
		Type:     PacketTypePreLogin,
		Status:   PacketStatus(StatusEOM),
		Length:   0x0102,
		SPID:     0x0304,
		PacketID: 5,
		Window:   0,
	}

	buf := hdr.MarshalBinary()
	// Length should be big-endian: 0x01, 0x02
	if buf[2] != 0x01 || buf[3] != 0x02 {
		t.Fatalf("expected length bytes [0x01, 0x02], got [0x%02x, 0x%02x]", buf[2], buf[3])
	}
	// SPID should be big-endian: 0x03, 0x04
	if buf[4] != 0x03 || buf[5] != 0x04 {
		t.Fatalf("expected SPID bytes [0x03, 0x04], got [0x%02x, 0x%02x]", buf[4], buf[5])
	}
}

func TestPacketRoundTrip(t *testing.T) {
	payload := []byte("SELECT 1")
	var buf bytes.Buffer
	pw := NewPacketWriter(&buf, DefaultPacketSize)
	if err := pw.WriteMessage(PacketTypeSQLBatch, payload); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	pr := NewPacketReader(&buf)
	pktType, data, err := pr.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if pktType != PacketTypeSQLBatch {
		t.Fatalf("expected packet type %v, got %v", PacketTypeSQLBatch, pktType)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("payload mismatch:\n  expected: %q\n  got:      %q", payload, data)
	}
}

func TestMultiPacketReassembly(t *testing.T) {
	// Use a very small packet size to force multi-packet splitting.
	// Header is 8 bytes, so with 20 bytes max, we get 12 bytes of data per packet.
	const smallPacketSize = 20
	payload := make([]byte, 50)
	for i := range payload {
		payload[i] = byte(i)
	}

	var buf bytes.Buffer
	pw := NewPacketWriter(&buf, smallPacketSize)
	if err := pw.WriteMessage(PacketTypeSQLBatch, payload); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	// Verify we actually got multiple packets.
	totalBytes := buf.Len()
	maxDataPerPacket := smallPacketSize - HeaderSize
	expectedPackets := (len(payload) + maxDataPerPacket - 1) / maxDataPerPacket
	// Each packet has HeaderSize overhead.
	expectedBytes := expectedPackets*HeaderSize + len(payload)
	if totalBytes != expectedBytes {
		t.Fatalf("expected %d total bytes (%d packets), got %d bytes",
			expectedBytes, expectedPackets, totalBytes)
	}

	// Verify intermediate packets have NORMAL status and last has EOM.
	raw := buf.Bytes()
	offset := 0
	for i := 0; i < expectedPackets; i++ {
		var hdr Header
		if err := hdr.UnmarshalBinary(raw[offset:]); err != nil {
			t.Fatalf("packet %d: header unmarshal failed: %v", i, err)
		}
		if i < expectedPackets-1 {
			if hdr.Status != StatusNormal {
				t.Fatalf("packet %d: expected NORMAL status, got %d", i, hdr.Status)
			}
		} else {
			if hdr.Status&StatusEOM == 0 {
				t.Fatalf("last packet: expected EOM status flag, got %d", hdr.Status)
			}
		}
		offset += int(hdr.Length)
	}

	// Now read back via PacketReader.
	pr := NewPacketReader(bytes.NewReader(raw))
	pktType, data, err := pr.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if pktType != PacketTypeSQLBatch {
		t.Fatalf("expected packet type %v, got %v", PacketTypeSQLBatch, pktType)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("reassembled payload mismatch")
	}
}

func TestEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	pw := NewPacketWriter(&buf, DefaultPacketSize)
	if err := pw.WriteMessage(PacketTypeAttention, nil); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	pr := NewPacketReader(&buf)
	pktType, data, err := pr.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if pktType != PacketTypeAttention {
		t.Fatalf("expected ATTENTION, got %v", pktType)
	}
	if len(data) != 0 {
		t.Fatalf("expected empty payload, got %d bytes", len(data))
	}
}

func TestPreLoginRoundTrip(t *testing.T) {
	original := &PreLoginMsg{
		Options: []PreLoginOption{
			{Token: PreLoginVersion, Data: EncodeVersionData(PreLoginVersionData{
				Major: 15, Minor: 0, Build: 4033, SubBuild: 0,
			})},
			{Token: PreLoginEncryption, Data: []byte{byte(EncryptNotSup)}},
			{Token: PreLoginThreadID, Data: []byte{0x00, 0x00, 0x10, 0x00}},
			{Token: PreLoginMARS, Data: []byte{0x00}},
		},
	}

	encoded := EncodePreLogin(original)
	decoded, err := DecodePreLogin(encoded)
	if err != nil {
		t.Fatalf("DecodePreLogin failed: %v", err)
	}

	if len(decoded.Options) != len(original.Options) {
		t.Fatalf("option count mismatch: expected %d, got %d",
			len(original.Options), len(decoded.Options))
	}

	for i, opt := range decoded.Options {
		orig := original.Options[i]
		if opt.Token != orig.Token {
			t.Fatalf("option %d: token mismatch: expected %d, got %d", i, orig.Token, opt.Token)
		}
		if !bytes.Equal(opt.Data, orig.Data) {
			t.Fatalf("option %d: data mismatch:\n  expected: %v\n  got:      %v", i, orig.Data, opt.Data)
		}
	}
}

func TestPreLoginVersionData(t *testing.T) {
	v := PreLoginVersionData{Major: 15, Minor: 0, Build: 4033, SubBuild: 1}
	encoded := EncodeVersionData(v)
	if len(encoded) != 6 {
		t.Fatalf("expected 6 bytes, got %d", len(encoded))
	}

	decoded, err := DecodeVersionData(encoded)
	if err != nil {
		t.Fatalf("DecodeVersionData failed: %v", err)
	}
	if decoded != v {
		t.Fatalf("version round-trip mismatch: expected %+v, got %+v", v, decoded)
	}
}

func TestPreLoginEncryptionOption(t *testing.T) {
	msg := &PreLoginMsg{
		Options: []PreLoginOption{
			{Token: PreLoginEncryption, Data: []byte{byte(EncryptReq)}},
		},
	}

	encoded := EncodePreLogin(msg)
	decoded, err := DecodePreLogin(encoded)
	if err != nil {
		t.Fatalf("DecodePreLogin failed: %v", err)
	}
	if len(decoded.Options) != 1 {
		t.Fatalf("expected 1 option, got %d", len(decoded.Options))
	}
	if decoded.Options[0].Token != PreLoginEncryption {
		t.Fatalf("expected ENCRYPTION token, got %d", decoded.Options[0].Token)
	}
	if EncryptionLevel(decoded.Options[0].Data[0]) != EncryptReq {
		t.Fatalf("expected ENCRYPT_REQ, got %d", decoded.Options[0].Data[0])
	}
}

func TestPasswordObfuscation(t *testing.T) {
	// Test the round-trip: obfuscate then deobfuscate should return original.
	original := []byte("p@ssw0rd!123")
	buf := make([]byte, len(original))
	copy(buf, original)

	ObfuscatePassword(buf)
	// Obfuscated should differ from original.
	if bytes.Equal(buf, original) {
		t.Fatal("obfuscated password should differ from original")
	}
	deobfuscatePassword(buf)
	if !bytes.Equal(buf, original) {
		t.Fatalf("deobfuscated password mismatch:\n  expected: %v\n  got:      %v", original, buf)
	}
}

func TestPasswordDeobfuscationKnownValue(t *testing.T) {
	// Verify against a known obfuscated value.
	// The character 'a' (0x61):
	//   XOR with 0xA5: 0x61 ^ 0xA5 = 0xC4
	//   Swap nibbles: 0x4C
	// So obfuscated 'a' = 0x4C
	// Deobfuscation of 0x4C:
	//   Swap nibbles: 0xC4
	//   XOR with 0xA5: 0xC4 ^ 0xA5 = 0x61 = 'a'
	obfuscated := []byte{0x4C}
	deobfuscatePassword(obfuscated)
	if obfuscated[0] != 'a' {
		t.Fatalf("expected 'a' (0x61), got 0x%02x", obfuscated[0])
	}
}

func TestLogin7Decode(t *testing.T) {
	// Build a minimal LOGIN7 packet.
	hostname := "TESTHOST"
	username := "sa"
	password := "secret"
	appName := "TestApp"
	serverName := "localhost"
	libraryName := "go-tds"
	language := "us_english"
	database := "master"

	// Encode strings as UTF-16LE.
	hostBytes := encodeUTF16LE(hostname)
	userBytes := encodeUTF16LE(username)
	passBytes := encodeUTF16LE(password)
	appBytes := encodeUTF16LE(appName)
	serverBytes := encodeUTF16LE(serverName)
	libBytes := encodeUTF16LE(libraryName)
	langBytes := encodeUTF16LE(language)
	dbBytes := encodeUTF16LE(database)

	// Obfuscate password.
	obfPass := make([]byte, len(passBytes))
	copy(obfPass, passBytes)
	ObfuscatePassword(obfPass)

	// Variable data starts at offset 94 (Login7FixedLen).
	varOffset := Login7FixedLen
	allVarData := make([]byte, 0)
	type fieldInfo struct {
		offset int
		length int // in chars
	}
	fields := []fieldInfo{
		{varOffset, len(hostBytes) / 2},
		{varOffset + len(hostBytes), len(userBytes) / 2},
		{varOffset + len(hostBytes) + len(userBytes), len(obfPass) / 2},
		{varOffset + len(hostBytes) + len(userBytes) + len(obfPass), len(appBytes) / 2},
		{varOffset + len(hostBytes) + len(userBytes) + len(obfPass) + len(appBytes), len(serverBytes) / 2},
	}
	// Extension offset at position 56 (unused, just zero).
	extOffset := varOffset + len(hostBytes) + len(userBytes) + len(obfPass) + len(appBytes) + len(serverBytes)
	libField := fieldInfo{extOffset, len(libBytes) / 2}
	langField := fieldInfo{extOffset + len(libBytes), len(langBytes) / 2}
	dbField := fieldInfo{extOffset + len(libBytes) + len(langBytes), len(dbBytes) / 2}

	allVarData = append(allVarData, hostBytes...)
	allVarData = append(allVarData, userBytes...)
	allVarData = append(allVarData, obfPass...)
	allVarData = append(allVarData, appBytes...)
	allVarData = append(allVarData, serverBytes...)
	allVarData = append(allVarData, libBytes...)
	allVarData = append(allVarData, langBytes...)
	allVarData = append(allVarData, dbBytes...)

	totalLen := Login7FixedLen + len(allVarData)
	buf := make([]byte, totalLen)

	// Total length.
	binary.LittleEndian.PutUint32(buf[0:4], uint32(totalLen))
	// TDS version (7.4 = 0x74000004).
	binary.LittleEndian.PutUint32(buf[4:8], 0x74000004)
	// Packet size.
	binary.LittleEndian.PutUint32(buf[8:12], 4096)
	// Client version.
	binary.LittleEndian.PutUint32(buf[12:16], 0x00000007)
	// Client PID.
	binary.LittleEndian.PutUint32(buf[16:20], 12345)
	// Connection ID.
	binary.LittleEndian.PutUint32(buf[20:24], 0)
	// Option flags.
	buf[24] = 0xE0 // OptionFlags1
	buf[25] = 0x03 // OptionFlags2
	buf[26] = 0x00 // TypeFlags
	buf[27] = 0x00 // OptionFlags3
	// Timezone (signed, stored as uint32 two's complement).
	binary.LittleEndian.PutUint32(buf[28:32], 0xFFFFFED4) // -300 as uint32
	// Collation.
	binary.LittleEndian.PutUint32(buf[32:36], 0x00000409)

	// Variable-length field offset/length pairs (offset in bytes, length in chars).
	// hostname at fixed offset 36.
	binary.LittleEndian.PutUint16(buf[36:38], uint16(fields[0].offset))
	binary.LittleEndian.PutUint16(buf[38:40], uint16(fields[0].length))
	// username at fixed offset 40.
	binary.LittleEndian.PutUint16(buf[40:42], uint16(fields[1].offset))
	binary.LittleEndian.PutUint16(buf[42:44], uint16(fields[1].length))
	// password at fixed offset 44.
	binary.LittleEndian.PutUint16(buf[44:46], uint16(fields[2].offset))
	binary.LittleEndian.PutUint16(buf[46:48], uint16(fields[2].length))
	// appname at fixed offset 48.
	binary.LittleEndian.PutUint16(buf[48:50], uint16(fields[3].offset))
	binary.LittleEndian.PutUint16(buf[50:52], uint16(fields[3].length))
	// servername at fixed offset 52.
	binary.LittleEndian.PutUint16(buf[52:54], uint16(fields[4].offset))
	binary.LittleEndian.PutUint16(buf[54:56], uint16(fields[4].length))
	// extension (offset 56) - unused.
	binary.LittleEndian.PutUint16(buf[56:58], 0)
	binary.LittleEndian.PutUint16(buf[58:60], 0)
	// libraryname at fixed offset 60.
	binary.LittleEndian.PutUint16(buf[60:62], uint16(libField.offset))
	binary.LittleEndian.PutUint16(buf[62:64], uint16(libField.length))
	// language at fixed offset 64.
	binary.LittleEndian.PutUint16(buf[64:66], uint16(langField.offset))
	binary.LittleEndian.PutUint16(buf[66:68], uint16(langField.length))
	// database at fixed offset 68.
	binary.LittleEndian.PutUint16(buf[68:70], uint16(dbField.offset))
	binary.LittleEndian.PutUint16(buf[70:72], uint16(dbField.length))

	// Copy variable data.
	copy(buf[Login7FixedLen:], allVarData)

	// Decode.
	login, err := DecodeLogin7(buf)
	if err != nil {
		t.Fatalf("DecodeLogin7 failed: %v", err)
	}

	// Verify fields.
	if login.TDSVersion != 0x74000004 {
		t.Fatalf("TDSVersion: expected 0x74000004, got 0x%08x", login.TDSVersion)
	}
	if login.PacketSize != 4096 {
		t.Fatalf("PacketSize: expected 4096, got %d", login.PacketSize)
	}
	if login.ClientPID != 12345 {
		t.Fatalf("ClientPID: expected 12345, got %d", login.ClientPID)
	}
	if login.Timezone != -300 {
		t.Fatalf("Timezone: expected -300, got %d", login.Timezone)
	}
	if login.Hostname != hostname {
		t.Fatalf("Hostname: expected %q, got %q", hostname, login.Hostname)
	}
	if login.Username != username {
		t.Fatalf("Username: expected %q, got %q", username, login.Username)
	}
	if login.Password != password {
		t.Fatalf("Password: expected %q, got %q", password, login.Password)
	}
	if login.AppName != appName {
		t.Fatalf("AppName: expected %q, got %q", appName, login.AppName)
	}
	if login.ServerName != serverName {
		t.Fatalf("ServerName: expected %q, got %q", serverName, login.ServerName)
	}
	if login.LibraryName != libraryName {
		t.Fatalf("LibraryName: expected %q, got %q", libraryName, login.LibraryName)
	}
	if login.Language != language {
		t.Fatalf("Language: expected %q, got %q", language, login.Language)
	}
	if login.Database != database {
		t.Fatalf("Database: expected %q, got %q", database, login.Database)
	}
}

func TestLogin7TooShort(t *testing.T) {
	buf := make([]byte, 50)
	_, err := DecodeLogin7(buf)
	if err == nil {
		t.Fatal("expected error for short LOGIN7, got nil")
	}
}

func TestPacketTypeString(t *testing.T) {
	tests := []struct {
		pt   PacketType
		want string
	}{
		{PacketTypeSQLBatch, "SQL_BATCH"},
		{PacketTypeTabularResult, "TABULAR_RESULT"},
		{PacketTypeAttention, "ATTENTION"},
		{PacketTypeTransactionManager, "TRANSACTION_MANAGER"},
		{PacketTypeLogin7, "LOGIN7"},
		{PacketTypePreLogin, "PRELOGIN"},
		{PacketType(99), "UNKNOWN(99)"},
	}
	for _, tc := range tests {
		if got := tc.pt.String(); got != tc.want {
			t.Fatalf("PacketType(%d).String() = %q, want %q", tc.pt, got, tc.want)
		}
	}
}

func TestHeaderUnmarshalTooShort(t *testing.T) {
	var hdr Header
	err := hdr.UnmarshalBinary([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for short header, got nil")
	}
}

func TestPacketWriterIncrementingPacketID(t *testing.T) {
	// Verify that each packet gets an incrementing packet ID.
	const smallPacketSize = 16 // 8 header + 8 data
	payload := make([]byte, 20)
	var buf bytes.Buffer
	pw := NewPacketWriter(&buf, smallPacketSize)
	if err := pw.WriteMessage(PacketTypeSQLBatch, payload); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	raw := buf.Bytes()
	offset := 0
	expectedID := uint8(0)
	for offset < len(raw) {
		var hdr Header
		if err := hdr.UnmarshalBinary(raw[offset:]); err != nil {
			t.Fatalf("header unmarshal at offset %d: %v", offset, err)
		}
		if hdr.PacketID != expectedID {
			t.Fatalf("packet at offset %d: expected ID %d, got %d", offset, expectedID, hdr.PacketID)
		}
		expectedID++
		offset += int(hdr.Length)
	}
}
