// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package auth implements the server-side Oracle TNS authentication protocol,
// including the CONNECT/ACCEPT handshake, O5LOGON challenge-response
// authentication, and session setup (charset negotiation and NLS parameter
// exchange).
//
// The O5LOGON protocol is Oracle's password-based authentication mechanism.
// The server generates a random session key and salt, sends them to the
// client, and the client responds with an encrypted password derived from
// the shared key material. The server verifies the response by decrypting
// the password and checking it against the stored credentials.
//
// Session setup follows authentication: the server and client negotiate a
// character set (AL32UTF8) and exchange NLS (National Language Support)
// parameters that govern date formats, numeric formats, and similar
// locale-specific behavior.
package auth

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/tns/tnswire"
	"github.com/cockroachdb/errors"
)

// TNS protocol version constants.
const (
	// ProtocolVersion is the TNS protocol version we advertise. Version 314
	// corresponds to Oracle 12c and later, which is the minimum version that
	// supports O5LOGON.
	ProtocolVersion = 314

	// MinProtocolVersion is the oldest version we accept from clients.
	MinProtocolVersion = 300
)

// SDU/TDU size constants.
const (
	DefaultSDUSize = 8192
	DefaultTDUSize = 32767
)

// TTI function codes used during authentication. These extend the data
// protocol's function codes with auth-specific operations.
const (
	// TTIProtocolNeg is sent by the client to negotiate protocol features.
	TTIProtocolNeg tnswire.TTIFuncCode = 0x01

	// TTIDataTypeNeg is sent by the client to negotiate data types and
	// character sets.
	TTIDataTypeNeg tnswire.TTIFuncCode = 0x02

	// TTIAuth is the function code for O5LOGON authentication messages.
	// Both the client's initial auth request and the server's challenge
	// use this code, distinguished by message structure.
	TTIAuth tnswire.TTIFuncCode = 0x76

	// TTIAuthResponse is the function code for the server's authentication
	// response (success or failure).
	TTIAuthResponse tnswire.TTIFuncCode = 0x08
)

// O5LOGON key-value pair keys used in the authentication exchange.
const (
	authKeyUsername      = "AUTH_TERMINAL"
	authKeyProgramName   = "AUTH_PROGRAM_NM"
	authKeyMachine       = "AUTH_MACHINE"
	authKeyPID           = "AUTH_PID"
	authKeySID           = "AUTH_SID"
	authKeyACE           = "AUTH_ACE"
	authKeySessKey       = "AUTH_SESSKEY"
	authKeyVFRData       = "AUTH_VFR_DATA"
	authKeyPassword      = "AUTH_PASSWORD"
	authKeyDBName        = "AUTH_DBNAME"
	authKeyAlterSession  = "AUTH_ALTER_SESSION"
	authKeyGlobalDBName  = "AUTH_GLOBALLY_UNIQUE_DBID"
)

// sessKeyLen is the length of the hex-encoded server/client session key.
// O5LOGON uses 48 raw bytes (96 hex chars).
const sessKeyLen = 48

// saltLen is the length of the password salt/verifier data.
const saltLen = 10

// NLSParam represents a single National Language Support parameter.
type NLSParam struct {
	Name  string
	Value string
}

// DefaultNLSParams returns the NLS parameters used for session setup.
// These configure date/time formats, numeric formats, and other
// locale-specific behaviors to match Oracle's default UTF8 configuration.
func DefaultNLSParams() []NLSParam {
	return []NLSParam{
		{Name: "NLS_LANGUAGE", Value: "AMERICAN"},
		{Name: "NLS_TERRITORY", Value: "AMERICA"},
		{Name: "NLS_CURRENCY", Value: "$"},
		{Name: "NLS_ISO_CURRENCY", Value: "AMERICA"},
		{Name: "NLS_NUMERIC_CHARACTERS", Value: ".,"},
		{Name: "NLS_DATE_FORMAT", Value: "DD-MON-RR"},
		{Name: "NLS_DATE_LANGUAGE", Value: "AMERICAN"},
		{Name: "NLS_CHARACTERSET", Value: "AL32UTF8"},
		{Name: "NLS_SORT", Value: "BINARY"},
		{Name: "NLS_COMP", Value: "BINARY"},
		{Name: "NLS_NCHAR_CHARACTERSET", Value: "AL16UTF16"},
		{Name: "NLS_TIMESTAMP_FORMAT", Value: "DD-MON-RR HH.MI.SSXFF AM"},
		{Name: "NLS_TIMESTAMP_TZ_FORMAT", Value: "DD-MON-RR HH.MI.SSXFF AM TZR"},
	}
}

// charsetID is the Oracle character set ID for AL32UTF8.
const charsetID uint16 = 873

// Handshaker performs the TNS connection handshake and authentication.
// It reads/writes TNS packets on the underlying connection and manages
// the authentication state machine.
type Handshaker struct {
	conn io.ReadWriter

	// PasswordVerifier is called with the username and cleartext password
	// extracted from the O5LOGON exchange. It returns nil if the credentials
	// are valid.
	PasswordVerifier func(username, password string) error

	// ConnectData is the connect descriptor received from the client during
	// the CONNECT handshake. Populated after Handshake completes.
	ConnectData string

	// Username is the authenticated user. Populated after Handshake completes.
	Username string
}

// Handshake performs the full TNS authentication sequence:
//  1. CONNECT/ACCEPT packet exchange
//  2. Protocol negotiation
//  3. Data type and charset negotiation
//  4. O5LOGON challenge-response authentication
//  5. NLS parameter exchange
//
// On success, it populates h.ConnectData and h.Username and returns nil.
func (h *Handshaker) Handshake() error {
	if err := h.handleConnect(); err != nil {
		return errors.Wrap(err, "TNS connect")
	}
	if err := h.handleProtocolNeg(); err != nil {
		return errors.Wrap(err, "protocol negotiation")
	}
	if err := h.handleDataTypeNeg(); err != nil {
		return errors.Wrap(err, "data type negotiation")
	}
	if err := h.handleAuth(); err != nil {
		return errors.Wrap(err, "authentication")
	}
	return nil
}

// handleConnect reads the CONNECT packet from the client, validates it,
// and sends an ACCEPT response.
func (h *Handshaker) handleConnect() error {
	hdr, payload, err := tnswire.ReadPacket(h.conn)
	if err != nil {
		return err
	}
	if hdr.Type != tnswire.PacketTypeConnect {
		return errors.Newf(
			"expected CONNECT packet, got %s", hdr.Type,
		)
	}
	conn, err := tnswire.DecodeConnect(payload)
	if err != nil {
		return err
	}
	if conn.Version < MinProtocolVersion {
		refusePayload := tnswire.EncodeRefuse(tnswire.RefusePacket{
			SystemReason: 0,
			UserReason:   4, // version mismatch
			Data:         "protocol version not supported",
		})
		_ = tnswire.WritePacket(h.conn, tnswire.PacketTypeRefuse, refusePayload)
		return errors.Newf(
			"client version %d below minimum %d", conn.Version, MinProtocolVersion,
		)
	}
	h.ConnectData = conn.ConnectData

	// Negotiate version: use the lower of our version and the client's.
	negotiatedVersion := ProtocolVersion
	if conn.Version < negotiatedVersion {
		negotiatedVersion = conn.Version
	}

	acceptPayload := tnswire.EncodeAccept(tnswire.AcceptPacket{
		Version:        uint16(negotiatedVersion),
		ServiceOptions: conn.ServiceOptions,
		SDUSize:        DefaultSDUSize,
		TDUSize:        DefaultTDUSize,
		ValueOfOne:     1,
		ConnectFlags0:  conn.ConnectFlags0,
		ConnectFlags1:  conn.ConnectFlags1,
	})
	return tnswire.WritePacket(h.conn, tnswire.PacketTypeAccept, acceptPayload)
}

// handleProtocolNeg reads the protocol negotiation DATA packet and sends
// a response. Protocol negotiation establishes the TTI capabilities each
// side supports.
func (h *Handshaker) handleProtocolNeg() error {
	ttiPayload, err := h.readDataPayload()
	if err != nil {
		return err
	}
	if len(ttiPayload) < 1 {
		return errors.New("empty protocol negotiation payload")
	}
	funcCode := tnswire.TTIFuncCode(ttiPayload[0])
	if funcCode != TTIProtocolNeg {
		return errors.Newf(
			"expected protocol negotiation (0x%02x), got 0x%02x",
			byte(TTIProtocolNeg), byte(funcCode),
		)
	}

	// Respond with our protocol negotiation. The response echoes the
	// function code and includes our supported protocol version bytes.
	resp := []byte{
		byte(TTIProtocolNeg),
		0x06,       // protocol version
		0x00, 0x00, // flags
		0x00, 0x00, // server banner length (none)
		byte(charsetID >> 8), byte(charsetID), // character set
	}
	return h.writeDataPayload(resp)
}

// handleDataTypeNeg reads the data type negotiation packet and responds
// with the server's supported types and character set.
func (h *Handshaker) handleDataTypeNeg() error {
	ttiPayload, err := h.readDataPayload()
	if err != nil {
		return err
	}
	if len(ttiPayload) < 1 {
		return errors.New("empty data type negotiation payload")
	}
	funcCode := tnswire.TTIFuncCode(ttiPayload[0])
	if funcCode != TTIDataTypeNeg {
		return errors.Newf(
			"expected data type negotiation (0x%02x), got 0x%02x",
			byte(TTIDataTypeNeg), byte(funcCode),
		)
	}

	// Respond with server's data type capabilities. The payload encodes the
	// character set (AL32UTF8 = 873) and the server's supported data types.
	resp := encodeDataTypeNegResponse()
	return h.writeDataPayload(resp)
}

// handleAuth performs the O5LOGON challenge-response authentication. The
// sequence is:
//  1. Client sends initial auth request with username and client parameters
//  2. Server generates session key and salt, sends challenge
//  3. Client sends encrypted password response
//  4. Server decrypts and verifies the password
func (h *Handshaker) handleAuth() error {
	// Step 1: Read client's initial auth request.
	ttiPayload, err := h.readDataPayload()
	if err != nil {
		return err
	}
	if len(ttiPayload) < 1 {
		return errors.New("empty auth payload")
	}
	funcCode := tnswire.TTIFuncCode(ttiPayload[0])
	if funcCode != TTIAuth {
		return errors.Newf(
			"expected auth request (0x%02x), got 0x%02x",
			byte(TTIAuth), byte(funcCode),
		)
	}

	kvPairs, err := decodeAuthKVPairs(ttiPayload[1:])
	if err != nil {
		return errors.Wrap(err, "decoding auth request")
	}
	username := kvPairs[authKeyACE]
	if username == "" {
		return errors.New("AUTH_ACE (username) missing from auth request")
	}
	h.Username = strings.ToLower(username)

	// Step 2: Generate server session key and salt, send challenge.
	serverSessKey := make([]byte, sessKeyLen)
	if _, err := rand.Read(serverSessKey); err != nil {
		return errors.Wrap(err, "generating server session key")
	}
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return errors.Wrap(err, "generating salt")
	}

	challengeKV := map[string]string{
		authKeySessKey:      hex.EncodeToString(serverSessKey),
		authKeyVFRData:      hex.EncodeToString(salt),
		authKeyGlobalDBName: "CockroachDB",
	}
	challengePayload := append(
		[]byte{byte(TTIAuth)}, encodeAuthKVPairs(challengeKV)...,
	)
	if err := h.writeDataPayload(challengePayload); err != nil {
		return errors.Wrap(err, "sending auth challenge")
	}

	// Step 3: Read client's auth response with encrypted password.
	ttiPayload, err = h.readDataPayload()
	if err != nil {
		return errors.Wrap(err, "reading auth response")
	}
	if len(ttiPayload) < 1 {
		return errors.New("empty auth response payload")
	}
	funcCode = tnswire.TTIFuncCode(ttiPayload[0])
	if funcCode != TTIAuth {
		return errors.Newf(
			"expected auth response (0x%02x), got 0x%02x",
			byte(TTIAuth), byte(funcCode),
		)
	}

	respKV, err := decodeAuthKVPairs(ttiPayload[1:])
	if err != nil {
		return errors.Wrap(err, "decoding auth response")
	}

	// Step 4: Decrypt and verify the password.
	encryptedPwd := respKV[authKeyPassword]
	clientSessKeyHex := respKV[authKeySessKey]
	if encryptedPwd == "" || clientSessKeyHex == "" {
		return errors.New("missing AUTH_PASSWORD or AUTH_SESSKEY in auth response")
	}

	cleartext, err := decryptO5LOGONPassword(
		serverSessKey, clientSessKeyHex, encryptedPwd, salt,
	)
	if err != nil {
		if sendErr := h.sendAuthFailure("ORA-01017: invalid username/password"); sendErr != nil {
			return errors.CombineErrors(err, sendErr)
		}
		return errors.Wrap(err, "O5LOGON password decryption")
	}

	// Verify the password against the stored credentials.
	if h.PasswordVerifier != nil {
		if err := h.PasswordVerifier(h.Username, cleartext); err != nil {
			if sendErr := h.sendAuthFailure("ORA-01017: invalid username/password"); sendErr != nil {
				return errors.CombineErrors(err, sendErr)
			}
			return err
		}
	}

	// Step 5: Send auth success and NLS parameters.
	return h.sendAuthSuccess()
}

// sendAuthSuccess sends the auth success response followed by NLS parameters.
func (h *Handshaker) sendAuthSuccess() error {
	// Auth OK response: function code + status byte (0 = success) +
	// NLS parameter count + NLS key-value pairs.
	nlsParams := DefaultNLSParams()
	kvPairs := make(map[string]string, len(nlsParams)+1)
	kvPairs[authKeyAlterSession] = ""
	for _, p := range nlsParams {
		kvPairs[p.Name] = p.Value
	}

	successPayload := append(
		[]byte{byte(TTIAuthResponse), 0x00}, // func code + success status
		encodeAuthKVPairs(kvPairs)...,
	)
	return h.writeDataPayload(successPayload)
}

// sendAuthFailure sends an auth failure response with the given error message.
func (h *Handshaker) sendAuthFailure(msg string) error {
	msgBytes := []byte(msg)
	failPayload := make([]byte, 1+1+2+len(msgBytes))
	failPayload[0] = byte(TTIAuthResponse)
	failPayload[1] = 0x01 // failure status
	binary.BigEndian.PutUint16(failPayload[2:4], uint16(len(msgBytes)))
	copy(failPayload[4:], msgBytes)
	return h.writeDataPayload(failPayload)
}

// readDataPayload reads a DATA packet and returns the TTI payload (after
// the 2-byte data flags).
func (h *Handshaker) readDataPayload() ([]byte, error) {
	hdr, payload, err := tnswire.ReadPacket(h.conn)
	if err != nil {
		return nil, err
	}
	if hdr.Type != tnswire.PacketTypeData {
		return nil, errors.Newf("expected DATA packet, got %s", hdr.Type)
	}
	dataPkt, err := tnswire.DecodeData(payload)
	if err != nil {
		return nil, err
	}
	return dataPkt.Payload, nil
}

// writeDataPayload wraps a TTI payload in a DATA packet and writes it.
func (h *Handshaker) writeDataPayload(ttiPayload []byte) error {
	dataPayload := tnswire.EncodeData(tnswire.DataPacket{
		Flags:   0,
		Payload: ttiPayload,
	})
	return tnswire.WritePacket(h.conn, tnswire.PacketTypeData, dataPayload)
}

// encodeDataTypeNegResponse builds the server's data type negotiation response.
// It advertises AL32UTF8 (charset ID 873) and the standard Oracle data types.
func encodeDataTypeNegResponse() []byte {
	// The response starts with the function code, then encodes the server's
	// character set and a list of supported data type representations.
	buf := make([]byte, 0, 64)
	buf = append(buf, byte(TTIDataTypeNeg))

	// Character set: AL32UTF8 (873).
	buf = append(buf, byte(charsetID>>8), byte(charsetID))

	// Server flags and capabilities (simplified).
	buf = append(buf,
		0x01,       // server charset form (1 = implicit)
		0x01,       // compiletime charset form
		0x00, 0x01, // ncharset ID (AL16UTF16 = 2000, but simplified)
	)
	return buf
}

// decodeAuthKVPairs decodes the O5LOGON key-value pair encoding from the
// given byte slice. The format is:
//
//	uint16: number of pairs
//	Per pair:
//	  uint16: key length
//	  []byte: key (UTF-8)
//	  uint16: value length
//	  []byte: value (UTF-8)
func decodeAuthKVPairs(data []byte) (map[string]string, error) {
	if len(data) < 2 {
		return nil, errors.New("auth KV data too short for pair count")
	}
	numPairs := int(binary.BigEndian.Uint16(data[0:2]))
	result := make(map[string]string, numPairs)
	off := 2
	for i := range numPairs {
		if off+2 > len(data) {
			return nil, errors.Newf("auth KV truncated at key length of pair %d", i)
		}
		keyLen := int(binary.BigEndian.Uint16(data[off : off+2]))
		off += 2
		if off+keyLen > len(data) {
			return nil, errors.Newf("auth KV truncated at key of pair %d", i)
		}
		key := string(data[off : off+keyLen])
		off += keyLen
		if off+2 > len(data) {
			return nil, errors.Newf("auth KV truncated at value length of pair %d", i)
		}
		valLen := int(binary.BigEndian.Uint16(data[off : off+2]))
		off += 2
		if off+valLen > len(data) {
			return nil, errors.Newf("auth KV truncated at value of pair %d", i)
		}
		result[key] = string(data[off : off+valLen])
		off += valLen
	}
	return result, nil
}

// encodeAuthKVPairs encodes key-value pairs in the O5LOGON wire format.
func encodeAuthKVPairs(pairs map[string]string) []byte {
	// Calculate total size.
	size := 2 // pair count
	for k, v := range pairs {
		size += 2 + len(k) + 2 + len(v)
	}
	buf := make([]byte, size)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(pairs)))
	off := 2
	for k, v := range pairs {
		binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(k)))
		off += 2
		copy(buf[off:], k)
		off += len(k)
		binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(v)))
		off += 2
		copy(buf[off:], v)
		off += len(v)
	}
	return buf
}

// decryptO5LOGONPassword decrypts the client's encrypted password from the
// O5LOGON exchange.
//
// The combined key is derived by XORing the server session key with the
// client session key, then hashing with SHA-1. The result is used as an
// AES-192-CBC key to decrypt the password.
func decryptO5LOGONPassword(
	serverSessKey []byte, clientSessKeyHex string, encryptedPwdHex string, salt []byte,
) (string, error) {
	clientSessKey, err := hex.DecodeString(clientSessKeyHex)
	if err != nil {
		return "", errors.Wrap(err, "decoding client session key")
	}

	encryptedPwd, err := hex.DecodeString(encryptedPwdHex)
	if err != nil {
		return "", errors.Wrap(err, "decoding encrypted password")
	}

	// Derive the combined key by XORing server and client session keys.
	combinedLen := len(serverSessKey)
	if len(clientSessKey) < combinedLen {
		combinedLen = len(clientSessKey)
	}
	combined := make([]byte, combinedLen)
	for i := range combinedLen {
		combined[i] = serverSessKey[i] ^ clientSessKey[i]
	}

	// Hash the combined key with SHA-1 to produce the encryption key.
	// O5LOGON also mixes in the salt.
	h := sha1.New()
	h.Write(combined)
	h.Write(salt)
	keyHash := h.Sum(nil)

	// AES-192-CBC requires a 24-byte key. We pad the 20-byte SHA-1 hash
	// with 4 zero bytes.
	aesKey := make([]byte, 24)
	copy(aesKey, keyHash)

	if len(encryptedPwd) < aes.BlockSize {
		return "", errors.New("encrypted password too short")
	}

	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return "", errors.Wrap(err, "creating AES cipher")
	}

	// The IV is the first AES block (16 bytes) of the encrypted password.
	iv := encryptedPwd[:aes.BlockSize]
	ciphertext := encryptedPwd[aes.BlockSize:]

	if len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return "", errors.New("invalid encrypted password length")
	}

	mode := cipher.NewCBCDecrypter(block, iv)
	plaintext := make([]byte, len(ciphertext))
	mode.CryptBlocks(plaintext, ciphertext)

	// Remove PKCS7 padding.
	plaintext, err = removePKCS7Padding(plaintext)
	if err != nil {
		return "", errors.Wrap(err, "removing padding")
	}

	return string(plaintext), nil
}

// removePKCS7Padding removes PKCS#7 padding from the plaintext.
func removePKCS7Padding(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("empty plaintext")
	}
	padLen := int(data[len(data)-1])
	if padLen == 0 || padLen > aes.BlockSize || padLen > len(data) {
		return nil, errors.New("invalid PKCS7 padding")
	}
	for i := len(data) - padLen; i < len(data); i++ {
		if data[i] != byte(padLen) {
			return nil, errors.New("invalid PKCS7 padding bytes")
		}
	}
	return data[:len(data)-padLen], nil
}

// EncryptO5LOGONPassword encrypts a cleartext password using the O5LOGON
// protocol for the client side of the exchange. This is primarily used in
// testing.
func EncryptO5LOGONPassword(
	serverSessKeyHex string, clientSessKey []byte, password string, saltHex string,
) (encryptedPwdHex string, clientSessKeyHex string, err error) {
	serverSessKey, err := hex.DecodeString(serverSessKeyHex)
	if err != nil {
		return "", "", errors.Wrap(err, "decoding server session key")
	}
	salt, err := hex.DecodeString(saltHex)
	if err != nil {
		return "", "", errors.Wrap(err, "decoding salt")
	}

	// Derive the combined key.
	combinedLen := len(serverSessKey)
	if len(clientSessKey) < combinedLen {
		combinedLen = len(clientSessKey)
	}
	combined := make([]byte, combinedLen)
	for i := range combinedLen {
		combined[i] = serverSessKey[i] ^ clientSessKey[i]
	}

	// Hash to get AES key.
	h := sha1.New()
	h.Write(combined)
	h.Write(salt)
	keyHash := h.Sum(nil)

	aesKey := make([]byte, 24)
	copy(aesKey, keyHash)

	block, err := aes.NewCipher(aesKey)
	if err != nil {
		return "", "", errors.Wrap(err, "creating AES cipher")
	}

	// Add PKCS7 padding.
	plaintext := []byte(password)
	padLen := aes.BlockSize - (len(plaintext) % aes.BlockSize)
	for i := 0; i < padLen; i++ {
		plaintext = append(plaintext, byte(padLen))
	}

	// Generate random IV.
	iv := make([]byte, aes.BlockSize)
	if _, err := rand.Read(iv); err != nil {
		return "", "", errors.Wrap(err, "generating IV")
	}

	mode := cipher.NewCBCEncrypter(block, iv)
	ciphertext := make([]byte, len(plaintext))
	mode.CryptBlocks(ciphertext, plaintext)

	// Prepend IV to ciphertext.
	encrypted := append(iv, ciphertext...)

	return hex.EncodeToString(encrypted),
		hex.EncodeToString(clientSessKey), nil
}
