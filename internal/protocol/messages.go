package protocol

import (
	"encoding/binary"
	"fmt"
)

// HandshakeRequest is sent by the client to initiate a connection.
type HandshakeRequest struct {
	ProtocolVersion uint16
	ClientVersion   string
	ClientInfo      string
	Extensions      []string // Supported extensions
}

// Encode encodes the handshake request to bytes.
func (r *HandshakeRequest) Encode() []byte {
	// Calculate size
	size := 2 + // ProtocolVersion
		2 + len(r.ClientVersion) + // ClientVersion
		2 + len(r.ClientInfo) + // ClientInfo
		2 + len(r.Extensions)*2 // Extensions count + each string length

	for _, ext := range r.Extensions {
		size += len(ext)
	}

	buf := make([]byte, size)
	offset := 0

	// ProtocolVersion
	binary.BigEndian.PutUint16(buf[offset:], r.ProtocolVersion)
	offset += 2

	// ClientVersion
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(r.ClientVersion)))
	offset += 2
	copy(buf[offset:], r.ClientVersion)
	offset += len(r.ClientVersion)

	// ClientInfo
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(r.ClientInfo)))
	offset += 2
	copy(buf[offset:], r.ClientInfo)
	offset += len(r.ClientInfo)

	// Extensions count
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(r.Extensions)))
	offset += 2

	// Extensions
	for _, ext := range r.Extensions {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(ext)))
		offset += 2
		copy(buf[offset:], ext)
		offset += len(ext)
	}

	return buf
}

// DecodeHandshakeRequest decodes a handshake request from bytes.
func DecodeHandshakeRequest(payload []byte) (*HandshakeRequest, error) {
	if len(payload) < 6 {
		return nil, fmt.Errorf("payload too short for handshake request")
	}

	r := &HandshakeRequest{}
	offset := 0

	// ProtocolVersion
	r.ProtocolVersion = binary.BigEndian.Uint16(payload[offset:])
	offset += 2

	// ClientVersion
	verLen := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	if offset+verLen > len(payload) {
		return nil, fmt.Errorf("invalid client version length")
	}
	r.ClientVersion = string(payload[offset : offset+verLen])
	offset += verLen

	// ClientInfo
	if offset+2 > len(payload) {
		return nil, fmt.Errorf("payload truncated at client info")
	}
	infoLen := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	if offset+infoLen > len(payload) {
		return nil, fmt.Errorf("invalid client info length")
	}
	r.ClientInfo = string(payload[offset : offset+infoLen])
	offset += infoLen

	// Extensions
	if offset+2 > len(payload) {
		// Extensions are optional
		return r, nil
	}
	extCount := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	r.Extensions = make([]string, 0, extCount)
	for i := 0; i < extCount; i++ {
		if offset+2 > len(payload) {
			return nil, fmt.Errorf("payload truncated at extension %d", i)
		}
		extLen := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		if offset+extLen > len(payload) {
			return nil, fmt.Errorf("invalid extension %d length", i)
		}
		r.Extensions = append(r.Extensions, string(payload[offset:offset+extLen]))
		offset += extLen
	}

	return r, nil
}

// HandshakeResponse is sent by the server in response to a handshake.
type HandshakeResponse struct {
	ProtocolVersion uint16
	ServerVersion   string
	Supported       bool
	Downgrade       bool
	AuthChallenge   []byte // Challenge for authentication
	Extensions      []string
}

// Encode encodes the handshake response to bytes.
func (r *HandshakeResponse) Encode() []byte {
	// Calculate size
	supported := byte(0)
	if r.Supported {
		supported = 1
	}
	downgrade := byte(0)
	if r.Downgrade {
		downgrade = 1
	}

	size := 2 + // ProtocolVersion
		2 + len(r.ServerVersion) + // ServerVersion
		1 + // Supported
		1 + // Downgrade
		1 + len(r.AuthChallenge) + // AuthChallenge
		2 // Extensions count

	for _, ext := range r.Extensions {
		size += 2 + len(ext)
	}

	buf := make([]byte, size)
	offset := 0

	// ProtocolVersion
	binary.BigEndian.PutUint16(buf[offset:], r.ProtocolVersion)
	offset += 2

	// ServerVersion
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(r.ServerVersion)))
	offset += 2
	copy(buf[offset:], r.ServerVersion)
	offset += len(r.ServerVersion)

	// Supported
	buf[offset] = supported
	offset += 1

	// Downgrade
	buf[offset] = downgrade
	offset += 1

	// AuthChallenge
	buf[offset] = byte(len(r.AuthChallenge))
	offset += 1
	copy(buf[offset:], r.AuthChallenge)
	offset += len(r.AuthChallenge)

	// Extensions count
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(r.Extensions)))
	offset += 2

	// Extensions
	for _, ext := range r.Extensions {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(ext)))
		offset += 2
		copy(buf[offset:], ext)
		offset += len(ext)
	}

	return buf
}

// DecodeHandshakeResponse decodes a handshake response from bytes.
func DecodeHandshakeResponse(payload []byte) (*HandshakeResponse, error) {
	if len(payload) < 8 {
		return nil, fmt.Errorf("payload too short for handshake response")
	}

	r := &HandshakeResponse{}
	offset := 0

	// ProtocolVersion
	r.ProtocolVersion = binary.BigEndian.Uint16(payload[offset:])
	offset += 2

	// ServerVersion
	verLen := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	if offset+verLen > len(payload) {
		return nil, fmt.Errorf("invalid server version length")
	}
	r.ServerVersion = string(payload[offset : offset+verLen])
	offset += verLen

	// Supported
	r.Supported = payload[offset] == 1
	offset += 1

	// Downgrade
	r.Downgrade = payload[offset] == 1
	offset += 1

	// AuthChallenge
	challengeLen := int(payload[offset])
	offset += 1
	if offset+challengeLen > len(payload) {
		return nil, fmt.Errorf("invalid auth challenge length")
	}
	r.AuthChallenge = make([]byte, challengeLen)
	copy(r.AuthChallenge, payload[offset:offset+challengeLen])
	offset += challengeLen

	// Extensions (optional)
	if offset+2 > len(payload) {
		return r, nil
	}
	extCount := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	r.Extensions = make([]string, 0, extCount)
	for i := 0; i < extCount; i++ {
		if offset+2 > len(payload) {
			return nil, fmt.Errorf("payload truncated at extension %d", i)
		}
		extLen := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		if offset+extLen > len(payload) {
			return nil, fmt.Errorf("invalid extension %d length", i)
		}
		r.Extensions = append(r.Extensions, string(payload[offset:offset+extLen]))
		offset += extLen
	}

	return r, nil
}

// AuthRequest is sent by the client to authenticate.
type AuthRequest struct {
	Username string
	Password []byte // Hashed or encrypted password
	Database string // Optional database to connect to
}

// Encode encodes the auth request to bytes.
func (r *AuthRequest) Encode() []byte {
	size := 1 + len(r.Username) + // Username
		1 + len(r.Password) + // Password
		2 + len(r.Database) // Database

	buf := make([]byte, size)
	offset := 0

	// Username
	buf[offset] = byte(len(r.Username))
	offset += 1
	copy(buf[offset:], r.Username)
	offset += len(r.Username)

	// Password
	buf[offset] = byte(len(r.Password))
	offset += 1
	copy(buf[offset:], r.Password)
	offset += len(r.Password)

	// Database
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(r.Database)))
	offset += 2
	copy(buf[offset:], r.Database)

	return buf
}

// DecodeAuthRequest decodes an auth request from bytes.
func DecodeAuthRequest(payload []byte) (*AuthRequest, error) {
	if len(payload) < 2 {
		return nil, fmt.Errorf("payload too short for auth request")
	}

	r := &AuthRequest{}
	offset := 0

	// Username
	userLen := int(payload[offset])
	offset += 1
	if offset+userLen > len(payload) {
		return nil, fmt.Errorf("invalid username length")
	}
	r.Username = string(payload[offset : offset+userLen])
	offset += userLen

	// Password
	passLen := int(payload[offset])
	offset += 1
	if offset+passLen > len(payload) {
		return nil, fmt.Errorf("invalid password length")
	}
	r.Password = make([]byte, passLen)
	copy(r.Password, payload[offset:offset+passLen])
	offset += passLen

	// Database (optional)
	if offset+2 > len(payload) {
		return r, nil
	}
	dbLen := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	if offset+dbLen > len(payload) {
		return nil, fmt.Errorf("invalid database length")
	}
	r.Database = string(payload[offset : offset+dbLen])

	return r, nil
}

// AuthResponse is sent by the server in response to authentication.
type AuthResponse struct {
	Status     byte
	Message    string
	SessionID  string
	Permission uint32
}

// Encode encodes the auth response to bytes.
func (r *AuthResponse) Encode() []byte {
	size := 1 + // Status
		2 + len(r.Message) + // Message
		2 + len(r.SessionID) + // SessionID
		4 // Permission

	buf := make([]byte, size)
	offset := 0

	// Status
	buf[offset] = r.Status
	offset += 1

	// Message
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(r.Message)))
	offset += 2
	copy(buf[offset:], r.Message)
	offset += len(r.Message)

	// SessionID
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(r.SessionID)))
	offset += 2
	copy(buf[offset:], r.SessionID)
	offset += len(r.SessionID)

	// Permission
	binary.BigEndian.PutUint32(buf[offset:], r.Permission)

	return buf
}

// DecodeAuthResponse decodes an auth response from bytes.
func DecodeAuthResponse(payload []byte) (*AuthResponse, error) {
	if len(payload) < 9 {
		return nil, fmt.Errorf("payload too short for auth response")
	}

	r := &AuthResponse{}
	offset := 0

	// Status
	r.Status = payload[offset]
	offset += 1

	// Message
	msgLen := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	if offset+msgLen > len(payload) {
		return nil, fmt.Errorf("invalid message length")
	}
	r.Message = string(payload[offset : offset+msgLen])
	offset += msgLen

	// SessionID
	sidLen := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	if offset+sidLen > len(payload) {
		return nil, fmt.Errorf("invalid session id length")
	}
	r.SessionID = string(payload[offset : offset+sidLen])
	offset += sidLen

	// Permission
	r.Permission = binary.BigEndian.Uint32(payload[offset:])

	return r, nil
}

// QueryRequest is sent by the client to execute a query.
type QueryRequest struct {
	SQL       string
	Params    [][]byte // Prepared statement parameters
	BatchMode bool     // Is this part of a batch?
}

// Encode encodes the query request to bytes.
func (r *QueryRequest) Encode() []byte {
	flags := byte(0)
	if r.BatchMode {
		flags |= 0x01
	}

	size := 4 + len(r.SQL) + // SQL
		1 + 1 + // Flags (2 bytes: length + value)
		2 // Param count

	for _, p := range r.Params {
		size += 2 + len(p)
	}

	buf := make([]byte, size)
	offset := 0

	// SQL
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(r.SQL)))
	offset += 4
	copy(buf[offset:], r.SQL)
	offset += len(r.SQL)

	// Flags
	buf[offset] = flags
	offset += 1

	// Param count
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(r.Params)))
	offset += 2

	// Params
	for _, p := range r.Params {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(p)))
		offset += 2
		copy(buf[offset:], p)
		offset += len(p)
	}

	return buf
}

// DecodeQueryRequest decodes a query request from bytes.
func DecodeQueryRequest(payload []byte) (*QueryRequest, error) {
	if len(payload) < 7 {
		return nil, fmt.Errorf("payload too short for query request")
	}

	r := &QueryRequest{}
	offset := 0

	// SQL
	sqlLen := int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	if offset+sqlLen > len(payload) {
		return nil, fmt.Errorf("invalid sql length")
	}
	r.SQL = string(payload[offset : offset+sqlLen])
	offset += sqlLen

	// Flags
	flags := payload[offset]
	r.BatchMode = flags&0x01 != 0
	offset += 1

	// Params
	paramCount := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	r.Params = make([][]byte, 0, paramCount)
	for i := 0; i < paramCount; i++ {
		if offset+2 > len(payload) {
			return nil, fmt.Errorf("payload truncated at param %d", i)
		}
		pLen := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		if offset+pLen > len(payload) {
			return nil, fmt.Errorf("invalid param %d length", i)
		}
		p := make([]byte, pLen)
		copy(p, payload[offset:offset+pLen])
		r.Params = append(r.Params, p)
		offset += pLen
	}

	return r, nil
}

// QueryResponse is sent by the server with query results.
type QueryResponse struct {
	Status      byte
	Message     string
	Columns     []ColumnInfo
	Rows        [][]interface{}
	RowCount    uint32
	Affected    uint32
	LastInsertID uint64
	ExecuteTime uint32 // milliseconds
}

// ColumnInfo describes a result column.
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

// Encode encodes the query response to bytes.
func (r *QueryResponse) Encode() []byte {
	// For simplicity, we'll encode as a basic format
	// A more efficient encoding would use a binary format
	// This is a placeholder - actual implementation would be more complex

	size := 1 + // Status
		2 + len(r.Message) + // Message
		2 + // Column count
		4 + // Row count
		4 + // Affected
		8 + // LastInsertID
		4 // ExecuteTime

	// Columns
	for _, col := range r.Columns {
		size += 2 + len(col.Name) + 2 + len(col.Type) + 1
	}

	buf := make([]byte, size)
	offset := 0

	// Status
	buf[offset] = r.Status
	offset += 1

	// Message
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(r.Message)))
	offset += 2
	copy(buf[offset:], r.Message)
	offset += len(r.Message)

	// Column count
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(r.Columns)))
	offset += 2

	// Columns
	for _, col := range r.Columns {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(col.Name)))
		offset += 2
		copy(buf[offset:], col.Name)
		offset += len(col.Name)

		binary.BigEndian.PutUint16(buf[offset:], uint16(len(col.Type)))
		offset += 2
		copy(buf[offset:], col.Type)
		offset += len(col.Type)

		if col.Nullable {
			buf[offset] = 1
		} else {
			buf[offset] = 0
		}
		offset += 1
	}

	// Row count
	binary.BigEndian.PutUint32(buf[offset:], r.RowCount)
	offset += 4

	// Affected
	binary.BigEndian.PutUint32(buf[offset:], r.Affected)
	offset += 4

	// LastInsertID
	binary.BigEndian.PutUint64(buf[offset:], r.LastInsertID)
	offset += 8

	// ExecuteTime
	binary.BigEndian.PutUint32(buf[offset:], r.ExecuteTime)

	// Note: Row data would need additional encoding for actual data
	// This is simplified for the skeleton

	return buf
}

// DecodeQueryResponse decodes a query response from bytes.
func DecodeQueryResponse(payload []byte) (*QueryResponse, error) {
	if len(payload) < 23 {
		return nil, fmt.Errorf("payload too short for query response")
	}

	r := &QueryResponse{}
	offset := 0

	// Status
	r.Status = payload[offset]
	offset += 1

	// Message
	msgLen := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	if offset+msgLen > len(payload) {
		return nil, fmt.Errorf("invalid message length")
	}
	r.Message = string(payload[offset : offset+msgLen])
	offset += msgLen

	// Column count
	colCount := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	// Columns
	r.Columns = make([]ColumnInfo, 0, colCount)
	for i := 0; i < colCount; i++ {
		col := ColumnInfo{}

		nameLen := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		col.Name = string(payload[offset : offset+nameLen])
		offset += nameLen

		typeLen := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		col.Type = string(payload[offset : offset+typeLen])
		offset += typeLen

		col.Nullable = payload[offset] == 1
		offset += 1

		r.Columns = append(r.Columns, col)
	}

	// Row count
	r.RowCount = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	// Affected
	r.Affected = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	// LastInsertID
	r.LastInsertID = binary.BigEndian.Uint64(payload[offset:])
	offset += 8

	// ExecuteTime
	r.ExecuteTime = binary.BigEndian.Uint32(payload[offset:])

	return r, nil
}

// ErrorPayload represents an error message payload.
type ErrorPayload struct {
	Code    uint32
	Message string
	Detail  string
}

// Encode encodes the error payload to bytes.
func (e *ErrorPayload) Encode() []byte {
	size := 4 + // Code
		2 + len(e.Message) + // Message
		2 + len(e.Detail) // Detail

	buf := make([]byte, size)
	offset := 0

	binary.BigEndian.PutUint32(buf[offset:], e.Code)
	offset += 4

	binary.BigEndian.PutUint16(buf[offset:], uint16(len(e.Message)))
	offset += 2
	copy(buf[offset:], e.Message)
	offset += len(e.Message)

	binary.BigEndian.PutUint16(buf[offset:], uint16(len(e.Detail)))
	offset += 2
	copy(buf[offset:], e.Detail)

	return buf
}

// DecodeErrorPayload decodes an error payload from bytes.
func DecodeErrorPayload(payload []byte) (*ErrorPayload, error) {
	if len(payload) < 8 {
		return nil, fmt.Errorf("payload too short for error")
	}

	e := &ErrorPayload{}
	offset := 0

	e.Code = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	msgLen := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	if offset+msgLen > len(payload) {
		return nil, fmt.Errorf("invalid message length")
	}
	e.Message = string(payload[offset : offset+msgLen])
	offset += msgLen

	if offset+2 > len(payload) {
		return e, nil
	}

	detailLen := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	if offset+detailLen > len(payload) {
		return nil, fmt.Errorf("invalid detail length")
	}
	e.Detail = string(payload[offset : offset+detailLen])

	return e, nil
}