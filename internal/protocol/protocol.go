// Package protocol implements the XxSql communication protocols.
package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Magic bytes for protocol identification
const (
	MagicPrivate = "XXSQ"
	MagicLen     = 4
)

// Message types for private protocol
const (
	MsgHandshakeRequest  byte = 0x01
	MsgHandshakeResponse byte = 0x02
	MsgAuthRequest       byte = 0x03
	MsgAuthResponse      byte = 0x04
	MsgQueryRequest      byte = 0x05
	MsgQueryResponse     byte = 0x06
	MsgPing              byte = 0x07
	MsgPong              byte = 0x08
	MsgError             byte = 0x09
	MsgClose             byte = 0x0A
	MsgBatchRequest      byte = 0x0B
	MsgBatchResponse     byte = 0x0C
)

// Protocol versions
const (
	ProtocolV1 = 1 // Basic protocol
	ProtocolV2 = 2 // v1.1.0: Batch operations, compression
	ProtocolV3 = 3 // v1.2.0: Streaming queries
)

// Status codes for responses
const (
	StatusOK    byte = 0x00
	StatusError byte = 0x01
	StatusAuth  byte = 0x02
)

// Header size: Magic(4) + Length(4) + Type(1) + SeqID(4) = 13 bytes
const HeaderSize = 13

// MaxMessageSize is the maximum allowed message size (10MB)
const MaxMessageSize = 10 * 1024 * 1024

// Header represents a message header.
type Header struct {
	Magic  [MagicLen]byte
	Length uint32
	Type   byte
	SeqID  uint32
}

// Message represents a complete protocol message.
type Message struct {
	Header  Header
	Payload []byte
}

// ReadHeader reads and parses a message header from the reader.
func ReadHeader(r io.Reader) (*Header, error) {
	var buf [HeaderSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	h := &Header{}
	copy(h.Magic[:], buf[0:4])
	h.Length = binary.BigEndian.Uint32(buf[4:8])
	h.Type = buf[8]
	h.SeqID = binary.BigEndian.Uint32(buf[9:13])

	// Validate magic
	if string(h.Magic[:]) != MagicPrivate {
		return nil, fmt.Errorf("invalid magic: %q", string(h.Magic[:]))
	}

	// Validate length
	if h.Length > MaxMessageSize {
		return nil, fmt.Errorf("message too large: %d > %d", h.Length, MaxMessageSize)
	}

	return h, nil
}

// ReadMessage reads a complete message from the reader.
func ReadMessage(r io.Reader) (*Message, error) {
	header, err := ReadHeader(r)
	if err != nil {
		return nil, err
	}

	// Read payload
	payload := make([]byte, header.Length-HeaderSize)
	if len(payload) > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("failed to read payload: %w", err)
		}
	}

	return &Message{
		Header:  *header,
		Payload: payload,
	}, nil
}

// WriteMessage writes a message to the writer.
func WriteMessage(w io.Writer, msgType byte, seqID uint32, payload []byte) error {
	length := uint32(HeaderSize + len(payload))

	// Write header
	var buf [HeaderSize]byte
	copy(buf[0:4], MagicPrivate)
	binary.BigEndian.PutUint32(buf[4:8], length)
	buf[8] = msgType
	binary.BigEndian.PutUint32(buf[9:13], seqID)

	if _, err := w.Write(buf[:]); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write payload
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return fmt.Errorf("failed to write payload: %w", err)
		}
	}

	return nil
}

// NewMessage creates a new message with the given type and payload.
func NewMessage(msgType byte, seqID uint32, payload []byte) *Message {
	length := uint32(HeaderSize + len(payload))
	h := Header{
		Length: length,
		Type:   msgType,
		SeqID:  seqID,
	}
	copy(h.Magic[:], MagicPrivate)

	return &Message{
		Header:  h,
		Payload: payload,
	}
}

// Encode encodes the message to bytes.
func (m *Message) Encode() ([]byte, error) {
	buf := make([]byte, HeaderSize+len(m.Payload))
	copy(buf[0:4], m.Header.Magic[:])
	binary.BigEndian.PutUint32(buf[4:8], m.Header.Length)
	buf[8] = m.Header.Type
	binary.BigEndian.PutUint32(buf[9:13], m.Header.SeqID)
	if len(m.Payload) > 0 {
		copy(buf[HeaderSize:], m.Payload)
	}
	return buf, nil
}

// DecodeMessage decodes a message from bytes.
func DecodeMessage(data []byte) (*Message, error) {
	if len(data) < HeaderSize {
		return nil, fmt.Errorf("data too short: %d < %d", len(data), HeaderSize)
	}

	m := &Message{}
	copy(m.Header.Magic[:], data[0:4])
	m.Header.Length = binary.BigEndian.Uint32(data[4:8])
	m.Header.Type = data[8]
	m.Header.SeqID = binary.BigEndian.Uint32(data[9:13])

	if m.Header.Length > uint32(len(data)) {
		return nil, fmt.Errorf("declared length exceeds data: %d > %d", m.Header.Length, len(data))
	}

	m.Payload = data[HeaderSize:m.Header.Length]
	return m, nil
}

// MessageType returns a string representation of the message type.
func MessageType(t byte) string {
	switch t {
	case MsgHandshakeRequest:
		return "HandshakeRequest"
	case MsgHandshakeResponse:
		return "HandshakeResponse"
	case MsgAuthRequest:
		return "AuthRequest"
	case MsgAuthResponse:
		return "AuthResponse"
	case MsgQueryRequest:
		return "QueryRequest"
	case MsgQueryResponse:
		return "QueryResponse"
	case MsgPing:
		return "Ping"
	case MsgPong:
		return "Pong"
	case MsgError:
		return "Error"
	case MsgClose:
		return "Close"
	case MsgBatchRequest:
		return "BatchRequest"
	case MsgBatchResponse:
		return "BatchResponse"
	default:
		return fmt.Sprintf("Unknown(%d)", t)
	}
}