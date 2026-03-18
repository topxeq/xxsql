package integration

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/mysql"
)

// TestHandshakeDirectly tests the MySQL handshake directly without the full server
func TestHandshakeDirectly(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	// Create handler on server side
	handler := mysql.NewMySQLHandler(server, 1,
		mysql.WithMySQLAuthHandler(func(h *mysql.MySQLHandler, username, database string, authResponse []byte) (bool, error) {
			t.Logf("Auth: username=%s, database=%s, authResponse len=%d", username, database, len(authResponse))
			return true, nil
		}),
		mysql.WithMySQLQueryHandler(func(h *mysql.MySQLHandler, sql string) ([]*mysql.ColumnDefinition, [][]interface{}, error) {
			return nil, nil, nil
		}),
	)

	// Run handler in goroutine
	go func() {
		handler.Handle()
	}()

	// Read handshake from server - use direct reads without bufio
	// Read packet header (4 bytes)
	header := make([]byte, 4)
	if _, err := io.ReadFull(client, header); err != nil {
		t.Fatalf("Failed to read handshake header: %v", err)
	}

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	seqID := header[3]
	t.Logf("Handshake packet: length=%d, seqID=%d", length, seqID)

	// Read handshake payload
	payload := make([]byte, length)
	if _, err := io.ReadFull(client, payload); err != nil {
		t.Fatalf("Failed to read handshake payload: %v", err)
	}

	t.Logf("Handshake payload first 20 bytes: %v", payload[:min(20, len(payload))])

	// Parse protocol version
	protoVersion := payload[0]
	t.Logf("Protocol version: %d", protoVersion)

	// Parse server version
	idx := bytes.IndexByte(payload[1:], 0)
	if idx < 0 {
		t.Fatal("No null terminator for server version")
	}
	serverVersion := string(payload[1 : 1+idx])
	t.Logf("Server version: %s", serverVersion)

	// Send auth response
	authPacket := buildAuthPacket(t)
	authHeader := make([]byte, 4)
	authHeader[0] = byte(len(authPacket))
	authHeader[1] = byte(len(authPacket) >> 8)
	authHeader[2] = byte(len(authPacket) >> 16)
	authHeader[3] = 1 // seqID 1

	if _, err := client.Write(authHeader); err != nil {
		t.Fatalf("Failed to write auth header: %v", err)
	}
	if _, err := client.Write(authPacket); err != nil {
		t.Fatalf("Failed to write auth packet: %v", err)
	}

	t.Log("Sent auth packet, waiting for response...")

	// Read response with timeout
	done := make(chan struct{})
	var respHeader []byte
	var respPayload []byte
	var readErr error

	go func() {
		defer close(done)
		respHeader = make([]byte, 4)
		if _, readErr = io.ReadFull(client, respHeader); readErr != nil {
			return
		}

		respLength := int(uint32(respHeader[0]) | uint32(respHeader[1])<<8 | uint32(respHeader[2])<<16)
		respSeqID := respHeader[3]
		t.Logf("Response packet: length=%d, seqID=%d", respLength, respSeqID)

		respPayload = make([]byte, respLength)
		_, readErr = io.ReadFull(client, respPayload)
	}()

	select {
	case <-done:
		if readErr != nil {
			t.Fatalf("Failed to read response: %v", readErr)
		}
		t.Logf("Response payload: %v", respPayload)
		if len(respPayload) > 0 && respPayload[0] == 0x00 {
			t.Log("Received OK packet - auth successful!")
		} else if len(respPayload) > 0 && respPayload[0] == 0xFF {
			t.Log("Received ERR packet - auth failed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for auth response")
	}
}

func buildAuthPacket(t *testing.T) []byte {
	var packet []byte

	// Capability flags (CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONN | CLIENT_PLUGIN_AUTH)
	capability := uint32(0x00000200 | 0x00008000 | 0x00080000)
	packet = append(packet, byte(capability), byte(capability>>8), byte(capability>>16), byte(capability>>24))

	// Max packet size
	packet = append(packet, 0xFF, 0xFF, 0xFF, 0x00)

	// Character set (utf8mb4)
	packet = append(packet, 45)

	// Reserved (23 bytes)
	packet = append(packet, make([]byte, 23)...)

	// Username (null-terminated)
	packet = append(packet, "testuser"...)
	packet = append(packet, 0)

	// Auth response (empty password)
	packet = append(packet, 0) // length 0

	// Auth plugin name
	packet = append(packet, "mysql_native_password"...)
	packet = append(packet, 0)

	t.Logf("Auth packet size: %d", len(packet))
	return packet
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestPacketHeaderParsing tests that we correctly parse packet headers
func TestPacketHeaderParsing(t *testing.T) {
	// OK packet: 7 bytes payload, seqID 2
	header := []byte{0x07, 0x00, 0x00, 0x02}

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	seqID := header[3]

	if length != 7 {
		t.Errorf("Expected length 7, got %d", length)
	}
	if seqID != 2 {
		t.Errorf("Expected seqID 2, got %d", seqID)
	}
}

// TestOKPacketParsing tests parsing an OK packet
func TestOKPacketParsing(t *testing.T) {
	// Simulate an OK packet
	// Header: length=7, seqID=2
	// Payload: 0x00 (OK), 0x00 (affected rows), 0x00 (last insert ID), status flags, warnings
	header := []byte{0x07, 0x00, 0x00, 0x02}
	payload := []byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00}

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	if length != len(payload) {
		t.Errorf("Length mismatch: header says %d, payload is %d", length, len(payload))
	}

	if payload[0] != 0x00 {
		t.Errorf("Expected OK packet marker 0x00, got 0x%02x", payload[0])
	}
}