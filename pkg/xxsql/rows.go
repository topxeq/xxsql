package xxsql

import (
	"database/sql/driver"
	"io"
	"strconv"
	"time"
)

// MySQL column type codes
const (
	TypeDecimal    = 0x00
	TypeTiny       = 0x01
	TypeShort      = 0x02
	TypeLong       = 0x03
	TypeFloat      = 0x04
	TypeDouble     = 0x05
	TypeNull       = 0x06
	TypeTimestamp  = 0x07
	TypeLongLong   = 0x08
	TypeInt24      = 0x09
	TypeDate       = 0x0A
	TypeTime       = 0x0B
	TypeDateTime   = 0x0C
	TypeYear       = 0x0D
	TypeNewDate    = 0x0E
	TypeVarChar    = 0x0F
	TypeBit        = 0x10
	TypeNewDecimal = 0xE6
	TypeEnum       = 0xF7
	TypeSet        = 0xF8
	TypeTinyBlob   = 0xF9
	TypeMediumBlob = 0xFA
	TypeLongBlob   = 0xFB
	TypeBlob       = 0xFC
	TypeVarString  = 0xFD
	TypeString     = 0xFE
	TypeGeometry   = 0xFF
)

// rows implements driver.Rows.
type rows struct {
	mysqlConn *mysqlConn
	columns   []string
	colTypes  []byte
	rowData   [][]byte
	pos       int
}

// Columns returns the column names.
func (r *rows) Columns() []string {
	return r.columns
}

// Close closes the rows.
func (r *rows) Close() error {
	r.rowData = nil
	return nil
}

// Next populates dest with the next row values.
func (r *rows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rowData) {
		return io.EOF
	}

	data := r.rowData[r.pos]
	r.pos++

	return r.parseRow(data, dest)
}

// HasNextResultSet reports whether there is another result set.
func (r *rows) HasNextResultSet() bool {
	return false
}

// NextResultSet advances to the next result set.
func (r *rows) NextResultSet() error {
	return io.EOF
}

// parseRow parses a row data packet into driver values.
func (r *rows) parseRow(data []byte, dest []driver.Value) error {
	offset := 0

	for i := range dest {
		if offset >= len(data) {
			return io.ErrUnexpectedEOF
		}

		// Check for NULL
		if data[offset] == 0xFB {
			dest[i] = nil
			offset++
			continue
		}

		// Read length-encoded value
		length, n := readLengthEncodedInt(data[offset:])
		offset += n

		valueStart := offset
		offset += int(length)

		if offset > len(data) {
			return io.ErrUnexpectedEOF
		}

		valueData := data[valueStart:offset]

		// Convert based on column type
		if i < len(r.colTypes) {
			dest[i] = convertValue(valueData, r.colTypes[i])
		} else {
			// Default to string
			dest[i] = string(valueData)
		}
	}

	return nil
}

// convertValue converts raw MySQL data to a Go value based on column type.
func convertValue(data []byte, colType byte) driver.Value {
	if len(data) == 0 {
		return ""
	}

	switch colType {
	case TypeTiny, TypeShort, TypeLong, TypeLongLong, TypeInt24, TypeYear:
		// Integer types - parse as int64
		s := string(data)
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return s
		}
		return v

	case TypeFloat, TypeDouble, TypeDecimal, TypeNewDecimal:
		// Float types - parse as float64
		s := string(data)
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return s
		}
		return v

	case TypeDate, TypeDateTime, TypeTimestamp:
		// Date/time types - parse as time.Time
		s := string(data)
		t, err := parseMySQLTime(s)
		if err != nil {
			return s
		}
		return t

	case TypeTime:
		// Time type - parse as duration string
		return string(data)

	case TypeBit:
		// Bit type - return as []byte
		return data

	case TypeBlob, TypeTinyBlob, TypeMediumBlob, TypeLongBlob:
		// Blob types - return as []byte
		return data

	case TypeVarChar, TypeVarString, TypeString, TypeEnum, TypeSet:
		// String types
		return string(data)

	case TypeNull:
		return nil

	default:
		// Unknown type - return as string
		return string(data)
	}
}

// parseMySQLTime parses a MySQL datetime string.
func parseMySQLTime(s string) (time.Time, error) {
	// Try common MySQL datetime formats
	formats := []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, io.ErrUnexpectedEOF
}
