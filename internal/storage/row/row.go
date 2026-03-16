// Package row provides row serialization for XxSql storage engine.
package row

import (
	"encoding/binary"
	"fmt"

	"github.com/topxeq/xxsql/internal/storage/types"
)

const (
	// RowHeaderSize is the size of the row header in bytes.
	// RowID (8 bytes) + ColumnCount (2 bytes) = 10 bytes
	RowHeaderSize = 10

	// MaxRowSize is the maximum size of a row.
	// Limit to fit within a page with some overhead
	MaxRowSize = 3500
)

// RowID represents a unique row identifier.
type RowID uint64

// InvalidRowID represents an invalid row ID.
const InvalidRowID RowID = 0

// Row represents a table row.
type Row struct {
	ID         RowID
	Values     []types.Value
	ColumnDefs []*types.ColumnInfo
}

// NewRow creates a new row with the given column definitions.
func NewRow(id RowID, defs []*types.ColumnInfo) *Row {
	return &Row{
		ID:         id,
		Values:     make([]types.Value, len(defs)),
		ColumnDefs: defs,
	}
}

// Size returns the serialized size of the row.
func (r *Row) Size() int {
	size := RowHeaderSize
	for _, v := range r.Values {
		size += v.Size()
	}
	return size
}

// Serialize serializes the row to bytes.
func (r *Row) Serialize() ([]byte, error) {
	if len(r.Values) > 65535 {
		return nil, fmt.Errorf("too many columns: %d", len(r.Values))
	}

	size := r.Size()
	if size > MaxRowSize {
		return nil, fmt.Errorf("row too large: %d bytes (max %d)", size, MaxRowSize)
	}

	buf := make([]byte, size)

	// Write row header
	binary.LittleEndian.PutUint64(buf[0:8], uint64(r.ID))
	binary.LittleEndian.PutUint16(buf[8:10], uint16(len(r.Values)))

	// Write values
	offset := RowHeaderSize
	for _, v := range r.Values {
		data := v.Marshal()
		copy(buf[offset:offset+len(data)], data)
		offset += len(data)
	}

	return buf, nil
}

// DeserializeRow deserializes bytes to a row.
// It handles the case where columns have been added after the row was created
// by filling missing columns with NULL values.
func DeserializeRow(data []byte, defs []*types.ColumnInfo) (*Row, error) {
	if len(data) < RowHeaderSize {
		return nil, fmt.Errorf("data too short for row header: %d bytes", len(data))
	}

	row := &Row{
		ID:         RowID(binary.LittleEndian.Uint64(data[0:8])),
		Values:     make([]types.Value, len(defs)),
		ColumnDefs: defs,
	}

	// Initialize all values to NULL
	for i := range row.Values {
		row.Values[i] = types.NewNullValue()
	}

	storedColCount := int(binary.LittleEndian.Uint16(data[8:10]))

	// Handle schema evolution: if more columns were added, fill with NULL
	// If columns were removed, ignore the extra stored columns
	colsToRead := storedColCount
	if colsToRead > len(defs) {
		colsToRead = len(defs)
	}

	offset := RowHeaderSize
	for i := 0; i < colsToRead; i++ {
		if offset >= len(data) {
			return nil, fmt.Errorf("unexpected end of row data at column %d", i)
		}

		v, bytesRead := types.UnmarshalValue(data[offset:], defs[i].Type)
		row.Values[i] = v
		offset += bytesRead
	}

	return row, nil
}

// GetValue returns the value at the given column index.
func (r *Row) GetValue(index int) (types.Value, error) {
	if index < 0 || index >= len(r.Values) {
		return types.NewNullValue(), fmt.Errorf("column index out of range: %d", index)
	}
	return r.Values[index], nil
}

// GetValueByName returns the value for the given column name.
func (r *Row) GetValueByName(name string) (types.Value, error) {
	for i, def := range r.ColumnDefs {
		if def.Name == name {
			return r.Values[i], nil
		}
	}
	return types.NewNullValue(), fmt.Errorf("column not found: %s", name)
}

// SetValue sets the value at the given column index.
func (r *Row) SetValue(index int, v types.Value) error {
	if index < 0 || index >= len(r.Values) {
		return fmt.Errorf("column index out of range: %d", index)
	}
	r.Values[index] = v
	return nil
}

// String returns a string representation of the row.
func (r *Row) String() string {
	result := fmt.Sprintf("Row{id=%d, values=[", r.ID)
	for i, v := range r.Values {
		if i > 0 {
			result += ", "
		}
		result += v.String()
	}
	result += "]}"
	return result
}

// RowBuilder provides a fluent interface for building rows.
type RowBuilder struct {
	row *Row
	err error
}

// NewRowBuilder creates a new row builder.
func NewRowBuilder(id RowID, defs []*types.ColumnInfo) *RowBuilder {
	return &RowBuilder{
		row: NewRow(id, defs),
	}
}

// SetInt sets an integer value.
func (b *RowBuilder) SetInt(index int, v int64) *RowBuilder {
	if b.err != nil {
		return b
	}
	b.err = b.row.SetValue(index, types.NewIntValue(v))
	return b
}

// SetFloat sets a float value.
func (b *RowBuilder) SetFloat(index int, v float64) *RowBuilder {
	if b.err != nil {
		return b
	}
	b.err = b.row.SetValue(index, types.NewFloatValue(v))
	return b
}

// SetString sets a string value.
func (b *RowBuilder) SetString(index int, v string, typ types.TypeID) *RowBuilder {
	if b.err != nil {
		return b
	}
	b.err = b.row.SetValue(index, types.NewStringValue(v, typ))
	return b
}

// SetNull sets a null value.
func (b *RowBuilder) SetNull(index int) *RowBuilder {
	if b.err != nil {
		return b
	}
	b.err = b.row.SetValue(index, types.NewNullValue())
	return b
}

// SetBool sets a boolean value.
func (b *RowBuilder) SetBool(index int, v bool) *RowBuilder {
	if b.err != nil {
		return b
	}
	b.err = b.row.SetValue(index, types.NewBoolValue(v))
	return b
}

// Set sets a value directly.
func (b *RowBuilder) Set(index int, v types.Value) *RowBuilder {
	if b.err != nil {
		return b
	}
	b.err = b.row.SetValue(index, v)
	return b
}

// Build returns the built row or an error.
func (b *RowBuilder) Build() (*Row, error) {
	return b.row, b.err
}

// SerializeRow serializes a slice of values with row ID.
func SerializeRow(id RowID, values []types.Value) ([]byte, error) {
	if len(values) > 65535 {
		return nil, fmt.Errorf("too many columns: %d", len(values))
	}

	// Calculate size
	size := RowHeaderSize
	for _, v := range values {
		size += v.Size()
	}

	if size > MaxRowSize {
		return nil, fmt.Errorf("row too large: %d bytes (max %d)", size, MaxRowSize)
	}

	buf := make([]byte, size)

	// Write row header
	binary.LittleEndian.PutUint64(buf[0:8], uint64(id))
	binary.LittleEndian.PutUint16(buf[8:10], uint16(len(values)))

	// Write values
	offset := RowHeaderSize
	for _, v := range values {
		data := v.Marshal()
		copy(buf[offset:offset+len(data)], data)
		offset += len(data)
	}

	return buf, nil
}

// DeserializeValues deserializes bytes to values (without row ID).
func DeserializeValues(data []byte, types_ []types.TypeID) ([]types.Value, error) {
	if len(data) < RowHeaderSize {
		return nil, fmt.Errorf("data too short for row header: %d bytes", len(data))
	}

	colCount := int(binary.LittleEndian.Uint16(data[8:10]))
	if colCount != len(types_) {
		return nil, fmt.Errorf("column count mismatch: expected %d, got %d", len(types_), colCount)
	}

	values := make([]types.Value, len(types_))
	offset := RowHeaderSize

	for i, typ := range types_ {
		if offset >= len(data) {
			return nil, fmt.Errorf("unexpected end of row data at column %d", i)
		}

		v, bytesRead := types.UnmarshalValue(data[offset:], typ)
		values[i] = v
		offset += bytesRead
	}

	return values, nil
}
