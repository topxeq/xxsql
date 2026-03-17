package row

import (
	"testing"
	"time"

	"github.com/topxeq/xxsql/internal/storage/types"
)

func TestNewRow(t *testing.T) {
	defs := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt},
		{Name: "name", Type: types.TypeVarchar},
	}

	row := NewRow(1, defs)

	if row.ID != 1 {
		t.Errorf("ID = %d, want 1", row.ID)
	}
	if len(row.Values) != 2 {
		t.Errorf("Values length = %d, want 2", len(row.Values))
	}
	if len(row.ColumnDefs) != 2 {
		t.Errorf("ColumnDefs length = %d, want 2", len(row.ColumnDefs))
	}
}

func TestRow_Size(t *testing.T) {
	tests := []struct {
		name     string
		row      *Row
		minSize  int
	}{
		{
			name: "empty row",
			row:  NewRow(1, []*types.ColumnInfo{}),
			minSize: RowHeaderSize,
		},
		{
			name: "row with int",
			row: func() *Row {
				r := NewRow(1, []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}})
				r.Values[0] = types.NewIntValue(42)
				return r
			}(),
			minSize: RowHeaderSize + 9, // header + null flag + 8 bytes
		},
		{
			name: "row with string",
			row: func() *Row {
				r := NewRow(1, []*types.ColumnInfo{{Name: "name", Type: types.TypeVarchar}})
				r.Values[0] = types.NewStringValue("hello", types.TypeVarchar)
				return r
			}(),
			minSize: RowHeaderSize + 8, // header + null flag + length + data
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := tt.row.Size()
			if size < tt.minSize {
				t.Errorf("Size() = %d, want at least %d", size, tt.minSize)
			}
		})
	}
}

func TestRow_SerializeDeserialize(t *testing.T) {
	defs := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt},
		{Name: "name", Type: types.TypeVarchar},
		{Name: "price", Type: types.TypeFloat},
		{Name: "active", Type: types.TypeBool},
	}

	row := NewRow(1, defs)
	row.Values[0] = types.NewIntValue(42)
	row.Values[1] = types.NewStringValue("test", types.TypeVarchar)
	row.Values[2] = types.NewFloatValue(3.14)
	row.Values[3] = types.NewBoolValue(true)

	// Serialize
	data, err := row.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Deserialize
	row2, err := DeserializeRow(data, defs)
	if err != nil {
		t.Fatalf("DeserializeRow failed: %v", err)
	}

	if row2.ID != row.ID {
		t.Errorf("ID = %d, want %d", row2.ID, row.ID)
	}

	if row2.Values[0].AsInt() != 42 {
		t.Errorf("id = %d, want 42", row2.Values[0].AsInt())
	}

	if row2.Values[1].AsString() != "test" {
		t.Errorf("name = %s, want 'test'", row2.Values[1].AsString())
	}

	if row2.Values[2].AsFloat() != 3.14 {
		t.Errorf("price = %f, want 3.14", row2.Values[2].AsFloat())
	}

	if row2.Values[3].AsBool() != true {
		t.Errorf("active = %v, want true", row2.Values[3].AsBool())
	}
}

func TestRow_Serialize_TooManyColumns(t *testing.T) {
	defs := make([]*types.ColumnInfo, 70000) // More than 65535
	for i := range defs {
		defs[i] = &types.ColumnInfo{Name: "col", Type: types.TypeInt}
	}
	row := NewRow(1, defs)

	_, err := row.Serialize()
	if err == nil {
		t.Error("expected error for too many columns")
	}
}

func TestRow_Serialize_TooLarge(t *testing.T) {
	defs := []*types.ColumnInfo{
		{Name: "data", Type: types.TypeText},
	}
	row := NewRow(1, defs)
	// Large text that exceeds MaxRowSize
	row.Values[0] = types.NewStringValue(string(make([]byte, 4000)), types.TypeText)

	_, err := row.Serialize()
	if err == nil {
		t.Error("expected error for row too large")
	}
}

func TestDeserializeRow_DataTooShort(t *testing.T) {
	defs := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}

	_, err := DeserializeRow([]byte{1, 2, 3}, defs) // Less than RowHeaderSize
	if err == nil {
		t.Error("expected error for data too short")
	}
}

func TestDeserializeRow_SchemaEvolution(t *testing.T) {
	// Original schema: 2 columns
	defs1 := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt},
		{Name: "name", Type: types.TypeVarchar},
	}
	row1 := NewRow(1, defs1)
	row1.Values[0] = types.NewIntValue(42)
	row1.Values[1] = types.NewStringValue("test", types.TypeVarchar)

	data, _ := row1.Serialize()

	// New schema: 3 columns (added new column)
	defs2 := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt},
		{Name: "name", Type: types.TypeVarchar},
		{Name: "extra", Type: types.TypeInt},
	}

	row2, err := DeserializeRow(data, defs2)
	if err != nil {
		t.Fatalf("DeserializeRow failed: %v", err)
	}

	// Extra column should be NULL
	if !row2.Values[2].Null {
		t.Error("added column should be NULL")
	}
}

func TestRow_GetValue(t *testing.T) {
	defs := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt},
		{Name: "name", Type: types.TypeVarchar},
	}
	row := NewRow(1, defs)
	row.Values[0] = types.NewIntValue(42)
	row.Values[1] = types.NewStringValue("test", types.TypeVarchar)

	val, err := row.GetValue(0)
	if err != nil {
		t.Fatalf("GetValue failed: %v", err)
	}
	if val.AsInt() != 42 {
		t.Errorf("GetValue(0) = %d, want 42", val.AsInt())
	}
}

func TestRow_GetValue_OutOfRange(t *testing.T) {
	row := NewRow(1, []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}})

	_, err := row.GetValue(-1)
	if err == nil {
		t.Error("expected error for negative index")
	}

	_, err = row.GetValue(10)
	if err == nil {
		t.Error("expected error for out of range index")
	}
}

func TestRow_GetValueByName(t *testing.T) {
	defs := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt},
		{Name: "name", Type: types.TypeVarchar},
	}
	row := NewRow(1, defs)
	row.Values[0] = types.NewIntValue(42)
	row.Values[1] = types.NewStringValue("test", types.TypeVarchar)

	val, err := row.GetValueByName("id")
	if err != nil {
		t.Fatalf("GetValueByName failed: %v", err)
	}
	if val.AsInt() != 42 {
		t.Errorf("GetValueByName('id') = %d, want 42", val.AsInt())
	}

	_, err = row.GetValueByName("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent column")
	}
}

func TestRow_SetValue(t *testing.T) {
	row := NewRow(1, []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}})

	err := row.SetValue(0, types.NewIntValue(100))
	if err != nil {
		t.Fatalf("SetValue failed: %v", err)
	}
	if row.Values[0].AsInt() != 100 {
		t.Errorf("SetValue result = %d, want 100", row.Values[0].AsInt())
	}
}

func TestRow_SetValue_OutOfRange(t *testing.T) {
	row := NewRow(1, []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}})

	err := row.SetValue(-1, types.NewIntValue(100))
	if err == nil {
		t.Error("expected error for negative index")
	}

	err = row.SetValue(10, types.NewIntValue(100))
	if err == nil {
		t.Error("expected error for out of range index")
	}
}

func TestRow_String(t *testing.T) {
	defs := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt},
		{Name: "name", Type: types.TypeVarchar},
	}
	row := NewRow(1, defs)
	row.Values[0] = types.NewIntValue(42)
	row.Values[1] = types.NewStringValue("test", types.TypeVarchar)

	s := row.String()
	if s == "" {
		t.Error("String() should not be empty")
	}
}

// RowBuilder tests

func TestRowBuilder_Int(t *testing.T) {
	defs := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt},
		{Name: "count", Type: types.TypeInt},
	}

	row, err := NewRowBuilder(1, defs).
		SetInt(0, 42).
		SetInt(1, 100).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if row.Values[0].AsInt() != 42 {
		t.Errorf("id = %d, want 42", row.Values[0].AsInt())
	}
	if row.Values[1].AsInt() != 100 {
		t.Errorf("count = %d, want 100", row.Values[1].AsInt())
	}
}

func TestRowBuilder_Float(t *testing.T) {
	defs := []*types.ColumnInfo{{Name: "price", Type: types.TypeFloat}}

	row, err := NewRowBuilder(1, defs).
		SetFloat(0, 3.14).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if row.Values[0].AsFloat() != 3.14 {
		t.Errorf("price = %f, want 3.14", row.Values[0].AsFloat())
	}
}

func TestRowBuilder_String(t *testing.T) {
	defs := []*types.ColumnInfo{{Name: "name", Type: types.TypeVarchar}}

	row, err := NewRowBuilder(1, defs).
		SetString(0, "hello", types.TypeVarchar).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if row.Values[0].AsString() != "hello" {
		t.Errorf("name = %s, want 'hello'", row.Values[0].AsString())
	}
}

func TestRowBuilder_Null(t *testing.T) {
	defs := []*types.ColumnInfo{{Name: "value", Type: types.TypeInt}}

	row, err := NewRowBuilder(1, defs).
		SetNull(0).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if !row.Values[0].Null {
		t.Error("value should be NULL")
	}
}

func TestRowBuilder_Bool(t *testing.T) {
	defs := []*types.ColumnInfo{{Name: "active", Type: types.TypeBool}}

	row, err := NewRowBuilder(1, defs).
		SetBool(0, true).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if row.Values[0].AsBool() != true {
		t.Errorf("active = %v, want true", row.Values[0].AsBool())
	}
}

func TestRowBuilder_ChainError(t *testing.T) {
	defs := []*types.ColumnInfo{{Name: "id", Type: types.TypeInt}}

	// Set error by using invalid index
	row, err := NewRowBuilder(1, defs).
		SetInt(10, 42). // Invalid index
		SetInt(0, 100). // Should be skipped due to error
		Build()

	if err == nil {
		t.Error("expected error from invalid index")
	}
	// Note: row is still returned, but with error set
	if row == nil {
		t.Error("row should not be nil (it's built regardless of error)")
	}
}

// SerializeRow and DeserializeValues tests

func TestSerializeRow(t *testing.T) {
	values := []types.Value{
		types.NewIntValue(42),
		types.NewStringValue("test", types.TypeVarchar),
	}

	data, err := SerializeRow(1, values)
	if err != nil {
		t.Fatalf("SerializeRow failed: %v", err)
	}
	if len(data) < RowHeaderSize {
		t.Errorf("data length = %d, want at least %d", len(data), RowHeaderSize)
	}
}

func TestSerializeRow_TooManyColumns(t *testing.T) {
	values := make([]types.Value, 70000)

	_, err := SerializeRow(1, values)
	if err == nil {
		t.Error("expected error for too many columns")
	}
}

func TestDeserializeValues(t *testing.T) {
	typeIDs := []types.TypeID{types.TypeInt, types.TypeVarchar}
	values := []types.Value{
		types.NewIntValue(42),
		types.NewStringValue("test", types.TypeVarchar),
	}

	data, _ := SerializeRow(1, values)
	result, err := DeserializeValues(data, typeIDs)

	if err != nil {
		t.Fatalf("DeserializeValues failed: %v", err)
	}
	if len(result) != 2 {
		t.Errorf("result length = %d, want 2", len(result))
	}
}

func TestDeserializeValues_CountMismatch(t *testing.T) {
	typeIDs := []types.TypeID{types.TypeInt, types.TypeVarchar, types.TypeFloat}
	values := []types.Value{
		types.NewIntValue(42),
		types.NewStringValue("test", types.TypeVarchar),
	}

	data, _ := SerializeRow(1, values)
	_, err := DeserializeValues(data, typeIDs)

	if err == nil {
		t.Error("expected error for column count mismatch")
	}
}

func TestDeserializeValues_DataTooShort(t *testing.T) {
	typeIDs := []types.TypeID{types.TypeInt}

	_, err := DeserializeValues([]byte{1, 2, 3}, typeIDs)
	if err == nil {
		t.Error("expected error for data too short")
	}
}

func TestRow_NullHandling(t *testing.T) {
	defs := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt},
		{Name: "name", Type: types.TypeVarchar, Nullable: true},
	}

	row := NewRow(1, defs)
	row.Values[0] = types.NewIntValue(42)
	row.Values[1] = types.NewNullValue()

	data, err := row.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	row2, err := DeserializeRow(data, defs)
	if err != nil {
		t.Fatalf("DeserializeRow failed: %v", err)
	}

	if !row2.Values[1].Null {
		t.Error("second value should be NULL")
	}
}

func TestRow_DecimalType(t *testing.T) {
	defs := []*types.ColumnInfo{{Name: "price", Type: types.TypeDecimal}}

	row := NewRow(1, defs)
	row.Values[0] = types.NewDecimalValue(12345, 2) // 123.45

	data, err := row.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	row2, err := DeserializeRow(data, defs)
	if err != nil {
		t.Fatalf("DeserializeRow failed: %v", err)
	}

	unscaled, scale := row2.Values[0].AsDecimal()
	if unscaled != 12345 || scale != 2 {
		t.Errorf("AsDecimal() = (%d, %d), want (12345, 2)", unscaled, scale)
	}
}

func TestRow_DateTimeTypes(t *testing.T) {
	defs := []*types.ColumnInfo{
		{Name: "created", Type: types.TypeDatetime},
		{Name: "birth_date", Type: types.TypeDate},
		{Name: "start_time", Type: types.TypeTime},
	}

	row := NewRow(1, defs)
	// Set actual datetime values
	now := time.Now()
	row.Values[0] = types.NewDatetimeValue(now)
	row.Values[1] = types.NewDateValue(now)
	row.Values[2] = types.NewTimeValue(now)

	data, err := row.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	row2, err := DeserializeRow(data, defs)
	if err != nil {
		t.Fatalf("DeserializeRow failed: %v", err)
	}

	// Verify datetime value
	if row2.Values[0].Null {
		t.Error("created should not be NULL")
	}
}

func BenchmarkRow_Serialize(b *testing.B) {
	defs := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt},
		{Name: "name", Type: types.TypeVarchar},
		{Name: "price", Type: types.TypeFloat},
	}
	row := NewRow(1, defs)
	row.Values[0] = types.NewIntValue(42)
	row.Values[1] = types.NewStringValue("test value", types.TypeVarchar)
	row.Values[2] = types.NewFloatValue(3.14159)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row.Serialize()
	}
}

func BenchmarkRow_Deserialize(b *testing.B) {
	defs := []*types.ColumnInfo{
		{Name: "id", Type: types.TypeInt},
		{Name: "name", Type: types.TypeVarchar},
		{Name: "price", Type: types.TypeFloat},
	}
	row := NewRow(1, defs)
	row.Values[0] = types.NewIntValue(42)
	row.Values[1] = types.NewStringValue("test value", types.TypeVarchar)
	row.Values[2] = types.NewFloatValue(3.14159)
	data, _ := row.Serialize()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DeserializeRow(data, defs)
	}
}
