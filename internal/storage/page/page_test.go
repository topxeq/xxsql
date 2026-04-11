package page

import (
	"testing"
)

func TestPageType_String(t *testing.T) {
	tests := []struct {
		name     string
		typ      PageType
		expected string
	}{
		{"header", PageTypeHeader, "HEADER"},
		{"data", PageTypeData, "DATA"},
		{"index", PageTypeIndex, "INDEX"},
		{"overflow", PageTypeOverflow, "OVERFLOW"},
		{"free", PageTypeFree, "FREE"},
		{"unknown", PageType(99), "UNKNOWN(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.typ.String(); got != tt.expected {
				t.Errorf("PageType.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestNewPage(t *testing.T) {
	p := NewPage(1, PageTypeData)

	if p.ID != 1 {
		t.Errorf("expected ID 1, got %d", p.ID)
	}
	if p.Type != PageTypeData {
		t.Errorf("expected type DATA, got %v", p.Type)
	}
	if len(p.Data) != PageSize {
		t.Errorf("expected data size %d, got %d", PageSize, len(p.Data))
	}
	if !p.Modified {
		t.Error("expected page to be marked as modified")
	}
	if p.GetPageID() != 1 {
		t.Errorf("header page ID = %d, want 1", p.GetPageID())
	}
	if p.GetType() != PageTypeData {
		t.Errorf("header page type = %v, want DATA", p.GetType())
	}
	if p.GetFreeOffset() != PageHeaderSize {
		t.Errorf("initial free offset = %d, want %d", p.GetFreeOffset(), PageHeaderSize)
	}
	if p.GetSlotCount() != 0 {
		t.Errorf("initial slot count = %d, want 0", p.GetSlotCount())
	}
}

func TestPage_HeaderAccessors(t *testing.T) {
	p := NewPage(1, PageTypeData)

	t.Run("PageID", func(t *testing.T) {
		p.SetPageID(42)
		if p.GetPageID() != 42 {
			t.Errorf("GetPageID() = %d, want 42", p.GetPageID())
		}
	})

	t.Run("LSN", func(t *testing.T) {
		p.SetLSN(12345)
		if p.GetLSN() != 12345 {
			t.Errorf("GetLSN() = %d, want 12345", p.GetLSN())
		}
	})

	t.Run("Type", func(t *testing.T) {
		p.SetType(PageTypeIndex)
		if p.GetType() != PageTypeIndex {
			t.Errorf("GetType() = %v, want INDEX", p.GetType())
		}
	})

	t.Run("FreeOffset", func(t *testing.T) {
		p.SetFreeOffset(100)
		if p.GetFreeOffset() != 100 {
			t.Errorf("GetFreeOffset() = %d, want 100", p.GetFreeOffset())
		}
	})

	t.Run("SlotCount", func(t *testing.T) {
		p.SetSlotCount(5)
		if p.GetSlotCount() != 5 {
			t.Errorf("GetSlotCount() = %d, want 5", p.GetSlotCount())
		}
	})
}

func TestPage_Flags(t *testing.T) {
	p := NewPage(1, PageTypeData)

	t.Run("SetAndGetFlag", func(t *testing.T) {
		p.SetFlags(0)

		if p.HasFlag(FlagLeaf) {
			t.Error("should not have FlagLeaf")
		}

		p.SetFlag(FlagLeaf)
		if !p.HasFlag(FlagLeaf) {
			t.Error("should have FlagLeaf")
		}

		p.SetFlag(FlagRoot)
		if !p.HasFlag(FlagRoot) {
			t.Error("should have FlagRoot")
		}

		p.ClearFlag(FlagLeaf)
		if p.HasFlag(FlagLeaf) {
			t.Error("should not have FlagLeaf after clear")
		}
		if !p.HasFlag(FlagRoot) {
			t.Error("should still have FlagRoot")
		}
	})

	t.Run("MultipleFlags", func(t *testing.T) {
		p.SetFlags(0)
		p.SetFlag(FlagLeaf | FlagRoot)

		if !p.HasFlag(FlagLeaf) || !p.HasFlag(FlagRoot) {
			t.Error("should have both flags")
		}
	})
}

func TestPage_LeafRootFlags(t *testing.T) {
	p := NewPage(1, PageTypeData)

	t.Run("Leaf", func(t *testing.T) {
		p.SetLeaf(true)
		if !p.IsLeaf() {
			t.Error("should be leaf")
		}

		p.SetLeaf(false)
		if p.IsLeaf() {
			t.Error("should not be leaf")
		}
	})

	t.Run("Root", func(t *testing.T) {
		p.SetRoot(true)
		if !p.IsRoot() {
			t.Error("should be root")
		}

		p.SetRoot(false)
		if p.IsRoot() {
			t.Error("should not be root")
		}
	})
}

func TestPage_FreeSpace(t *testing.T) {
	p := NewPage(1, PageTypeData)

	initialFree := p.FreeSpace()
	if initialFree == 0 {
		t.Error("initial free space should not be zero")
	}

	// Insert some data
	p.InsertRow([]byte("test data"))

	if p.FreeSpace() >= initialFree {
		t.Error("free space should decrease after insert")
	}
}

func TestPage_DataRange(t *testing.T) {
	p := NewPage(1, PageTypeData)

	start, end := p.DataRange()
	if start != PageHeaderSize {
		t.Errorf("start = %d, want %d", start, PageHeaderSize)
	}
	if end != PageHeaderSize {
		t.Errorf("initial end = %d, want %d", end, PageHeaderSize)
	}
}

func TestPage_SlotOperations(t *testing.T) {
	p := NewPage(1, PageTypeData)

	// Test AppendSlot
	idx := p.AppendSlot(100, 20)
	if idx != 0 {
		t.Errorf("first slot index = %d, want 0", idx)
	}
	if p.GetSlotCount() != 1 {
		t.Errorf("slot count = %d, want 1", p.GetSlotCount())
	}

	// Get the slot
	slot := p.GetSlot(0)
	if slot.Offset != 100 || slot.Length != 20 {
		t.Errorf("slot = {Offset: %d, Length: %d}, want {Offset: 100, Length: 20}", slot.Offset, slot.Length)
	}

	// Set slot
	p.SetSlot(0, SlotEntry{Offset: 200, Length: 30})
	slot = p.GetSlot(0)
	if slot.Offset != 200 || slot.Length != 30 {
		t.Errorf("after set, slot = {Offset: %d, Length: %d}, want {Offset: 200, Length: 30}", slot.Offset, slot.Length)
	}

	// Add another slot
	idx = p.AppendSlot(300, 40)
	if idx != 1 {
		t.Errorf("second slot index = %d, want 1", idx)
	}
}

func TestPage_InsertRow(t *testing.T) {
	p := NewPage(1, PageTypeData)

	data := []byte("hello world")
	idx, err := p.InsertRow(data)
	if err != nil {
		t.Fatalf("InsertRow failed: %v", err)
	}
	if idx != 0 {
		t.Errorf("slot index = %d, want 0", idx)
	}
	if p.RowCount() != 1 {
		t.Errorf("row count = %d, want 1", p.RowCount())
	}
}

func TestPage_InsertRow_MultipleRows(t *testing.T) {
	p := NewPage(1, PageTypeData)

	rows := [][]byte{
		[]byte("row1"),
		[]byte("row2"),
		[]byte("row3"),
	}

	for i, row := range rows {
		idx, err := p.InsertRow(row)
		if err != nil {
			t.Fatalf("InsertRow %d failed: %v", i, err)
		}
		if idx != i {
			t.Errorf("row %d: slot index = %d, want %d", i, idx, i)
		}
	}

	if p.RowCount() != 3 {
		t.Errorf("row count = %d, want 3", p.RowCount())
	}
}

func TestPage_InsertRow_NotEnoughSpace(t *testing.T) {
	p := NewPage(1, PageTypeData)

	// Fill the page
	largeData := make([]byte, 3000)
	for i := 0; i < 10; i++ {
		_, err := p.InsertRow(largeData)
		if err != nil {
			break
		}
	}

	// This should fail
	_, err := p.InsertRow(largeData)
	if err == nil {
		t.Error("expected error for insufficient space")
	}
}

func TestPage_GetRow(t *testing.T) {
	p := NewPage(1, PageTypeData)

	data := []byte("test data")
	idx, _ := p.InsertRow(data)

	got, err := p.GetRow(idx)
	if err != nil {
		t.Fatalf("GetRow failed: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("GetRow() = %s, want %s", got, data)
	}
}

func TestPage_GetRow_OutOfRange(t *testing.T) {
	p := NewPage(1, PageTypeData)

	_, err := p.GetRow(0)
	if err == nil {
		t.Error("expected error for out of range")
	}

	_, err = p.GetRow(-1)
	if err == nil {
		t.Error("expected error for negative index")
	}
}

func TestPage_DeleteRow(t *testing.T) {
	p := NewPage(1, PageTypeData)

	p.InsertRow([]byte("row1"))
	p.InsertRow([]byte("row2"))

	err := p.DeleteRow(0)
	if err != nil {
		t.Fatalf("DeleteRow failed: %v", err)
	}

	// Verify slot is marked as deleted (offset = 0)
	slot := p.GetSlot(0)
	if slot.Offset != 0 || slot.Length != 0 {
		t.Errorf("deleted slot = {Offset: %d, Length: %d}, want {0, 0}", slot.Offset, slot.Length)
	}

	// Try to delete again - should fail
	err = p.DeleteRow(0)
	if err == nil {
		t.Error("expected error deleting already deleted row")
	}
}

func TestPage_DeleteRow_OutOfRange(t *testing.T) {
	p := NewPage(1, PageTypeData)

	err := p.DeleteRow(0)
	if err == nil {
		t.Error("expected error for out of range")
	}
}

func TestPage_UpdateRow(t *testing.T) {
	p := NewPage(1, PageTypeData)

	p.InsertRow([]byte("original data"))

	// Update with same or smaller size
	err := p.UpdateRow(0, []byte("new data"))
	if err != nil {
		t.Fatalf("UpdateRow failed: %v", err)
	}

	got, _ := p.GetRow(0)
	if string(got) != "new data" {
		t.Errorf("GetRow() = %s, want 'new data'", got)
	}
}

func TestPage_UpdateRow_LargerData(t *testing.T) {
	p := NewPage(1, PageTypeData)

	p.InsertRow([]byte("short"))

	// Try to update with larger data - should fail
	err := p.UpdateRow(0, []byte("this is much longer data"))
	if err == nil {
		t.Error("expected error for larger data")
	}
}

func TestPage_UpdateRow_DeletedRow(t *testing.T) {
	p := NewPage(1, PageTypeData)

	p.InsertRow([]byte("data"))
	p.DeleteRow(0)

	err := p.UpdateRow(0, []byte("new"))
	if err == nil {
		t.Error("expected error updating deleted row")
	}
}

func TestPage_UpdateRow_OutOfRange(t *testing.T) {
	p := NewPage(1, PageTypeData)

	err := p.UpdateRow(0, []byte("data"))
	if err == nil {
		t.Error("expected error for out of range")
	}
}

func TestPage_SerializeRoundTrip(t *testing.T) {
	p1 := NewPage(1, PageTypeData)
	p1.SetLSN(100)
	p1.SetLeaf(true)
	p1.InsertRow([]byte("row1"))
	p1.InsertRow([]byte("row2"))

	// Serialize
	data := p1.ToBytes()

	// Deserialize
	p2, err := NewPageFromBytes(data)
	if err != nil {
		t.Fatalf("NewPageFromBytes failed: %v", err)
	}

	if p2.ID != p1.ID {
		t.Errorf("ID = %d, want %d", p2.ID, p1.ID)
	}
	if p2.Type != p1.Type {
		t.Errorf("Type = %v, want %v", p2.Type, p1.Type)
	}
	if p2.GetLSN() != p1.GetLSN() {
		t.Errorf("LSN = %d, want %d", p2.GetLSN(), p1.GetLSN())
	}
	if !p2.IsLeaf() {
		t.Error("should be leaf")
	}
	if p2.RowCount() != 2 {
		t.Errorf("row count = %d, want 2", p2.RowCount())
	}
}

func TestNewPageFromBytes_InvalidSize(t *testing.T) {
	_, err := NewPageFromBytes([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for invalid size")
	}
}

func TestNewPageFromBytes_ChecksumMismatch(t *testing.T) {
	data := make([]byte, PageSize)
	// Don't compute proper checksum, should fail
	_, err := NewPageFromBytes(data)
	if err == nil {
		t.Error("expected error for checksum mismatch")
	}
}

func TestPage_RowCount(t *testing.T) {
	p := NewPage(1, PageTypeData)

	if p.RowCount() != 0 {
		t.Errorf("initial row count = %d, want 0", p.RowCount())
	}

	p.InsertRow([]byte("row1"))
	if p.RowCount() != 1 {
		t.Errorf("row count after insert = %d, want 1", p.RowCount())
	}

	p.InsertRow([]byte("row2"))
	if p.RowCount() != 2 {
		t.Errorf("row count after second insert = %d, want 2", p.RowCount())
	}
}

func TestPage_Modified(t *testing.T) {
	p := NewPage(1, PageTypeData)
	if !p.Modified {
		t.Error("new page should be modified")
	}

	// Clear modified flag
	p.Modified = false

	// Operations should set modified flag
	p.InsertRow([]byte("data"))
	if !p.Modified {
		t.Error("InsertRow should set modified")
	}

	p.Modified = false
	p.DeleteRow(0)
	if !p.Modified {
		t.Error("DeleteRow should set modified")
	}
}

func BenchmarkPage_InsertRow(b *testing.B) {
	p := NewPage(1, PageTypeData)
	data := []byte("benchmark data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.InsertRow(data)
		if p.FreeSpace() < 100 {
			p = NewPage(1, PageTypeData)
		}
	}
}

func BenchmarkPage_GetRow(b *testing.B) {
	p := NewPage(1, PageTypeData)
	for i := 0; i < 100; i++ {
		p.InsertRow([]byte("data"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.GetRow(i % 100)
	}
}
