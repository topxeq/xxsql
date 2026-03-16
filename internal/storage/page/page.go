// Package page provides page management for XxSql storage engine.
package page

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	// PageSize is the size of a page in bytes.
	PageSize = 4096

	// PageHeaderSize is the size of the page header in bytes.
	PageHeaderSize = 24

	// MaxDataSize is the maximum data size in a page.
	MaxDataSize = PageSize - PageHeaderSize
)

// PageType represents the type of a page.
type PageType uint8

const (
	PageTypeHeader PageType = iota // Database header page
	PageTypeData                   // Table data page
	PageTypeIndex                  // B+ tree index page
	PageTypeOverflow               // Overflow page for large values
	PageTypeFree                   // Free page in free list
)

// String returns the string representation of the page type.
func (t PageType) String() string {
	switch t {
	case PageTypeHeader:
		return "HEADER"
	case PageTypeData:
		return "DATA"
	case PageTypeIndex:
		return "INDEX"
	case PageTypeOverflow:
		return "OVERFLOW"
	case PageTypeFree:
		return "FREE"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", t)
	}
}

// PageID represents a unique page identifier.
type PageID uint64

// InvalidPageID represents an invalid page ID.
const InvalidPageID PageID = 0

// Page represents a database page.
type Page struct {
	ID       PageID
	Type     PageType
	Data     []byte
	Modified bool
}

// Header represents the page header (24 bytes).
// Layout:
//   - Bytes 0-7:   Page ID (8 bytes)
//   - Bytes 8-15:  LSN (Log Sequence Number) (8 bytes)
//   - Bytes 16:    Page Type (1 byte)
//   - Bytes 17:    Flags (1 byte)
//   - Bytes 18-19: Free space offset (2 bytes)
//   - Bytes 20-21: Slot count (2 bytes)
//   - Bytes 22-23: Checksum (2 bytes)
type Header struct {
	PageID      PageID
	LSN         uint64
	Type        PageType
	Flags       uint8
	FreeOffset  uint16
	SlotCount   uint16
	Checksum    uint16
}

// Page flags.
const (
	FlagLeaf     uint8 = 0x01 // Leaf page (vs internal)
	FlagRoot     uint8 = 0x02 // Root page of B+ tree
	FlagOverflow uint8 = 0x04 // Has overflow pages
	FlagDeleted  uint8 = 0x08 // Page is deleted
)

// NewPage creates a new page.
func NewPage(id PageID, typ PageType) *Page {
	p := &Page{
		ID:       id,
		Type:     typ,
		Data:     make([]byte, PageSize),
		Modified: true,
	}

	// Initialize header
	p.SetPageID(id)
	p.SetType(typ)
	p.SetFreeOffset(PageHeaderSize)
	p.SetSlotCount(0)
	p.SetLSN(0)
	p.SetFlags(0)

	return p
}

// NewPageFromBytes creates a page from bytes.
func NewPageFromBytes(data []byte) (*Page, error) {
	if len(data) != PageSize {
		return nil, fmt.Errorf("invalid page size: expected %d, got %d", PageSize, len(data))
	}

	p := &Page{
		Data: data,
	}

	// Parse header
	p.ID = p.GetPageID()
	p.Type = p.GetType()
	p.Modified = false

	// Verify checksum
	storedChecksum := p.GetChecksum()

	// Compute checksum over header (0-21) and data area (24-end)
	// The checksum field is at bytes 22-23
	headerPart := data[0:22]
	dataPart := data[24:]

	// Combine parts for checksum
	combined := make([]byte, 0, len(headerPart)+len(dataPart))
	combined = append(combined, headerPart...)
	combined = append(combined, dataPart...)

	computedChecksum := computeChecksum(combined)
	if storedChecksum != computedChecksum {
		return nil, fmt.Errorf("checksum mismatch: page %d may be corrupted", p.ID)
	}

	return p, nil
}

// ToBytes returns the page as bytes.
func (p *Page) ToBytes() []byte {
	// Compute checksum over header (0-21) and data area (24-end)
	// The checksum field is at bytes 22-23
	headerPart := p.Data[0:22]
	dataPart := p.Data[24:]

	// Combine parts for checksum
	combined := make([]byte, 0, len(headerPart)+len(dataPart))
	combined = append(combined, headerPart...)
	combined = append(combined, dataPart...)

	checksum := computeChecksum(combined)
	p.SetChecksum(checksum)
	return p.Data
}

// Header field accessors

// GetPageID returns the page ID from the header.
func (p *Page) GetPageID() PageID {
	return PageID(binary.LittleEndian.Uint64(p.Data[0:8]))
}

// SetPageID sets the page ID in the header.
func (p *Page) SetPageID(id PageID) {
	binary.LittleEndian.PutUint64(p.Data[0:8], uint64(id))
	p.ID = id
}

// GetLSN returns the LSN from the header.
func (p *Page) GetLSN() uint64 {
	return binary.LittleEndian.Uint64(p.Data[8:16])
}

// SetLSN sets the LSN in the header.
func (p *Page) SetLSN(lsn uint64) {
	binary.LittleEndian.PutUint64(p.Data[8:16], lsn)
}

// GetType returns the page type from the header.
func (p *Page) GetType() PageType {
	return PageType(p.Data[16])
}

// SetType sets the page type in the header.
func (p *Page) SetType(typ PageType) {
	p.Data[16] = byte(typ)
	p.Type = typ
}

// GetFlags returns the flags from the header.
func (p *Page) GetFlags() uint8 {
	return p.Data[17]
}

// SetFlags sets the flags in the header.
func (p *Page) SetFlags(flags uint8) {
	p.Data[17] = flags
}

// HasFlag checks if a flag is set.
func (p *Page) HasFlag(flag uint8) bool {
	return p.GetFlags()&flag != 0
}

// SetFlag sets a flag.
func (p *Page) SetFlag(flag uint8) {
	p.SetFlags(p.GetFlags() | flag)
}

// ClearFlag clears a flag.
func (p *Page) ClearFlag(flag uint8) {
	p.SetFlags(p.GetFlags() &^ flag)
}

// GetFreeOffset returns the free space offset.
func (p *Page) GetFreeOffset() uint16 {
	return binary.LittleEndian.Uint16(p.Data[18:20])
}

// SetFreeOffset sets the free space offset.
func (p *Page) SetFreeOffset(offset uint16) {
	binary.LittleEndian.PutUint16(p.Data[18:20], offset)
}

// GetSlotCount returns the slot count.
func (p *Page) GetSlotCount() uint16 {
	return binary.LittleEndian.Uint16(p.Data[20:22])
}

// SetSlotCount sets the slot count.
func (p *Page) SetSlotCount(count uint16) {
	binary.LittleEndian.PutUint16(p.Data[20:22], count)
}

// GetChecksum returns the checksum.
func (p *Page) GetChecksum() uint16 {
	return binary.LittleEndian.Uint16(p.Data[22:24])
}

// SetChecksum sets the checksum.
func (p *Page) SetChecksum(checksum uint16) {
	binary.LittleEndian.PutUint16(p.Data[22:24], checksum)
}

// FreeSpace returns the available free space in the page.
func (p *Page) FreeSpace() uint16 {
	freeOffset := p.GetFreeOffset()
	slotCount := p.GetSlotCount()
	slotTableSize := slotCount * 4 // Each slot entry is 4 bytes (2 offset + 2 length)
	return PageSize - uint16(slotTableSize) - freeOffset
}

// DataRange returns the data area range (header end to free offset).
func (p *Page) DataRange() (start, end uint16) {
	return PageHeaderSize, p.GetFreeOffset()
}

// SlotEntry represents a slot entry in the page.
type SlotEntry struct {
	Offset uint16
	Length uint16
}

// GetSlot returns the slot entry at the given index.
func (p *Page) GetSlot(index int) SlotEntry {
	slotStart := PageSize - (uint16(index)+1)*4
	offset := binary.LittleEndian.Uint16(p.Data[slotStart : slotStart+2])
	length := binary.LittleEndian.Uint16(p.Data[slotStart+2 : slotStart+4])
	return SlotEntry{Offset: offset, Length: length}
}

// SetSlot sets the slot entry at the given index.
func (p *Page) SetSlot(index int, entry SlotEntry) {
	slotStart := PageSize - (uint16(index)+1)*4
	binary.LittleEndian.PutUint16(p.Data[slotStart:slotStart+2], entry.Offset)
	binary.LittleEndian.PutUint16(p.Data[slotStart+2:slotStart+4], entry.Length)
}

// AppendSlot appends a new slot entry.
func (p *Page) AppendSlot(offset, length uint16) int {
	index := int(p.GetSlotCount())
	p.SetSlotCount(p.GetSlotCount() + 1)
	p.SetSlot(index, SlotEntry{Offset: offset, Length: length})
	return index
}

// InsertRow inserts a row at the end of the page.
// Returns the slot index or an error if there's not enough space.
func (p *Page) InsertRow(data []byte) (int, error) {
	rowLen := uint16(len(data))
	required := rowLen + 4 // data + slot entry

	if p.FreeSpace() < required {
		return -1, fmt.Errorf("not enough space in page: need %d, have %d", required, p.FreeSpace())
	}

	// Write data at free offset
	freeOffset := p.GetFreeOffset()
	copy(p.Data[freeOffset:freeOffset+rowLen], data)

	// Update free offset
	p.SetFreeOffset(freeOffset + rowLen)

	// Add slot entry
	slotIndex := p.AppendSlot(freeOffset, rowLen)

	p.Modified = true
	return slotIndex, nil
}

// GetRow returns the row data at the given slot index.
func (p *Page) GetRow(index int) ([]byte, error) {
	slotCount := p.GetSlotCount()
	if uint16(index) >= slotCount {
		return nil, fmt.Errorf("slot index out of range: %d >= %d", index, slotCount)
	}

	slot := p.GetSlot(index)
	data := make([]byte, slot.Length)
	copy(data, p.Data[slot.Offset:slot.Offset+slot.Length])
	return data, nil
}

// DeleteRow marks a row as deleted by setting its length to 0.
// This is a simple deletion - no compaction is performed.
func (p *Page) DeleteRow(index int) error {
	slotCount := p.GetSlotCount()
	if uint16(index) >= slotCount {
		return fmt.Errorf("slot index out of range: %d >= %d", index, slotCount)
	}

	// Mark as deleted by setting offset to 0
	slot := p.GetSlot(index)
	if slot.Offset == 0 {
		return fmt.Errorf("row already deleted")
	}

	p.SetSlot(index, SlotEntry{Offset: 0, Length: 0})
	p.Modified = true
	return nil
}

// UpdateRow updates the row data at the given slot index.
// Note: This only works if the new data is the same size or smaller.
// For larger data, the row should be deleted and reinserted.
func (p *Page) UpdateRow(index int, data []byte) error {
	slotCount := p.GetSlotCount()
	if uint16(index) >= slotCount {
		return fmt.Errorf("slot index out of range: %d >= %d", index, slotCount)
	}

	slot := p.GetSlot(index)
	if slot.Offset == 0 {
		return fmt.Errorf("row is deleted")
	}

	newLen := uint16(len(data))
	if newLen > slot.Length {
		return fmt.Errorf("new data is larger than existing row")
	}

	// Write data at existing offset
	copy(p.Data[slot.Offset:slot.Offset+newLen], data)

	// Update length if smaller
	if newLen != slot.Length {
		p.SetSlot(index, SlotEntry{Offset: slot.Offset, Length: newLen})
	}

	p.Modified = true
	return nil
}

// RowCount returns the number of rows (slots) in the page.
func (p *Page) RowCount() int {
	return int(p.GetSlotCount())
}

// IsLeaf returns true if this is a leaf page.
func (p *Page) IsLeaf() bool {
	return p.HasFlag(FlagLeaf)
}

// SetLeaf sets the leaf flag.
func (p *Page) SetLeaf(leaf bool) {
	if leaf {
		p.SetFlag(FlagLeaf)
	} else {
		p.ClearFlag(FlagLeaf)
	}
}

// IsRoot returns true if this is a root page.
func (p *Page) IsRoot() bool {
	return p.HasFlag(FlagRoot)
}

// SetRoot sets the root flag.
func (p *Page) SetRoot(root bool) {
	if root {
		p.SetFlag(FlagRoot)
	} else {
		p.ClearFlag(FlagRoot)
	}
}

// computeChecksum computes a CRC32 checksum.
func computeChecksum(data []byte) uint16 {
	return uint16(crc32.ChecksumIEEE(data) & 0xFFFF)
}
