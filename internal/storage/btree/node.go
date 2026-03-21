// Package btree provides B+ tree index implementation for XxSql.
package btree

import (
	"encoding/binary"
	"fmt"

	"github.com/topxeq/xxsql/internal/storage/page"
	"github.com/topxeq/xxsql/internal/storage/types"
)

const (
	// DefaultOrder is the default B+ tree order (max children per node).
	// With 4KB pages, we can fit approximately:
	// - For 8-byte keys + 8-byte values: ~200 entries per leaf node
	// - For 8-byte keys + 8-byte page IDs: ~200 entries per internal node
	DefaultOrder = 128

	// MinOrder is the minimum B+ tree order.
	MinOrder = 4
)

// Key represents an index key.
type Key struct {
	Value types.Value
	RowID uint64 // For non-unique indexes, this makes the key unique
}

// Compare compares two keys.
func (k Key) Compare(other Key) int {
	cmp := k.Value.Compare(other.Value)
	if cmp != 0 {
		return cmp
	}
	// If values are equal, compare by row ID to support non-unique indexes
	if k.RowID < other.RowID {
		return -1
	} else if k.RowID > other.RowID {
		return 1
	}
	return 0
}

// Entry represents a key-value entry in a leaf node.
type Entry struct {
	Key   Key
	Value []byte // Row data or row ID
}

// Node represents a B+ tree node.
type Node struct {
	PageID     page.PageID
	IsLeaf     bool
	Keys       []Key
	Children   []page.PageID // For internal nodes
	Entries    []Entry       // For leaf nodes
	Next       page.PageID   // For leaf nodes (pointer to next leaf)
	Parent     page.PageID
	Modified   bool
}

// NewNode creates a new B+ tree node.
func NewNode(pageID page.PageID, isLeaf bool) *Node {
	return &Node{
		PageID:   pageID,
		IsLeaf:   isLeaf,
		Keys:     make([]Key, 0),
		Children: make([]page.PageID, 0),
		Entries:  make([]Entry, 0),
		Next:     page.InvalidPageID,
		Parent:   page.InvalidPageID,
		Modified: true,
	}
}

// IsFull returns true if the node is full.
func (n *Node) IsFull(order int) bool {
	if n.IsLeaf {
		return len(n.Entries) >= order
	}
	return len(n.Keys) >= order-1
}

// IsUnderflow returns true if the node has too few entries.
func (n *Node) IsUnderflow(order int) bool {
	minKeys := order / 2
	if n.IsLeaf {
		return len(n.Entries) < minKeys
	}
	return len(n.Keys) < minKeys
}

// CanLend returns true if the node can lend an entry to a sibling.
func (n *Node) CanLend(order int) bool {
	minKeys := order / 2
	if n.IsLeaf {
		return len(n.Entries) > minKeys
	}
	return len(n.Keys) > minKeys
}

// Serialize serializes the node to bytes for page storage.
func (n *Node) Serialize() []byte {
	// Calculate size
	size := 1 + // isLeaf flag
		2 + // key count
		8 + // next page ID
		8 + // parent page ID
		4   // reserved

	if n.IsLeaf {
		// Leaf: entries (key + value length + value)
		for _, e := range n.Entries {
			size += e.Key.Value.Size() + 2 + len(e.Value)
		}
	} else {
		// Internal: children + keys
		size += 8 * (len(n.Keys) + 1) // children
		for _, k := range n.Keys {
			size += k.Value.Size()
		}
	}

	buf := make([]byte, size)
	offset := 0

	// IsLeaf flag
	if n.IsLeaf {
		buf[0] = 1
	}
	offset++

	// Key count
	binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(n.Keys)))
	offset += 2

	// Next page ID
	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(n.Next))
	offset += 8

	// Parent page ID
	binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(n.Parent))
	offset += 8

	// Reserved
	offset += 4

	if n.IsLeaf {
		// Write entries
		for _, e := range n.Entries {
			// Write key
			keyData := e.Key.Value.Marshal()
			copy(buf[offset:], keyData)
			offset += len(keyData)

			// Write value length
			binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(e.Value)))
			offset += 2

			// Write value
			copy(buf[offset:], e.Value)
			offset += len(e.Value)
		}
	} else {
		// Write children (one more than keys)
		for _, child := range n.Children {
			binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(child))
			offset += 8
		}

		// Write keys
		for _, k := range n.Keys {
			keyData := k.Value.Marshal()
			copy(buf[offset:], keyData)
			offset += len(keyData)
		}
	}

	return buf
}

// DeserializeNode deserializes a node from bytes.
func DeserializeNode(data []byte, pageID page.PageID) (*Node, error) {
	if len(data) < 23 {
		return nil, fmt.Errorf("data too short for node header")
	}

	n := &Node{
		PageID: pageID,
	}

	offset := 0

	// IsLeaf flag
	n.IsLeaf = data[0] == 1
	offset++

	// Key count
	keyCount := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
	offset += 2

	// Next page ID
	n.Next = page.PageID(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8

	// Parent page ID
	n.Parent = page.PageID(binary.LittleEndian.Uint64(data[offset : offset+8]))
	offset += 8

	// Reserved
	offset += 4

	if n.IsLeaf {
		// Read entries
		n.Entries = make([]Entry, 0, keyCount)
		for i := 0; i < keyCount && offset < len(data); i++ {
			entry := Entry{}

			// We need type info to deserialize key - assume INT for now
			// This should be passed from the tree
			keyVal, bytesRead := types.UnmarshalValue(data[offset:], types.TypeInt)
			entry.Key = Key{Value: keyVal}
			offset += bytesRead

			if offset+2 > len(data) {
				break
			}

			// Value length
			valLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
			offset += 2

			if offset+valLen > len(data) {
				valLen = len(data) - offset
			}

			// Value
			entry.Value = make([]byte, valLen)
			copy(entry.Value, data[offset:offset+valLen])
			offset += valLen

			n.Entries = append(n.Entries, entry)
		}
		n.Keys = make([]Key, len(n.Entries))
		for i, e := range n.Entries {
			n.Keys[i] = e.Key
		}
	} else {
		// Read children (keyCount + 1)
		n.Children = make([]page.PageID, 0, keyCount+1)
		for i := 0; i <= keyCount && offset+8 <= len(data); i++ {
			child := page.PageID(binary.LittleEndian.Uint64(data[offset : offset+8]))
			n.Children = append(n.Children, child)
			offset += 8
		}

		// Read keys
		n.Keys = make([]Key, 0, keyCount)
		for i := 0; i < keyCount && offset < len(data); i++ {
			keyVal, bytesRead := types.UnmarshalValue(data[offset:], types.TypeInt)
			n.Keys = append(n.Keys, Key{Value: keyVal})
			offset += bytesRead
		}
	}

	return n, nil
}

// String returns a string representation of the node.
func (n *Node) String() string {
	if n.IsLeaf {
		return fmt.Sprintf("LeafNode{page=%d, entries=%d, next=%d}", n.PageID, len(n.Entries), n.Next)
	}
	return fmt.Sprintf("InternalNode{page=%d, keys=%d, children=%d}", n.PageID, len(n.Keys), len(n.Children))
}
