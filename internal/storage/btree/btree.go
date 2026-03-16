// Package btree provides B+ tree index implementation for XxSql.
package btree

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/topxeq/xxsql/internal/storage/page"
	"github.com/topxeq/xxsql/internal/storage/types"
)

// BTree represents a B+ tree index.
type BTree struct {
	Root       page.PageID
	Order      int
	KeyType    types.TypeID
	NextPageID page.PageID

	// Node cache
	nodes    map[page.PageID]*Node
	nodeMu   sync.RWMutex

	// Page manager interface
	pageManager PageManager

	// For testing without persistence
	inMemory bool
}

// PageManager provides page management for the B+ tree.
type PageManager interface {
	AllocatePage() (*page.Page, error)
	GetPage(id page.PageID) (*page.Page, error)
	WritePage(p *page.Page) error
}

// NewBTree creates a new B+ tree.
func NewBTree(order int, keyType types.TypeID, pm PageManager) *BTree {
	if order < MinOrder {
		order = MinOrder
	}
	return &BTree{
		Root:        page.InvalidPageID,
		Order:       order,
		KeyType:     keyType,
		NextPageID:  1,
		nodes:       make(map[page.PageID]*Node),
		pageManager: pm,
		inMemory:    pm == nil,
	}
}

// NewInMemoryBTree creates an in-memory B+ tree for testing.
func NewInMemoryBTree(order int, keyType types.TypeID) *BTree {
	return NewBTree(order, keyType, nil)
}

// nextPageID generates the next page ID.
func (t *BTree) nextPageID() page.PageID {
	id := t.NextPageID
	t.NextPageID++
	return id
}

// getNode retrieves a node by page ID.
func (t *BTree) getNode(id page.PageID) (*Node, error) {
	if id == page.InvalidPageID {
		return nil, fmt.Errorf("invalid page ID")
	}

	t.nodeMu.RLock()
	if node, ok := t.nodes[id]; ok {
		t.nodeMu.RUnlock()
		return node, nil
	}
	t.nodeMu.RUnlock()

	// Load from page manager
	if t.pageManager != nil {
		p, err := t.pageManager.GetPage(id)
		if err != nil {
			return nil, err
		}

		node, err := DeserializeNode(p.Data[page.PageHeaderSize:], id)
		if err != nil {
			return nil, err
		}

		t.nodeMu.Lock()
		t.nodes[id] = node
		t.nodeMu.Unlock()

		return node, nil
	}

	return nil, fmt.Errorf("node not found: %d", id)
}

// putNode stores a node in cache.
func (t *BTree) putNode(node *Node) {
	t.nodeMu.Lock()
	t.nodes[node.PageID] = node
	t.nodeMu.Unlock()
}

// allocateNode creates a new node.
func (t *BTree) allocateNode(isLeaf bool) *Node {
	id := t.nextPageID()
	node := NewNode(id, isLeaf)
	t.putNode(node)

	// Persist to page manager
	if t.pageManager != nil {
		p := page.NewPage(id, page.PageTypeIndex)
		p.SetLeaf(isLeaf)
		t.pageManager.WritePage(p)
	}

	return node
}

// Search finds the value associated with the given key.
func (t *BTree) Search(key Key) ([]byte, bool) {
	if t.Root == page.InvalidPageID {
		return nil, false
	}

	return t.searchInNode(t.Root, key)
}

// searchInNode recursively searches for a key starting from a node.
func (t *BTree) searchInNode(nodeID page.PageID, key Key) ([]byte, bool) {
	node, err := t.getNode(nodeID)
	if err != nil {
		return nil, false
	}

	if node.IsLeaf {
		// Binary search in leaf
		for _, entry := range node.Entries {
			cmp := entry.Key.Compare(key)
			if cmp == 0 {
				return entry.Value, true
			}
			if cmp > 0 {
				break
			}
		}
		return nil, false
	}

	// Binary search in internal node
	i := 0
	for i < len(node.Keys) {
		cmp := node.Keys[i].Compare(key)
		if cmp > 0 {
			break
		}
		i++
	}

	if i >= len(node.Children) {
		i = len(node.Children) - 1
	}

	return t.searchInNode(node.Children[i], key)
}

// Insert inserts a key-value pair into the tree.
func (t *BTree) Insert(key Key, value []byte) error {
	if t.Root == page.InvalidPageID {
		// Create root
		root := t.allocateNode(true)
		root.Entries = append(root.Entries, Entry{Key: key, Value: value})
		root.Keys = append(root.Keys, key)
		t.Root = root.PageID
		return nil
	}

	// Insert into tree
	newKey, newChild, err := t.insertInNode(t.Root, key, value)
	if err != nil {
		return err
	}

	// If root was split, create new root
	if newChild != nil {
		newRoot := t.allocateNode(false)
		newRoot.Keys = append(newRoot.Keys, newKey)
		newRoot.Children = append(newRoot.Children, t.Root, newChild.PageID)

		// Update parent pointers
		oldRoot, _ := t.getNode(t.Root)
		if oldRoot != nil {
			oldRoot.Parent = newRoot.PageID
			oldRoot.Modified = true
		}
		newChild.Parent = newRoot.PageID
		newChild.Modified = true

		t.Root = newRoot.PageID
	}

	return nil
}

// insertInNode recursively inserts into a node.
func (t *BTree) insertInNode(nodeID page.PageID, key Key, value []byte) (Key, *Node, error) {
	node, err := t.getNode(nodeID)
	if err != nil {
		return Key{}, nil, err
	}

	if node.IsLeaf {
		// Insert into leaf
		return t.insertInLeaf(node, key, value)
	}

	// Find child to insert into
	i := 0
	for i < len(node.Keys) {
		cmp := node.Keys[i].Compare(key)
		if cmp > 0 {
			break
		}
		i++
	}

	if i >= len(node.Children) {
		i = len(node.Children) - 1
	}

	// Recurse into child
	newKey, newChild, err := t.insertInNode(node.Children[i], key, value)
	if err != nil {
		return Key{}, nil, err
	}

	// If child was split, insert new key and child
	if newChild != nil {
		return t.insertInInternal(node, newKey, newChild, i)
	}

	return Key{}, nil, nil
}

// insertInLeaf inserts into a leaf node.
func (t *BTree) insertInLeaf(node *Node, key Key, value []byte) (Key, *Node, error) {
	// Find insertion position
	pos := 0
	for pos < len(node.Entries) {
		cmp := node.Entries[pos].Key.Compare(key)
		if cmp == 0 {
			// Update existing entry
			node.Entries[pos].Value = value
			node.Modified = true
			return Key{}, nil, nil
		}
		if cmp > 0 {
			break
		}
		pos++
	}

	// Insert new entry
	entry := Entry{Key: key, Value: value}
	node.Entries = append(node.Entries, Entry{})
	copy(node.Entries[pos+1:], node.Entries[pos:])
	node.Entries[pos] = entry

	// Update keys
	node.Keys = make([]Key, len(node.Entries))
	for i, e := range node.Entries {
		node.Keys[i] = e.Key
	}

	node.Modified = true

	// Check if split needed
	if len(node.Entries) > t.Order {
		return t.splitLeaf(node)
	}

	return Key{}, nil, nil
}

// insertInInternal inserts a key and child into an internal node.
func (t *BTree) insertInInternal(node *Node, key Key, child *Node, pos int) (Key, *Node, error) {
	// Insert key
	node.Keys = append(node.Keys, Key{})
	copy(node.Keys[pos+1:], node.Keys[pos:])
	node.Keys[pos] = key

	// Insert child
	childIdx := pos + 1
	node.Children = append(node.Children, page.PageID(0))
	copy(node.Children[childIdx+1:], node.Children[childIdx:])
	node.Children[childIdx] = child.PageID
	child.Parent = node.PageID
	child.Modified = true

	node.Modified = true

	// Check if split needed
	if len(node.Keys) >= t.Order {
		return t.splitInternal(node)
	}

	return Key{}, nil, nil
}

// splitLeaf splits a leaf node.
func (t *BTree) splitLeaf(node *Node) (Key, *Node, error) {
	// Create new leaf
	newLeaf := t.allocateNode(true)
	newLeaf.Next = node.Next
	newLeaf.Parent = node.Parent

	// Split entries
	mid := len(node.Entries) / 2
	newLeaf.Entries = make([]Entry, len(node.Entries)-mid)
	copy(newLeaf.Entries, node.Entries[mid:])
	node.Entries = node.Entries[:mid]

	// Update keys
	node.Keys = make([]Key, len(node.Entries))
	for i, e := range node.Entries {
		node.Keys[i] = e.Key
	}
	newLeaf.Keys = make([]Key, len(newLeaf.Entries))
	for i, e := range newLeaf.Entries {
		newLeaf.Keys[i] = e.Key
	}

	// Link leaves
	node.Next = newLeaf.PageID

	node.Modified = true
	newLeaf.Modified = true

	// Return first key of new leaf and the new leaf
	return newLeaf.Keys[0], newLeaf, nil
}

// splitInternal splits an internal node.
func (t *BTree) splitInternal(node *Node) (Key, *Node, error) {
	// Create new internal node
	newInternal := t.allocateNode(false)
	newInternal.Parent = node.Parent

	// Split keys and children
	mid := len(node.Keys) / 2

	// The middle key goes up
	promotedKey := node.Keys[mid]

	// New internal gets keys after mid
	newInternal.Keys = make([]Key, len(node.Keys)-mid-1)
	copy(newInternal.Keys, node.Keys[mid+1:])

	// New internal gets children after mid
	newInternal.Children = make([]page.PageID, len(node.Children)-mid-1)
	copy(newInternal.Children, node.Children[mid+1:])

	// Update parent pointers for moved children
	for _, childID := range newInternal.Children {
		child, err := t.getNode(childID)
		if err == nil {
			child.Parent = newInternal.PageID
			child.Modified = true
		}
	}

	// Keep keys before mid
	node.Keys = node.Keys[:mid]
	node.Children = node.Children[:mid+1]

	node.Modified = true
	newInternal.Modified = true

	return promotedKey, newInternal, nil
}

// Delete removes a key from the tree.
func (t *BTree) Delete(key Key) error {
	if t.Root == page.InvalidPageID {
		return fmt.Errorf("tree is empty")
	}

	return t.deleteInNode(t.Root, key)
}

// deleteInNode recursively deletes from a node.
func (t *BTree) deleteInNode(nodeID page.PageID, key Key) error {
	node, err := t.getNode(nodeID)
	if err != nil {
		return err
	}

	if node.IsLeaf {
		return t.deleteFromLeaf(node, key)
	}

	// Find child
	i := 0
	for i < len(node.Keys) {
		cmp := node.Keys[i].Compare(key)
		if cmp > 0 {
			break
		}
		i++
	}

	if i >= len(node.Children) {
		i = len(node.Children) - 1
	}

	return t.deleteInNode(node.Children[i], key)
}

// deleteFromLeaf deletes a key from a leaf node.
func (t *BTree) deleteFromLeaf(node *Node, key Key) error {
	// Find key
	pos := -1
	for i, entry := range node.Entries {
		if entry.Key.Compare(key) == 0 {
			pos = i
			break
		}
	}

	if pos == -1 {
		return fmt.Errorf("key not found")
	}

	// Remove entry
	copy(node.Entries[pos:], node.Entries[pos+1:])
	node.Entries = node.Entries[:len(node.Entries)-1]

	// Update keys
	node.Keys = make([]Key, len(node.Entries))
	for i, e := range node.Entries {
		node.Keys[i] = e.Key
	}

	node.Modified = true

	// Note: For simplicity, we don't implement rebalancing on delete
	// In a production implementation, you would merge or redistribute nodes

	return nil
}

// Range returns all entries in the given key range [start, end].
func (t *BTree) Range(start, end Key) []Entry {
	if t.Root == page.InvalidPageID {
		return nil
	}

	var result []Entry

	// Find starting leaf
	leaf := t.findLeaf(t.Root, start)
	if leaf == nil {
		return nil
	}

	// Scan leaves until we pass the end key
	for leaf != nil {
		for _, entry := range leaf.Entries {
			if entry.Key.Compare(end) > 0 {
				return result
			}
			if entry.Key.Compare(start) >= 0 {
				result = append(result, entry)
			}
		}
		if leaf.Next == page.InvalidPageID {
			break
		}
		leaf, _ = t.getNode(leaf.Next)
	}

	return result
}

// findLeaf finds the leaf node where a key should be located.
func (t *BTree) findLeaf(nodeID page.PageID, key Key) *Node {
	node, err := t.getNode(nodeID)
	if err != nil {
		return nil
	}

	if node.IsLeaf {
		return node
	}

	// Find child
	i := 0
	for i < len(node.Keys) {
		cmp := node.Keys[i].Compare(key)
		if cmp > 0 {
			break
		}
		i++
	}

	if i >= len(node.Children) {
		i = len(node.Children) - 1
	}

	return t.findLeaf(node.Children[i], key)
}

// Scan returns all entries in the tree.
func (t *BTree) Scan() []Entry {
	if t.Root == page.InvalidPageID {
		return nil
	}

	// Find leftmost leaf
	node, err := t.getNode(t.Root)
	if err != nil {
		return nil
	}

	for !node.IsLeaf {
		if len(node.Children) == 0 {
			return nil
		}
		node, err = t.getNode(node.Children[0])
		if err != nil {
			return nil
		}
	}

	var result []Entry

	// Scan all leaves
	for node != nil {
		result = append(result, node.Entries...)
		if node.Next == page.InvalidPageID {
			break
		}
		node, err = t.getNode(node.Next)
		if err != nil {
			break
		}
	}

	return result
}

// Count returns the number of entries in the tree.
func (t *BTree) Count() int {
	entries := t.Scan()
	return len(entries)
}

// Height returns the height of the tree.
func (t *BTree) Height() int {
	if t.Root == page.InvalidPageID {
		return 0
	}

	height := 0
	node, err := t.getNode(t.Root)
	if err != nil {
		return 0
	}

	for !node.IsLeaf {
		height++
		if len(node.Children) == 0 {
			break
		}
		node, err = t.getNode(node.Children[0])
		if err != nil {
			break
		}
	}

	return height + 1
}

// Flush persists all modified nodes.
func (t *BTree) Flush() error {
	if t.pageManager == nil {
		return nil
	}

	t.nodeMu.RLock()
	defer t.nodeMu.RUnlock()

	for _, node := range t.nodes {
		if node.Modified {
			p, err := t.pageManager.GetPage(node.PageID)
			if err != nil {
				// Create new page
				p = page.NewPage(node.PageID, page.PageTypeIndex)
			}

			p.SetLeaf(node.IsLeaf)
			data := node.Serialize()
			copy(p.Data[page.PageHeaderSize:], data)
			p.Modified = true

			if err := t.pageManager.WritePage(p); err != nil {
				return err
			}
			node.Modified = false
		}
	}

	return nil
}

// Serialize serializes the tree metadata.
func (t *BTree) Serialize() []byte {
	buf := make([]byte, 21)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(t.Root))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(t.Order))
	buf[12] = byte(t.KeyType)
	binary.LittleEndian.PutUint64(buf[13:21], uint64(t.NextPageID))
	return buf
}

// DeserializeBTree deserializes tree metadata.
func DeserializeBTree(data []byte, pm PageManager) (*BTree, error) {
	if len(data) < 21 {
		return nil, fmt.Errorf("data too short for B+ tree metadata")
	}

	root := page.PageID(binary.LittleEndian.Uint64(data[0:8]))
	order := int(binary.LittleEndian.Uint32(data[8:12]))
	keyType := types.TypeID(data[12])
	nextPageID := page.PageID(binary.LittleEndian.Uint64(data[13:21]))

	t := NewBTree(order, keyType, pm)
	t.Root = root
	t.NextPageID = nextPageID

	return t, nil
}
