package database

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

const (
	// B-tree degree (minimum number of children per non-root node)
	// A node can have at most 2*degree-1 keys and 2*degree children
	DefaultDegree = 4
)

// BTreeNode represents a node in the B-tree
type BTreeNode struct {
	IsLeaf   bool
	Keys     []interface{}
	Values   [][]int64 // Row IDs for each key (can have duplicates for non-unique indexes)
	Children []*BTreeNode
}

// BTree represents a B-tree index
type BTree struct {
	Root   *BTreeNode
	Degree int
}

// Index represents an index on a table
type Index struct {
	Name      string
	TableName string
	Columns   []string
	IsUnique  bool
	Tree      *BTree
}

// NewBTree creates a new B-tree with the specified degree
func NewBTree(degree int) *BTree {
	if degree < 2 {
		degree = DefaultDegree
	}
	return &BTree{
		Root:   &BTreeNode{IsLeaf: true},
		Degree: degree,
	}
}

// NewIndex creates a new index
func NewIndex(name string, tableName string, columns []string, isUnique bool) *Index {
	return &Index{
		Name:      name,
		TableName: tableName,
		Columns:   columns,
		IsUnique:  isUnique,
		Tree:      NewBTree(DefaultDegree),
	}
}

// Search searches for a key in the B-tree
func (t *BTree) Search(key interface{}) ([]int64, bool) {
	if t.Root == nil {
		return nil, false
	}
	return t.searchNode(t.Root, key)
}

// searchNode searches for a key in a node
func (t *BTree) searchNode(node *BTreeNode, key interface{}) ([]int64, bool) {
	i := 0
	for i < len(node.Keys) && compareKeys(node.Keys[i], key) < 0 {
		i++
	}

	if i < len(node.Keys) && compareKeys(node.Keys[i], key) == 0 {
		return node.Values[i], true
	}

	if node.IsLeaf {
		return nil, false
	}

	return t.searchNode(node.Children[i], key)
}

// Insert inserts a key-value pair into the B-tree
func (t *BTree) Insert(key interface{}, rowID int64) error {
	// If the root is full, split it
	if len(t.Root.Keys) == 2*t.Degree-1 {
		newRoot := &BTreeNode{IsLeaf: false}
		newRoot.Children = append(newRoot.Children, t.Root)
		t.splitChild(newRoot, 0)
		t.Root = newRoot
	}
	return t.insertNonFull(t.Root, key, rowID)
}

// insertNonFull inserts a key-value pair into a non-full node
func (t *BTree) insertNonFull(node *BTreeNode, key interface{}, rowID int64) error {
	i := len(node.Keys) - 1

	if node.IsLeaf {
		// Find the position to insert the key
		for i >= 0 && compareKeys(node.Keys[i], key) > 0 {
			i--
		}

		// Check if the key already exists
		if i >= 0 && compareKeys(node.Keys[i], key) == 0 {
			// For duplicate keys, just append the rowID to the values
			node.Values[i] = append(node.Values[i], rowID)
			return nil
		}

		// Insert the key at the correct position
		insertPos := i + 1
		node.Keys = append(node.Keys, nil)
		node.Values = append(node.Values, nil)
		copy(node.Keys[insertPos+1:], node.Keys[insertPos:])
		copy(node.Values[insertPos+1:], node.Values[insertPos:])
		node.Keys[insertPos] = key
		node.Values[insertPos] = []int64{rowID}
	} else {
		// Find the child to insert into
		for i >= 0 && compareKeys(node.Keys[i], key) > 0 {
			i--
		}

		// Check if the key already exists
		if i >= 0 && compareKeys(node.Keys[i], key) == 0 {
			// For duplicate keys, just append the rowID to the values
			node.Values[i] = append(node.Values[i], rowID)
			return nil
		}

		childIndex := i + 1

		// If the child is full, split it
		if len(node.Children[childIndex].Keys) == 2*t.Degree-1 {
			t.splitChild(node, childIndex)
			if compareKeys(node.Keys[childIndex], key) < 0 {
				childIndex++
			}
		}

		return t.insertNonFull(node.Children[childIndex], key, rowID)
	}

	return nil
}

// splitChild splits a full child of a node
func (t *BTree) splitChild(parent *BTreeNode, childIndex int) {
	child := parent.Children[childIndex]
	newChild := &BTreeNode{IsLeaf: child.IsLeaf}

	// Move the right half of the keys and values to the new child
	midIndex := t.Degree - 1
	newChild.Keys = append(newChild.Keys, child.Keys[midIndex+1:]...)
	newChild.Values = append(newChild.Values, child.Values[midIndex+1:]...)

	// If the child is not a leaf, move the right half of the children too
	if !child.IsLeaf {
		newChild.Children = append(newChild.Children, child.Children[midIndex+1:]...)
		child.Children = child.Children[:midIndex+1]
	}

	// Truncate the child
	child.Keys = child.Keys[:midIndex]
	child.Values = child.Values[:midIndex]

	// Insert the middle key and the new child into the parent
	parent.Keys = append(parent.Keys, nil)
	parent.Values = append(parent.Values, nil)
	parent.Children = append(parent.Children, nil)

	copy(parent.Keys[childIndex+1:], parent.Keys[childIndex:])
	copy(parent.Values[childIndex+1:], parent.Values[childIndex:])
	copy(parent.Children[childIndex+2:], parent.Children[childIndex+1:])

	parent.Keys[childIndex] = child.Keys[midIndex]
	parent.Values[childIndex] = child.Values[midIndex]
	parent.Children[childIndex+1] = newChild
}

// Delete deletes a key from the B-tree
func (t *BTree) Delete(key interface{}, rowID int64) error {
	if t.Root == nil {
		return errors.New("tree is empty")
	}

	err := t.deleteFromNode(t.Root, key, rowID)

	// If the root has no keys and is not a leaf, make its first child the new root
	if len(t.Root.Keys) == 0 && !t.Root.IsLeaf {
		t.Root = t.Root.Children[0]
	}

	return err
}

// deleteFromNode deletes a key from a node
func (t *BTree) deleteFromNode(node *BTreeNode, key interface{}, rowID int64) error {
	// Find the position of the key
	i := 0
	for i < len(node.Keys) && compareKeys(node.Keys[i], key) < 0 {
		i++
	}

	// If the key is in this node
	if i < len(node.Keys) && compareKeys(node.Keys[i], key) == 0 {
		// If this is a leaf node, simply remove the key
		if node.IsLeaf {
			// Remove the rowID from the values
			values := node.Values[i]
			newValues := make([]int64, 0, len(values)-1)
			for _, v := range values {
				if v != rowID {
					newValues = append(newValues, v)
				}
			}

			// If there are still values, update the values
			if len(newValues) > 0 {
				node.Values[i] = newValues
				return nil
			}

			// Otherwise, remove the key and values
			node.Keys = append(node.Keys[:i], node.Keys[i+1:]...)
			node.Values = append(node.Values[:i], node.Values[i+1:]...)
		} else {
			// If this is an internal node, replace with predecessor or successor
			// For simplicity, we'll always use the predecessor
			pred, predValues := t.getPredecessor(node, i)
			node.Keys[i] = pred
			node.Values[i] = predValues
			return t.deleteFromNode(node.Children[i], pred, rowID)
		}
	} else {
		// If the key is not in this node
		if node.IsLeaf {
			return fmt.Errorf("key not found: %v", key)
		}

		// Ensure the child has enough keys
		if len(node.Children[i].Keys) < t.Degree {
			t.fillChild(node, i)
		}

		// If the last child has been merged, it might not exist anymore
		if i > len(node.Children)-1 {
			i--
		}

		return t.deleteFromNode(node.Children[i], key, rowID)
	}

	return nil
}

// getPredecessor gets the predecessor of a key in a node
func (t *BTree) getPredecessor(node *BTreeNode, index int) (interface{}, []int64) {
	current := node.Children[index]
	for !current.IsLeaf {
		current = current.Children[len(current.Children)-1]
	}
	return current.Keys[len(current.Keys)-1], current.Values[len(current.Values)-1]
}

// fillChild ensures that a child has at least t keys
func (t *BTree) fillChild(node *BTreeNode, index int) {
	// If the previous child has extra keys, borrow from it
	if index > 0 && len(node.Children[index-1].Keys) >= t.Degree {
		t.borrowFromPrev(node, index)
	} else if index < len(node.Children)-1 && len(node.Children[index+1].Keys) >= t.Degree {
		// If the next child has extra keys, borrow from it
		t.borrowFromNext(node, index)
	} else {
		// Otherwise, merge with a sibling
		if index < len(node.Children)-1 {
			t.mergeChildren(node, index)
		} else {
			t.mergeChildren(node, index-1)
		}
	}
}

// borrowFromPrev borrows a key from the previous child
func (t *BTree) borrowFromPrev(node *BTreeNode, index int) {
	child := node.Children[index]
	sibling := node.Children[index-1]

	// Make space for the new key in the child
	child.Keys = append(child.Keys, nil)
	child.Values = append(child.Values, nil)
	copy(child.Keys[1:], child.Keys)
	copy(child.Values[1:], child.Values)

	// If the child is not a leaf, move a child pointer too
	if !child.IsLeaf {
		child.Children = append(child.Children, nil)
		copy(child.Children[1:], child.Children)
		child.Children[0] = sibling.Children[len(sibling.Children)-1]
		sibling.Children = sibling.Children[:len(sibling.Children)-1]
	}

	// Move a key from the parent to the child
	child.Keys[0] = node.Keys[index-1]
	child.Values[0] = node.Values[index-1]

	// Move a key from the sibling to the parent
	node.Keys[index-1] = sibling.Keys[len(sibling.Keys)-1]
	node.Values[index-1] = sibling.Values[len(sibling.Values)-1]

	// Remove the key from the sibling
	sibling.Keys = sibling.Keys[:len(sibling.Keys)-1]
	sibling.Values = sibling.Values[:len(sibling.Values)-1]
}

// borrowFromNext borrows a key from the next child
func (t *BTree) borrowFromNext(node *BTreeNode, index int) {
	child := node.Children[index]
	sibling := node.Children[index+1]

	// Move a key from the parent to the child
	child.Keys = append(child.Keys, node.Keys[index])
	child.Values = append(child.Values, node.Values[index])

	// If the child is not a leaf, move a child pointer too
	if !child.IsLeaf {
		child.Children = append(child.Children, sibling.Children[0])
		sibling.Children = sibling.Children[1:]
	}

	// Move a key from the sibling to the parent
	node.Keys[index] = sibling.Keys[0]
	node.Values[index] = sibling.Values[0]

	// Remove the key from the sibling
	sibling.Keys = sibling.Keys[1:]
	sibling.Values = sibling.Values[1:]
}

// mergeChildren merges two children of a node
func (t *BTree) mergeChildren(node *BTreeNode, index int) {
	child := node.Children[index]
	sibling := node.Children[index+1]

	// Move a key from the parent to the child
	child.Keys = append(child.Keys, node.Keys[index])
	child.Values = append(child.Values, node.Values[index])

	// Move all keys from the sibling to the child
	child.Keys = append(child.Keys, sibling.Keys...)
	child.Values = append(child.Values, sibling.Values...)

	// If the child is not a leaf, move all child pointers too
	if !child.IsLeaf {
		child.Children = append(child.Children, sibling.Children...)
	}

	// Remove the key from the parent
	node.Keys = append(node.Keys[:index], node.Keys[index+1:]...)
	node.Values = append(node.Values[:index], node.Values[index+1:]...)

	// Remove the sibling from the parent's children
	node.Children = append(node.Children[:index+1], node.Children[index+2:]...)
}

// compareKeys compares two keys
func compareKeys(a, b interface{}) int {
	switch aVal := a.(type) {
	case int64:
		if bVal, ok := b.(int64); ok {
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0
		}
	case float64:
		if bVal, ok := b.(float64); ok {
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0
		}
	case string:
		if bVal, ok := b.(string); ok {
			return strings.Compare(aVal, bVal)
		}
	case bool:
		if bVal, ok := b.(bool); ok {
			if aVal == bVal {
				return 0
			} else if aVal {
				return 1
			}
			return -1
		}
	}

	// If types don't match or are not comparable, convert to string and compare
	return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
}

// SerializeIndex serializes an index to bytes
func SerializeIndex(index *Index) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write index name
	nameBytes := []byte(index.Name)
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(nameBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(nameBytes); err != nil {
		return nil, err
	}

	// Write table name
	tableNameBytes := []byte(index.TableName)
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(tableNameBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(tableNameBytes); err != nil {
		return nil, err
	}

	// Write column count
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(index.Columns))); err != nil {
		return nil, err
	}

	// Write columns
	for _, col := range index.Columns {
		colBytes := []byte(col)
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(colBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(colBytes); err != nil {
			return nil, err
		}
	}

	// Write isUnique flag
	if err := binary.Write(buf, binary.LittleEndian, index.IsUnique); err != nil {
		return nil, err
	}

	// Serialize the B-tree (this is a simplified version, a real implementation would be more complex)
	treeBytes, err := serializeBTree(index.Tree)
	if err != nil {
		return nil, err
	}

	// Write tree size
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(treeBytes))); err != nil {
		return nil, err
	}

	// Write tree
	if _, err := buf.Write(treeBytes); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DeserializeIndex deserializes an index from bytes
func DeserializeIndex(data []byte) (*Index, error) {
	buf := bytes.NewReader(data)

	// Read index name
	var nameLen uint16
	if err := binary.Read(buf, binary.LittleEndian, &nameLen); err != nil {
		return nil, err
	}
	nameBytes := make([]byte, nameLen)
	if _, err := buf.Read(nameBytes); err != nil {
		return nil, err
	}
	name := string(nameBytes)

	// Read table name
	var tableNameLen uint16
	if err := binary.Read(buf, binary.LittleEndian, &tableNameLen); err != nil {
		return nil, err
	}
	tableNameBytes := make([]byte, tableNameLen)
	if _, err := buf.Read(tableNameBytes); err != nil {
		return nil, err
	}
	tableName := string(tableNameBytes)

	// Read column count
	var colCount uint16
	if err := binary.Read(buf, binary.LittleEndian, &colCount); err != nil {
		return nil, err
	}

	// Read columns
	columns := make([]string, colCount)
	for i := range columns {
		var colLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &colLen); err != nil {
			return nil, err
		}
		colBytes := make([]byte, colLen)
		if _, err := buf.Read(colBytes); err != nil {
			return nil, err
		}
		columns[i] = string(colBytes)
	}

	// Read isUnique flag
	var isUnique bool
	if err := binary.Read(buf, binary.LittleEndian, &isUnique); err != nil {
		return nil, err
	}

	// Read tree size
	var treeSize uint32
	if err := binary.Read(buf, binary.LittleEndian, &treeSize); err != nil {
		return nil, err
	}

	// Read tree
	treeBytes := make([]byte, treeSize)
	if _, err := buf.Read(treeBytes); err != nil {
		return nil, err
	}

	// Deserialize the B-tree
	tree, err := deserializeBTree(treeBytes)
	if err != nil {
		return nil, err
	}

	return &Index{
		Name:      name,
		TableName: tableName,
		Columns:   columns,
		IsUnique:  isUnique,
		Tree:      tree,
	}, nil
}

// serializeBTree serializes a B-tree to bytes
func serializeBTree(tree *BTree) ([]byte, error) {
	// This is a simplified implementation
	// A real implementation would need to handle the tree structure properly
	buf := new(bytes.Buffer)

	// Write degree
	if err := binary.Write(buf, binary.LittleEndian, int32(tree.Degree)); err != nil {
		return nil, err
	}

	// Serialize the root node
	nodeBytes, err := serializeNode(tree.Root)
	if err != nil {
		return nil, err
	}

	// Write node size
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(nodeBytes))); err != nil {
		return nil, err
	}

	// Write node
	if _, err := buf.Write(nodeBytes); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// serializeNode serializes a B-tree node to bytes
func serializeNode(node *BTreeNode) ([]byte, error) {
	// This is a simplified implementation
	// A real implementation would need to handle the node structure properly
	buf := new(bytes.Buffer)

	// Write isLeaf flag
	if err := binary.Write(buf, binary.LittleEndian, node.IsLeaf); err != nil {
		return nil, err
	}

	// Write key count
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(node.Keys))); err != nil {
		return nil, err
	}

	// Write keys and values
	for i, key := range node.Keys {
		// Serialize key based on type
		switch k := key.(type) {
		case int64:
			if err := binary.Write(buf, binary.LittleEndian, byte(0)); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, k); err != nil {
				return nil, err
			}
		case float64:
			if err := binary.Write(buf, binary.LittleEndian, byte(1)); err != nil {
				return nil, err
			}
			if err := binary.Write(buf, binary.LittleEndian, k); err != nil {
				return nil, err
			}
		case string:
			if err := binary.Write(buf, binary.LittleEndian, byte(2)); err != nil {
				return nil, err
			}
			strBytes := []byte(k)
			if err := binary.Write(buf, binary.LittleEndian, uint16(len(strBytes))); err != nil {
				return nil, err
			}
			if _, err := buf.Write(strBytes); err != nil {
				return nil, err
			}
		case bool:
			if err := binary.Write(buf, binary.LittleEndian, byte(3)); err != nil {
				return nil, err
			}
			var boolByte byte
			if k {
				boolByte = 1
			}
			if err := binary.Write(buf, binary.LittleEndian, boolByte); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported key type: %T", key)
		}

		// Write value count
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(node.Values[i]))); err != nil {
			return nil, err
		}

		// Write values
		for _, v := range node.Values[i] {
			if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
				return nil, err
			}
		}
	}

	// Write child count
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(node.Children))); err != nil {
		return nil, err
	}

	// Write children recursively
	for _, child := range node.Children {
		childBytes, err := serializeNode(child)
		if err != nil {
			return nil, err
		}

		// Write child size
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(childBytes))); err != nil {
			return nil, err
		}

		// Write child
		if _, err := buf.Write(childBytes); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// deserializeBTree deserializes a B-tree from bytes
func deserializeBTree(data []byte) (*BTree, error) {
	buf := bytes.NewReader(data)

	// Read degree
	var degree int32
	if err := binary.Read(buf, binary.LittleEndian, &degree); err != nil {
		return nil, err
	}

	// Read node size
	var nodeSize uint32
	if err := binary.Read(buf, binary.LittleEndian, &nodeSize); err != nil {
		return nil, err
	}

	// Read node
	nodeBytes := make([]byte, nodeSize)
	if _, err := buf.Read(nodeBytes); err != nil {
		return nil, err
	}

	// Deserialize the root node
	root, err := deserializeNode(nodeBytes)
	if err != nil {
		return nil, err
	}

	return &BTree{
		Root:   root,
		Degree: int(degree),
	}, nil
}

// deserializeNode deserializes a B-tree node from bytes
func deserializeNode(data []byte) (*BTreeNode, error) {
	buf := bytes.NewReader(data)

	// Read isLeaf flag
	var isLeaf bool
	if err := binary.Read(buf, binary.LittleEndian, &isLeaf); err != nil {
		return nil, err
	}

	// Read key count
	var keyCount uint16
	if err := binary.Read(buf, binary.LittleEndian, &keyCount); err != nil {
		return nil, err
	}

	// Create node
	node := &BTreeNode{
		IsLeaf: isLeaf,
		Keys:   make([]interface{}, keyCount),
		Values: make([][]int64, keyCount),
	}

	// Read keys and values
	for i := uint16(0); i < keyCount; i++ {
		// Read key type
		var keyType byte
		if err := binary.Read(buf, binary.LittleEndian, &keyType); err != nil {
			return nil, err
		}

		// Read key based on type
		switch keyType {
		case 0: // int64
			var k int64
			if err := binary.Read(buf, binary.LittleEndian, &k); err != nil {
				return nil, err
			}
			node.Keys[i] = k
		case 1: // float64
			var k float64
			if err := binary.Read(buf, binary.LittleEndian, &k); err != nil {
				return nil, err
			}
			node.Keys[i] = k
		case 2: // string
			var strLen uint16
			if err := binary.Read(buf, binary.LittleEndian, &strLen); err != nil {
				return nil, err
			}
			strBytes := make([]byte, strLen)
			if _, err := buf.Read(strBytes); err != nil {
				return nil, err
			}
			node.Keys[i] = string(strBytes)
		case 3: // bool
			var boolByte byte
			if err := binary.Read(buf, binary.LittleEndian, &boolByte); err != nil {
				return nil, err
			}
			node.Keys[i] = boolByte == 1
		default:
			return nil, fmt.Errorf("unsupported key type: %d", keyType)
		}

		// Read value count
		var valueCount uint16
		if err := binary.Read(buf, binary.LittleEndian, &valueCount); err != nil {
			return nil, err
		}

		// Read values
		values := make([]int64, valueCount)
		for j := uint16(0); j < valueCount; j++ {
			if err := binary.Read(buf, binary.LittleEndian, &values[j]); err != nil {
				return nil, err
			}
		}
		node.Values[i] = values
	}

	// Read child count
	var childCount uint16
	if err := binary.Read(buf, binary.LittleEndian, &childCount); err != nil {
		return nil, err
	}

	// Read children
	node.Children = make([]*BTreeNode, childCount)
	for i := uint16(0); i < childCount; i++ {
		// Read child size
		var childSize uint32
		if err := binary.Read(buf, binary.LittleEndian, &childSize); err != nil {
			return nil, err
		}

		// Read child
		childBytes := make([]byte, childSize)
		if _, err := buf.Read(childBytes); err != nil {
			return nil, err
		}

		// Deserialize child
		child, err := deserializeNode(childBytes)
		if err != nil {
			return nil, err
		}

		node.Children[i] = child
	}

	return node, nil
}
