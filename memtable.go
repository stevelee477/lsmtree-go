package lsmtree

import "lsmtree/binarytree"

// MemTable. In memory structure for storing key-value pairs. Using bst to store for now.
type memTable struct {
	tree *binarytree.Tree
	keys int
}

// newMemTable creates a new memTable.
func newMemTable() *memTable {
	return &memTable{tree: binarytree.NewTree()}
}

// Put inserts a key-value pair into the memTable.
func (mt *memTable) put(key, value []byte) error {
	exists := mt.tree.Put(key, value)
	if !exists {
		mt.keys++
	}

	return nil
}

// Get returns the value for the given key.
// Returns <nil> for deleted keys.
func (mt *memTable) get(key []byte) ([]byte, bool) {
	return mt.tree.Get(key)
}

// clear clears the memTable.
func (mt *memTable) clear() {
	mt.tree.Clear()
	mt.keys = 0
}

// memTableIterator is an iterator for the memTable.
type memTableIterator struct {
	//TODO: Change with a interface
	it *binarytree.Iterator
}

func (mt *memTable) iterator() *memTableIterator {
	return &memTableIterator{it: mt.tree.Iterator()}
}

// next returns the next key-value pair in the memTable.
func (mti *memTableIterator) next() ([]byte, []byte, error) {
	return mti.it.Next()
}

// hasNext returns true if there are more key-value pairs in the memTable.
func (mti *memTableIterator) hasNext() bool {
	return mti.it.HasNext()
}
