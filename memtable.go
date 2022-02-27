package lsmtree

import (
	"lsmtree/skiplist"
)

// MemTable. In memory structure for storing key-value pairs. Using bst to store for now.
type memTable struct {
	// tree *binarytree.Tree
	list *skiplist.SkipList
	keys int
}

// newMemTable creates a new memTable.
func newMemTable() *memTable {
	return &memTable{list: skiplist.NewSkipList()}
}

// Put inserts a key-value pair into the memTable.
func (mt *memTable) put(key, value []byte) error {
	exists := mt.list.Put(key, value)
	if !exists {
		mt.keys++
	}

	return nil
}

// Get returns the value for the given key.
// Returns <nil> for deleted keys.
func (mt *memTable) get(key []byte) ([]byte, bool) {
	return mt.list.Get(key)
}

// clear clears the memTable.
func (mt *memTable) clear() {
	mt.list.Clear()
	mt.keys = 0
}

// memTableIterator is an iterator for the memTable.
type memTableIterator struct {
	//TODO: Change with a interface
	it *skiplist.Iterator
}

func (mt *memTable) iterator() *memTableIterator {
	return &memTableIterator{it: mt.list.Iterator()}
}

// next returns the next key-value pair in the memTable.
func (mti *memTableIterator) next() ([]byte, []byte, error) {
	return mti.it.Next()
}

// hasNext returns true if there are more key-value pairs in the memTable.
func (mti *memTableIterator) hasNext() bool {
	return mti.it.HasNext()
}
