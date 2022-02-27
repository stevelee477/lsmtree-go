package skiplist

import (
	"bytes"
	"math/rand"
	"time"
)

// SkipList is a skip list implementation.

const (
	// MaxLevel is the maximum level of the skip list.
	MaxLevel = 16

	// P is the probability of the node to be inserted to the next level.
	P = 0.5
)

type node struct {
	key   []byte
	value []byte
	next  []*node
}

type SkipList struct {
	head   *node
	length int
	level  int
}

func NewSkipList() *SkipList {
	// init random seed
	rand.Seed(time.Now().UnixNano())

	head := &node{
		next: make([]*node, MaxLevel),
		key:  nil,
	}
	return &SkipList{
		head:   head,
		length: 0,
		level:  1,
	}
}

func (sk *SkipList) Get(key []byte) ([]byte, bool) {
	node := sk.head
	for i := sk.level - 1; i >= 0; i-- {
		for node.next[i] != nil && bytes.Compare(node.next[i].key, key) < 0 {
			node = node.next[i]
		}
	}
	node = node.next[0]
	if node == nil || !bytes.Equal(node.key, key) {
		return nil, false
	}
	return node.value, true
}

func (sk *SkipList) getPrevNodes(key []byte) []*node {
	prev := sk.head
	prevNodes := make([]*node, MaxLevel)
	for i := sk.level - 1; i >= 0; i-- {
		for prev.next[i] != nil && bytes.Compare(prev.next[i].key, key) < 0 {
			prev = prev.next[i]
		}
		prevNodes[i] = prev
	}
	return prevNodes
}

func (sk *SkipList) randLevel() int {
	level := 1
	for level < MaxLevel && rand.Float64() < P {
		level++
	}
	return level
}

func (sk *SkipList) Put(key []byte, value []byte) bool {
	prevNodes := sk.getPrevNodes(key)
	if prevNodes[0].next[0] != nil && bytes.Equal(prevNodes[0].next[0].key, key) {
		prevNodes[0].next[0].value = value
		return true
	}
	node := &node{
		key:   key,
		value: value,
		next:  make([]*node, MaxLevel),
	}
	level := sk.randLevel()
	if level > sk.level {
		level = sk.level + 1
		prevNodes[sk.level] = sk.head
		sk.level = level
	}

	for i := 0; i < level; i++ {
		node.next[i] = prevNodes[i].next[i]
		prevNodes[i].next[i] = node
	}

	sk.length++
	return false
}

func (sk *SkipList) Clear() {
	// init random seed
	rand.Seed(time.Now().UnixNano())

	sk.head = &node{
		next: make([]*node, MaxLevel),
		key:  nil,
	}
	sk.length = 0
	sk.level = 1
}
