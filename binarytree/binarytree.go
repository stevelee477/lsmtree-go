package binarytree

import (
	"bytes"
	"fmt"
)

type Tree struct {
	root *node
}

type node struct {
	key   []byte
	value []byte
	left  *node
	right *node
}

func NewTree() *Tree {
	return &Tree{}
}

func insert(root **node, key, value []byte) (*node, bool) {
	exists := false
	if *root == nil {
		*root = &node{key: key, value: value}
		return *root, false
	}
	if bytes.Compare(key, (*root).key) < 0 {
		(*root).left, exists = insert(&(*root).left, key, value)
	} else if bytes.Compare(key, (*root).key) > 0 {
		(*root).right, exists = insert(&(*root).right, key, value)
	} else {
		(*root).value = value
		exists = true
	}
	return *root, exists
}

func search(root **node, key []byte) ([]byte, bool) {
	if *root == nil {
		return nil, false
	}
	if bytes.Compare(key, (*root).key) < 0 {
		return search(&(*root).left, key)
	} else if bytes.Compare(key, (*root).key) > 0 {
		return search(&(*root).right, key)
	} else {
		return (*root).value, true
	}
}

func (t *Tree) Put(key, value []byte) bool {
	_, exists := insert(&t.root, key, value)
	return exists
}

func (t *Tree) Get(key []byte) ([]byte, error) {
	value, exists := search(&t.root, key)
	if exists {
		return value, nil
	}
	return nil, fmt.Errorf("key not found")
}
