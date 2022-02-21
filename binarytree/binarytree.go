package binarytree

import (
	"bytes"
	"fmt"
)

type TreeNode struct {
	Key   []byte
	Value []byte
	Left  *TreeNode
	Right *TreeNode
}

func NewTree() *TreeNode {
	return &TreeNode{}
}

func insert(node *TreeNode, key, value []byte) (*TreeNode, bool) {
	exists := false
	if node == nil {
		return &TreeNode{Key: key, Value: value}, false
	}
	if bytes.Compare(key, node.Key) < 0 {
		node.Left, exists = insert(node.Left, key, value)
	} else if bytes.Compare(key, node.Key) > 0 {
		node.Right, exists = insert(node.Right, key, value)
	} else {
		node.Value = value
		exists = true
	}
	return node, exists
}

func search(node *TreeNode, key []byte) ([]byte, bool) {
	if node == nil {
		return nil, false
	}
	if bytes.Compare(key, node.Key) < 0 {
		return search(node.Left, key)
	} else if bytes.Compare(key, node.Key) > 0 {
		return search(node.Right, key)
	} else {
		return node.Value, true
	}
}

func Put(node *TreeNode, key, value []byte) bool {
	_, exists := insert(node, key, value)
	return exists
}

func Get(node *TreeNode, key []byte) ([]byte, error) {
	value, exists := search(node, key)
	if exists {
		return value, nil
	}
	return nil, fmt.Errorf("key not found")
}
