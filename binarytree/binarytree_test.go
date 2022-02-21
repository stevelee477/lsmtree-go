package binarytree

import "testing"
import "fmt"

type Element struct {
	Key   []byte
	Value []byte
}

func TestBinaryTree(t *testing.T) {
	elems := []Element{
		{Key: []byte("1"), Value: []byte("One")},
		{Key: []byte("2"), Value: []byte("Two")},
		{Key: []byte("3"), Value: []byte("Three")},
		{Key: []byte("4"), Value: []byte("Four")},
		{Key: []byte("5"), Value: []byte("Five")},
		{Key: []byte("6"), Value: []byte("Six")},
		{Key: []byte("7"), Value: []byte("Seven")},
	}
	tree := NewTree()
	for _, elem := range elems {
		Put(tree, elem.Key, elem.Value)
	}
	if value, err := Get(tree, []byte("1")); err == nil {
		fmt.Printf("%s\n", string(value))
	}
	if value, err := Get(tree, []byte("8")); err != nil {
		fmt.Printf("%s\n", value)
	}
}
