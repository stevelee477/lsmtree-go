package binarytree_test

import (
	"bytes"
	"lsmtree/binarytree"
	"math/rand"
	"testing"
	"time"
)

type Element struct {
	Key   []byte
	Value []byte
}

func TestBinaryTreeInsert(t *testing.T) {
	elems := []Element{
		{Key: []byte("4"), Value: []byte("Four")},
		{Key: []byte("2"), Value: []byte("Two")},
		{Key: []byte("1"), Value: []byte("One")},
		{Key: []byte("3"), Value: []byte("Three")},
		{Key: []byte("6"), Value: []byte("Six")},
		{Key: []byte("5"), Value: []byte("Five")},
		{Key: []byte("7"), Value: []byte("Seven")},
	}
	tree := binarytree.NewTree()
	for _, elem := range elems {
		tree.Put(elem.Key, elem.Value)
	}
	value, err := tree.Get([]byte("1"))
	if err != nil {
		t.Errorf("Error getting value for key 1: %v %v", value, err)
	}
	t.Logf("Value for key 1: %v", string(value))
	value, err = tree.Get([]byte("8"))
	if err == nil {
		t.Errorf("Error getting value for key 8: %v %v", value, err)
	}
	t.Logf("Value for key 8: %v", nil)
}

func TestBinaryTreeIterator(t *testing.T) {
	elems := []Element{
		{Key: []byte("4"), Value: []byte("Four")},
		{Key: []byte("2"), Value: []byte("Two")},
		{Key: []byte("1"), Value: []byte("One")},
		{Key: []byte("3"), Value: []byte("Three")},
		{Key: []byte("6"), Value: []byte("Six")},
		{Key: []byte("5"), Value: []byte("Five")},
		{Key: []byte("7"), Value: []byte("Seven")},
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(elems), func(i, j int) { elems[i], elems[j] = elems[j], elems[i] })
	tree := binarytree.NewTree()
	for _, elem := range elems {
		tree.Put(elem.Key, elem.Value)
	}
	iter := tree.Iterator()
	prev := []byte("")
	for iter.HasNext() {
		key, _ := iter.Next()
		t.Logf("prev %v key %v", string(prev), string(key))
		if bytes.Compare(key, prev) < 0 {
			t.Errorf("Iterator returned keys in wrong order prev %v key %v", string(prev), string(key))
		}
		prev = key
	}
}
