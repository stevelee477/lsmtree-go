package lsmtree_test

import (
	"lsmtree"
	"os"
	"testing"
)

func TestLSMTreePut(t *testing.T) {
	tree := lsmtree.NewLSMTree("/tmp/lsmtree", 1)

	key := []byte("key")
	value := []byte("value")

	tree.Put(key, value)

	valueDecoded, exists, err := tree.Get(key)

	if err != nil {
		t.Errorf("Get failed: %s", err)
	}
	if !exists {
		t.Errorf("Get failed: key not found")
	}
	t.Logf("key: %s, value: %s", key, valueDecoded)

}

func TestLSMTreeDiskTable(t *testing.T) {
	type Element struct {
		Key   []byte
		Value []byte
	}
	elems := []Element{
		{Key: []byte("1"), Value: []byte("One")},
		{Key: []byte("2"), Value: []byte("Two")},
		{Key: []byte("3"), Value: []byte("Three")},
		{Key: []byte("4"), Value: []byte("Four")},
		{Key: []byte("5"), Value: []byte("Five")},
		{Key: []byte("6"), Value: []byte("Six")},
		{Key: []byte("7"), Value: []byte("Seven")},
	}
	// rand.Seed(time.Now().UnixNano())
	// rand.Shuffle(len(elems), func(i, j int) { elems[i], elems[j] = elems[j], elems[i] })

	dir := os.TempDir()
	tree := lsmtree.NewLSMTree(dir, 2)
	t.Logf("Tmp dir: %s", dir)
	for _, elem := range elems {
		tree.Put(elem.Key, elem.Value)
	}

	value, _, _ := tree.Get([]byte("7"))
	t.Logf("Value for key 7: %s", value)
}

func TestLSMTreeDiskTableSparse(t *testing.T) {
	type Element struct {
		Key   []byte
		Value []byte
	}
	elems := []Element{
		{Key: []byte("1"), Value: []byte("One")},
		{Key: []byte("2"), Value: []byte("Two")},
		{Key: []byte("3"), Value: []byte("Three")},
		{Key: []byte("4"), Value: []byte("Four")},
		{Key: []byte("5"), Value: []byte("Five")},
		{Key: []byte("6"), Value: []byte("Six")},
		{Key: []byte("7"), Value: []byte("Seven")},
		{Key: []byte("8"), Value: []byte("Eight")},
		{Key: []byte("9"), Value: []byte("Nine")},
		{Key: []byte("10"), Value: []byte("Ten")},
		{Key: []byte("11"), Value: []byte("Eleven")},
		{Key: []byte("12"), Value: []byte("Twelve")},
	}

	dir := os.TempDir()
	tree := lsmtree.NewLSMTree(dir, 2)
	t.Logf("Tmp dir: %s", dir)
	for _, elem := range elems {
		tree.Put(elem.Key, elem.Value)
	}

	value, _, err := tree.Get([]byte("2"))
	if err != nil {
		t.Errorf("Get failed: %s", err)
	}
	t.Logf("Value for key 2: %s", value)
	// searchSparseIndex: key 2, from 0, to 0, exists false
	// searchSparseIndex: key 2, from 0, to 50, exists true

	tree.Get([]byte("3"))
	// searchSparseIndex: key 3, from 0, to 0, exists false
	// searchSparseIndex: key 3, from 50, to 50, exists true

	tree.Get([]byte("1"))
	//searchSparseIndex: key 1, from 0, to 0, exists false
	//searchSparseIndex: key 1, from 0, to 0, exists true

	tree.Get([]byte("4"))
	// searchSparseIndex: key 4, from 0, to 0, exists true
	// searchSparseIndex: key 4, from 50, to 0, exists true

	tree.Get([]byte("12"))
}
