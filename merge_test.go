package lsmtree

import (
	"os"
	"path"
	"strconv"
	"testing"
)

func TestDataFileIterator(t *testing.T) {
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
	}

	dir := os.TempDir()
	tree := NewLSMTree(dir, 2)
	t.Logf("Tmp dir: %s", dir)
	for _, elem := range elems {
		tree.Put(elem.Key, elem.Value)
	}

	prefix := strconv.Itoa(0) + "_"

	dfi, err := newDataFileIterator(path.Join(dir, prefix+diskTableDataFileNamePrefix))
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for dfi.hasNext() {
		key, value, err := dfi.next()
		if err != nil {
			t.Fatal(err)
		}
		count++
		t.Logf("Key: %s, Value: %s", key, value)
	}
	if count != memTableThreshold {
		t.Fatal("dataFileIterator Expected", memTableThreshold, "entries, got", count)
	}
}

func TestMergeDiskTable(t *testing.T) {
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
	}

	dir := os.TempDir()
	tree := NewLSMTree(dir, 2)
	t.Logf("Tmp dir: %s", dir)
	for _, elem := range elems {
		tree.Put(elem.Key, elem.Value)
	}

	err := mergeDiskTables(dir, 0, 1, 2)

	if err != nil {
		t.Fatal(err)
	}

	prefix := strconv.Itoa(1) + "_"

	dfi, err := newDataFileIterator(path.Join(dir, prefix+diskTableDataFileNamePrefix))
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for dfi.hasNext() {
		key, value, err := dfi.next()
		if err != nil {
			t.Fatal(err)
		}
		count++
		t.Logf("Key: %s, Value: %s", key, value)
	}
	if count != 2*memTableThreshold {
		t.Fatal("dataFileIterator Expected", memTableThreshold, "entries, got", count)
	}
}

func TestPutMerge(t *testing.T) {
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
	tree := NewLSMTree(dir, 2)
	t.Logf("Tmp dir: %s", dir)
	for _, elem := range elems {
		tree.Put(elem.Key, elem.Value)
	}

	// err := mergeDiskTables(dir, 0, 1, 2)

	// if err != nil {
	// 	t.Fatal(err)
	// }

	// prefix := strconv.Itoa(1) + "_"

	// dfi, err := newDataFileIterator(path.Join(dir, prefix+diskTableDataFileNamePrefix))
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// count := 0
	// for dfi.hasNext() {
	// 	key, value, err := dfi.next()
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}
	// 	count++
	// 	t.Logf("Key: %s, Value: %s", key, value)
	// }
	// if count != 2*memTableThreshold {
	// 	t.Fatal("dataFileIterator Expected", memTableThreshold, "entries, got", count)
	// }
}
