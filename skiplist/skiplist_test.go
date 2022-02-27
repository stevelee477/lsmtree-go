package skiplist_test

import (
	"bytes"
	"lsmtree/skiplist"
	"testing"
)

func getKeyShouldBe(t *testing.T, list *skiplist.SkipList, key []byte, rightValue []byte) {
	value, ok := list.Get(key)
	if !ok {
		t.Errorf("Get failed: Key: %s, should exists", key)
	}
	if !bytes.Equal(value, rightValue) {
		t.Errorf("Get failed: Key: %s Value: %s, right value should be %s", key, value, rightValue)
	}
	t.Logf("key: %s, value: %s", "key", value)
}

func getKeyShouldNotBe(t *testing.T, list *skiplist.SkipList, key []byte) {
	_, ok := list.Get(key)
	if ok {
		t.Errorf("Get failed: Key: %s, should not exists", key)
	}
}

func getKeyShouldBeDeleted(t *testing.T, list *skiplist.SkipList, key []byte) {
	value, ok := list.Get(key)
	if !ok {
		t.Errorf("Get failed: Key: %s, should exists", key)
	}
	if value != nil {
		t.Errorf("Get failed: Key: %s Value: %s, right value should be <nil>", key, value)
	}
	t.Logf("key: %s, value: <nil>", key)
}

func TestSkipListSimple(t *testing.T) {
	list := skiplist.NewSkipList()
	list.Put([]byte("key"), []byte("value"))
	getKeyShouldBe(t, list, []byte("key"), []byte("value"))
}

func TestSkipListMore(t *testing.T) {
	type Element struct {
		Key   []byte
		Value []byte
	}
	elems := []Element{
		{Key: []byte("4"), Value: []byte("Four")},
		{Key: []byte("2"), Value: []byte("Two")},
		{Key: []byte("1"), Value: []byte("One")},
		{Key: []byte("3"), Value: []byte("Three")},
		{Key: []byte("6"), Value: []byte("Six")},
		{Key: []byte("5"), Value: []byte("Five")},
		{Key: []byte("7"), Value: []byte("Seven")},
	}
	list := skiplist.NewSkipList()
	for elem := range elems {
		list.Put(elems[elem].Key, elems[elem].Value)
	}
	getKeyShouldBe(t, list, []byte("1"), []byte("One"))
	getKeyShouldBe(t, list, []byte("2"), []byte("Two"))
	getKeyShouldNotBe(t, list, []byte("8"))
}

func TestSkipListDelete(t *testing.T) {
	list := skiplist.NewSkipList()
	list.Put([]byte("a"), []byte("a"))
	list.Put([]byte("b"), []byte("b"))
	list.Put([]byte("c"), []byte("c"))
	exists := list.Put([]byte("a"), nil)
	if !exists {
		t.Errorf("Put Failed: Key %s should exists", []byte("a"))
	}
	getKeyShouldBeDeleted(t, list, []byte("a"))
	getKeyShouldBe(t, list, []byte("b"), []byte("b"))
}

func TestSkipListIterator(t *testing.T) {
	type Element struct {
		Key   []byte
		Value []byte
	}
	elems := []Element{
		{Key: []byte("4"), Value: []byte("Four")},
		{Key: []byte("2"), Value: []byte("Two")},
		{Key: []byte("1"), Value: []byte("One")},
		{Key: []byte("3"), Value: []byte("Three")},
		{Key: []byte("6"), Value: []byte("Six")},
		{Key: []byte("5"), Value: []byte("Five")},
		{Key: []byte("7"), Value: []byte("Seven")},
	}
	list := skiplist.NewSkipList()
	for elem := range elems {
		list.Put(elems[elem].Key, elems[elem].Value)
	}
	it := list.Iterator()
	prev := []byte("")
	for it.HasNext() {
		key, value, err := it.Next()
		if err != nil {
			t.Errorf("Iterator failed: %s", err)
		}
		if bytes.Compare(prev, key) > 0 {
			t.Errorf("Iterator failed: prev key %s next key %s", prev, key)
		}
		t.Logf("key: %s, value: %s", key, value)
		prev = key
	}
}

func TestSkipListIteratorDelete(t *testing.T) {
	type Element struct {
		Key   []byte
		Value []byte
	}
	elems := []Element{
		{Key: []byte("4"), Value: []byte("Four")},
		{Key: []byte("2"), Value: []byte("Two")},
		{Key: []byte("1"), Value: []byte("One")},
		{Key: []byte("3"), Value: []byte("Three")},
		{Key: []byte("6"), Value: []byte("Six")},
		{Key: []byte("5"), Value: []byte("Five")},
		{Key: []byte("7"), Value: []byte("Seven")},
	}
	list := skiplist.NewSkipList()
	for elem := range elems {
		list.Put(elems[elem].Key, elems[elem].Value)
	}
	list.Put([]byte("3"), nil)
	it := list.Iterator()
	prev := []byte("")
	for it.HasNext() {
		key, value, err := it.Next()
		if err != nil {
			t.Errorf("Iterator failed: %s", err)
		}
		if bytes.Compare(prev, key) > 0 {
			t.Errorf("Iterator failed: prev key %s next key %s", prev, key)
		}
		t.Logf("key: %s, value: %s", key, value)
		prev = key
	}
}
