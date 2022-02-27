package skiplist

type Iterator struct {
	cur *node
}

func (sk *SkipList) Iterator() *Iterator {
	return &Iterator{cur: sk.head.next[0]}
}

func (it *Iterator) HasNext() bool {
	return it.cur != nil
}

func (it *Iterator) Next() ([]byte, []byte, error) {
	node := it.cur
	it.cur = it.cur.next[0]

	return node.key, node.value, nil
}
