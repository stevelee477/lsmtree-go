package binarytree

type Iterator struct {
	cur   *node
	stack []*node
}

func (t *Tree) Iterator() *Iterator {
	cur := t.root

	return &Iterator{cur: cur}
}

func (iter *Iterator) Next() ([]byte, []byte, error) {
	for iter.cur != nil {
		iter.stack = append(iter.stack, iter.cur)
		iter.cur = iter.cur.left
	}
	iter.cur, iter.stack = iter.stack[len(iter.stack)-1], iter.stack[:len(iter.stack)-1]

	node := iter.cur
	iter.cur = iter.cur.right

	return node.key, node.value, nil
}

func (iter *Iterator) HasNext() bool {
	return iter.cur != nil || len(iter.stack) > 0
}
