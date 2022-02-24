package lsmtree

type LSMTree struct {
	// memTable stays in memory.
	// Contains Key-Value pairs to be flushed to disk.
	memTable *memTable

	diskTableNum       int
	diskTableLastIndex int

	dbDir             string
	sparseKeyDistance int
}

const (
	// memTableThreshold is the number of key-value pairs in the memTable before it is flushed to disk.
	memTableThreshold = 3
)

func NewLSMTree(dbDir string, sparseKeyDistance int) *LSMTree {
	return &LSMTree{
		memTable:          newMemTable(),
		dbDir:             dbDir,
		sparseKeyDistance: sparseKeyDistance,
	}
}

func (t *LSMTree) Put(key, value []byte) error {
	err := t.memTable.put(key, value)
	if err != nil {
		return err
	}

	if t.memTable.keys >= memTableThreshold {
		// Flush memTable to disk.
		if err := t.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func (t *LSMTree) Get(key []byte) ([]byte, bool, error) {
	value, exists := t.memTable.get(key)
	return value, exists, nil
}

func (t *LSMTree) Flush() error {
	newDiskTableNum := t.diskTableNum + 1
	newDiskTableLastIndex := t.diskTableLastIndex + 1

	if err := createDiskTable(t.memTable, t.dbDir, newDiskTableLastIndex, t.sparseKeyDistance); err != nil {
		return err
	}

	t.memTable.clear()
	t.diskTableNum = newDiskTableNum
	t.diskTableLastIndex = newDiskTableLastIndex
	return nil
}
