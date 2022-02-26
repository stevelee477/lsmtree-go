package lsmtree

import (
	"os"
	"path"
)

type LSMTree struct {
	// memTable stays in memory.
	// Contains Key-Value pairs to be flushed to disk.
	memTable *memTable

	diskTableNum       int
	diskTableLastIndex int

	dbDir             string
	sparseKeyDistance int

	wal *os.File
}

const (
	// memTableThreshold is the number of key-value pairs in the memTable before it is flushed to disk.
	memTableThreshold = 4

	// mergeThreshold is the number of disk tables to merge.
	mergeThreshold = 2

	// walFileName is the name of WAL file
	walFileName = "wal.dat"
)

func NewLSMTree(dbDir string, sparseKeyDistance int) *LSMTree {
	diskTableNum, diskTableLastIndex, err := readMetaData(dbDir)
	if err != nil {
		panic(err)
	}

	walPath := path.Join(dbDir, walFileName)
	wal, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}

	mt, err := loadWAL(wal)
	if err != nil {
		panic(err)
	}

	return &LSMTree{
		memTable:           mt,
		diskTableNum:       diskTableNum,
		diskTableLastIndex: diskTableLastIndex,
		dbDir:              dbDir,
		sparseKeyDistance:  sparseKeyDistance,
		wal:                wal,
	}
}

func (t *LSMTree) Put(key, value []byte) error {

	if err := appendWAL(t.wal, key, value); err != nil {
		return err
	}

	if err := t.memTable.put(key, value); err != nil {
		return err
	}

	if t.memTable.keys >= memTableThreshold {
		// Flush memTable to disk.
		if err := t.Flush(); err != nil {
			return err
		}
	}

	if t.diskTableNum > mergeThreshold {
		// merge oldest and oldest+1 disk tables.
		oldest := t.diskTableLastIndex - t.diskTableNum + 1
		if err := mergeDiskTables(t.dbDir, oldest, oldest+1, t.sparseKeyDistance); err != nil {
			return err
		}

		newDiskTableNum := t.diskTableNum - 1
		if err := writeMetaData(t.dbDir, newDiskTableNum, t.diskTableLastIndex); err != nil {
			return err
		}

		t.diskTableNum--
	}

	return nil
}

func (t *LSMTree) Get(key []byte) ([]byte, bool, error) {
	value, exists := t.memTable.get(key)
	if exists {
		return value, exists, nil
	}

	diskTableFirstIndex := t.diskTableLastIndex - t.diskTableNum + 1
	for i := t.diskTableLastIndex; i >= diskTableFirstIndex; i-- {
		value, exists, err := searchDiskTable(t.dbDir, i, key)
		if err != nil {
			return nil, false, err
		}
		if exists {
			return value, exists, nil
		}
	}

	return nil, false, nil
}

func (t *LSMTree) Flush() error {
	newDiskTableNum := t.diskTableNum + 1
	newDiskTableLastIndex := t.diskTableLastIndex + 1

	if err := createDiskTable(t.memTable, t.dbDir, newDiskTableLastIndex, t.sparseKeyDistance); err != nil {
		return err
	}

	if err := writeMetaData(t.dbDir, newDiskTableNum, newDiskTableLastIndex); err != nil {
		return err
	}

	t.memTable.clear()
	t.diskTableNum = newDiskTableNum
	t.diskTableLastIndex = newDiskTableLastIndex
	return nil
}
