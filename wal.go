package lsmtree

import (
	"io"
	"os"
)

func loadWAL(wal *os.File) (*memTable, error) {
	mt := newMemTable()
	for {
		key, value, err := decode(wal)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if err == io.EOF {
			return mt, nil
		}
		err = mt.put(key, value)
		if err != nil {
			return nil, err
		}
	}
}

func appendWAL(wal *os.File, key, value []byte) error {
	if _, err := encode(wal, key, value); err != nil {
		return nil
	}

	if err := wal.Sync(); err != nil {
		return err
	}

	return nil
}
