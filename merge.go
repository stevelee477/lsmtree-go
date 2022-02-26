package lsmtree

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
)

const (
	// mergePrefix is prefix of merging file
	mergePrefix = "merge_"
)

func mergeDiskTables(dbDir string, db1, db2, sparseKeyDistance int) error {
	fmt.Printf("mergeDiskTables: db1=%d, db2=%d\n", db1, db2)
	prefix1 := strconv.Itoa(db1) + "_"
	path1 := path.Join(dbDir, prefix1+diskTableDataFileNamePrefix)
	dfi1, err := newDataFileIterator(path1)
	if err != nil {
		return err
	}
	defer dfi1.close()

	prefix2 := strconv.Itoa(db2) + "_"
	path2 := path.Join(dbDir, prefix2+diskTableDataFileNamePrefix)
	dfi2, err := newDataFileIterator(path2)
	if err != nil {
		return err
	}
	defer dfi2.close()

	w, err := newDiskTableWriter(dbDir, mergePrefix+prefix2, sparseKeyDistance)
	if err != nil {
		return nil
	}

	// merge data
	if err := merge(dfi1, dfi2, w); err != nil {
		return nil
	}

	if err := dfi1.close(); err != nil {
		return err
	}
	if err := dfi2.close(); err != nil {
		return err
	}

	// delete db1 and db2
	if err := deleteDiskTables(dbDir, prefix1); err != nil {
		return err
	}
	if err := deleteDiskTables(dbDir, prefix2); err != nil {
		return err
	}

	// rename merge file to db2
	if err := renameDiskTables(dbDir, mergePrefix+prefix2, prefix2); err != nil {
		return err
	}

	return nil
}

// merge two dataFileIterator to the writer
func merge(dfi1, dfi2 *dataFileIterator, w *diskTableWriter) error {
	var key1, key2, value1, value2 []byte
	var err error
	for {
		if key1 == nil && dfi1.hasNext() {
			key1, value1, err = dfi1.next()
			if err != nil {
				return err
			}
		}

		if key2 == nil && dfi2.hasNext() {
			key2, value2, err = dfi2.next()
			if err != nil {
				return err
			}
		}

		if key1 == nil && key2 == nil && !dfi1.hasNext() && !dfi2.hasNext() {
			return nil
		}

		if key1 != nil && key2 != nil {
			if bytes.Compare(key1, key2) < 0 {
				// key1 < key2, write key1, value1
				err := w.write(key1, value1)
				if err != nil {
					return err
				}
				key1, value1 = nil, nil
			} else if bytes.Compare(key1, key2) > 0 {
				// key1 > key2, write key2, value2
				err := w.write(key2, value2)
				if err != nil {
					return err
				}
				key2, value2 = nil, nil
			} else {
				// key1 == key2, write key2, value2
				err := w.write(key2, value2)
				if err != nil {
					return err
				}
				key1, value1 = nil, nil
				key2, value2 = nil, nil
			}
		} else if key1 != nil {
			if err := w.write(key1, value1); err != nil {
				return err
			}
			key1, value1 = nil, nil
		} else if key2 != nil {
			if err := w.write(key2, value2); err != nil {
				return err
			}
			key2, value2 = nil, nil
		}
	}
}

// dataFileIterator is an iterator for diskTable data file.
type dataFileIterator struct {
	file  *os.File
	key   []byte
	value []byte
	eof   bool
}

// newDataFileIterator creates a new dataFileIterator.
func newDataFileIterator(path string) (*dataFileIterator, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	key, value, err := decode(file)
	if err != nil && err != io.EOF {
		return nil, err
	}

	eof := err == io.EOF

	return &dataFileIterator{
		file:  file,
		key:   key,
		value: value,
		eof:   eof,
	}, nil
}

// next returns next Key-Value pair of dataFileIterator
func (dfi *dataFileIterator) next() ([]byte, []byte, error) {
	if dfi.eof {
		return dfi.key, dfi.value, nil
	}

	key := dfi.key
	value := dfi.value

	nextKey, nextValue, err := decode(dfi.file)
	if err != nil && err != io.EOF {
		return nil, nil, err
	}

	dfi.eof = err == io.EOF
	dfi.key = nextKey
	dfi.value = nextValue

	return key, value, nil
}

// hasNext returns true if data has next
func (dti *dataFileIterator) hasNext() bool {
	return !dti.eof
}

// close closes dataFileIterator
func (dti *dataFileIterator) close() error {
	return dti.file.Close()
}
