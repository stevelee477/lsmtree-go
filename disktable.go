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
	// diskTableDataFileName is the name of the file that contains the data.
	diskTableDataFileNamePrefix = "data.dat"
	// diskTableIndexFileName is the name of the file that contains the index of data.
	diskTableIndexFileNamePrefix = "index.dat"
	// diskTableSparseIndexFileName is the name of the file that contains the sparse index of index.
	diskTableSparseIndexFileNamePrefix = "sparseindex.dat"
)

// createDiskTable creates a new diskTable for given memTable.
// prefx is the prefix of the file names.
func createDiskTable(mt *memTable, dir string, index, sparseKeyDistance int) error {
	// prefix of the database file
	prefix := strconv.Itoa(index) + "_"

	writer, err := createDiskTableWriter(dir, prefix, sparseKeyDistance)
	if err != nil {
		return err
	}

	mti := mt.iterator()
	for mti.hasNext() {
		key, value, err := mti.next()
		if err != nil {
			return err
		}

		err = writer.write(key, value)
		if err != nil {
			return err
		}
	}

	if err := writer.sync(); err != nil {
		return err
	}

	if err := writer.close(); err != nil {
		return err
	}

	return nil
}

// searchDiskTable search the key-value in diskTable for giving diskTable index.
func searchDiskTable(dir string, index int, key []byte) ([]byte, bool, error) {
	// prefix of the database file
	prefix := strconv.Itoa(index) + "_"

	sparseIndexPath := path.Join(dir, prefix+diskTableSparseIndexFileNamePrefix)
	sparseIndexFile, err := os.OpenFile(sparseIndexPath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, false, err
	}
	defer sparseIndexFile.Close()

	from, to, exists, err := searchSparseIndex(sparseIndexFile, key)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}

	fmt.Printf("searchSparseIndex: key %s, from %x, to %x, exists %t\n", string(key), from, to, exists)

	indexPath := path.Join(dir, prefix+diskTableIndexFileNamePrefix)
	indexFile, err := os.OpenFile(indexPath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, false, err
	}
	defer indexFile.Close()
	offset, exists, err := searchIndexFile(indexFile, key, from, to)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}

	fmt.Printf("searchIndexFile: key %s, exists %t, offset %x\n", string(key), exists, offset)

	dataPath := path.Join(dir, prefix+diskTableDataFileNamePrefix)
	dataFile, err := os.OpenFile(dataPath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, false, err
	}
	defer dataFile.Close()
	value, err := searchDataFile(dataFile, key, offset)
	if err != nil {
		return nil, false, err
	}

	fmt.Printf("searchDataFile: key %s, value %s\n", string(key), string(value))

	return value, true, nil
}

// searchSparseIndex search the key in sparseIndexFile.
// Return false if key not found.
// [from, to) interval is the range of key in indexFile.
func searchSparseIndex(r io.Reader, key []byte) (int, int, bool, error) {
	from := -1
	for {
		sparseKey, value, err := decode(r)
		if err != nil && err != io.EOF {
			return 0, 0, false, err
		}
		if err == io.EOF {
			return from, 0, true, nil
		}

		if bytes.Compare(key, sparseKey) < 0 {
			if from == -1 {
				// key less than first sparseKey
				return 0, 0, false, nil
			}
			return from, int(decodeInt(value)), true, nil
		} else if bytes.Equal(key, sparseKey) {
			return int(decodeInt(value)), int(decodeInt(value)), true, nil
		} else {
			from = int(decodeInt(value))
		}
	}
}

// searchIndexFile search the key in indexFile.
// Return false if key not found.
// Return offset in dataFile.
func searchIndexFile(r io.ReadSeeker, key []byte, from, to int) (int, bool, error) {
	if _, err := r.Seek(int64(from), io.SeekStart); err != nil {
		return 0, false, err
	}

	for {
		indexKey, value, err := decode(r)
		if err != nil && err != io.EOF {
			return 0, false, err
		}
		if err == io.EOF {
			// already reach the end of the file
			return 0, false, nil
		}
		offset := decodeInt(value)

		if bytes.Equal(key, indexKey) {
			return offset, true, nil
		}

		if to > from {
			current, err := r.Seek(0, io.SeekCurrent)
			if err != nil {
				return 0, false, err
			}

			if current > int64(to) {
				// already reach the uppper bound
				return 0, false, nil
			}
		}
	}
}

// searchDataFile search the key in dataFile at giving offset.
// Return false if key not found.
// Return value if key found.
func searchDataFile(r io.ReadSeeker, key []byte, offset int) ([]byte, error) {
	if _, err := r.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	dataKey, value, err := decode(r)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(key, dataKey) {
		return nil, nil
	}

	return value, nil
}

type diskTableWriter struct {
	dataFile        *os.File
	indexFile       *os.File
	sparseIndexFile *os.File

	sparseKeyDistance int

	// Position of the last byte written to the data file.
	keyNum, dataPos, indexPos int
}

// createDiskTableWriter create write for writing diskTable
func createDiskTableWriter(dir, prefix string, sparseKeyDistance int) (*diskTableWriter, error) {
	dataPath := path.Join(dir, prefix+diskTableDataFileNamePrefix)
	dataFile, err := os.OpenFile(dataPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("createDiskTableWriter: failed to open data file %s: %s", dataPath, err)
	}

	indexPath := path.Join(dir, prefix+diskTableIndexFileNamePrefix)
	indexFile, err := os.OpenFile(indexPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("createDiskTableWriter: failed to open index file %s: %s", indexPath, err)
	}

	sparseIndexPath := path.Join(dir, prefix+diskTableSparseIndexFileNamePrefix)
	sparseIndexFile, err := os.OpenFile(sparseIndexPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("createDiskTableWriter: failed to open sparse index file %s: %s", sparseIndexPath, err)
	}

	writer := &diskTableWriter{
		dataFile:        dataFile,
		indexFile:       indexFile,
		sparseIndexFile: sparseIndexFile,

		sparseKeyDistance: sparseKeyDistance,

		keyNum:   0,
		dataPos:  0,
		indexPos: 0,
	}
	return writer, nil
}

// write the key-value to diskTable using diskTableWriter.
func (writer *diskTableWriter) write(key, value []byte) error {
	dataBytes, err := encode(writer.dataFile, key, value)
	if err != nil {
		return err
	}

	indexBytes, err := encode(writer.indexFile, key, encodeInt(writer.dataPos))
	if err != nil {
		return err
	}

	if writer.keyNum%writer.sparseKeyDistance == 0 {
		_, err := encode(writer.sparseIndexFile, key, encodeInt(writer.indexPos))
		if err != nil {
			return err
		}
	}

	writer.keyNum++
	writer.dataPos += dataBytes
	writer.indexPos += indexBytes
	return nil
}

// sync diskTableWriter to disk
func (writer *diskTableWriter) sync() error {
	if err := writer.dataFile.Sync(); err != nil {
		return err
	}

	if err := writer.indexFile.Sync(); err != nil {
		return err
	}

	if err := writer.sparseIndexFile.Sync(); err != nil {
		return err
	}
	return nil
}

// close closes diskTableWriter all three files
func (writer *diskTableWriter) close() error {
	if err := writer.dataFile.Close(); err != nil {
		return err
	}

	if err := writer.indexFile.Close(); err != nil {
		return err
	}

	if err := writer.sparseIndexFile.Close(); err != nil {
		return err
	}
	return nil
}
