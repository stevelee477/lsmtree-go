package lsmtree

import (
	"fmt"
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
