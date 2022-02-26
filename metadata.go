package lsmtree

import (
	"os"
	"path"
)

const (
	// metaDataFileName is the name of metadata file
	metaDataFileName = "metadata.dat"
)

// readMetaData reads metadata from disk contains diskTableNum and diskTableLastIndex
func readMetaData(dbDir string) (int, int, error) {
	metaDataFilePath := path.Join(dbDir, metaDataFileName)
	f, err := os.Open(metaDataFilePath)
	if err != nil && !os.IsNotExist(err) {
		return 0, 0, err
	}
	if os.IsNotExist(err) {
		return 0, -1, nil
	}
	defer f.Close()

	var diskTableNum, diskTableLastIndex int
	var diskTableNumEncoded, diskTableLastIndexEncoded [8]byte
	if _, err := f.Read(diskTableNumEncoded[:]); err != nil {
		return 0, -1, err
	}
	diskTableNum = decodeInt(diskTableNumEncoded[:])

	if _, err := f.Read(diskTableLastIndexEncoded[:]); err != nil {
		return 0, -1, err
	}
	diskTableLastIndex = decodeInt(diskTableLastIndexEncoded[:])

	return diskTableNum, diskTableLastIndex, nil
}

// writeMetaData writes metadata to disk
func writeMetaData(dbDir string, diskTableNum, diskTableLastIndex int) error {
	metaDataFilePath := path.Join(dbDir, metaDataFileName)
	f, err := os.OpenFile(metaDataFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	diskTableNumEncoded := encodeInt(diskTableNum)
	diskTableLastIndexEncoded := encodeInt(diskTableLastIndex)

	if _, err := f.Write(diskTableNumEncoded[:]); err != nil {
		return err
	}

	if _, err := f.Write(diskTableLastIndexEncoded[:]); err != nil {
		return err
	}

	return nil
}
