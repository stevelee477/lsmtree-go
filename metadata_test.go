package lsmtree

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestWriteReadMetaData(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "lsmtree")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(dbDir)
	writeMetaData(dbDir, 0, -1)
	a, b, err := readMetaData(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d %d", a, b)
	if a != 0 || b != -1 {
		t.Fatal("readMetaData error")
	}
	writeMetaData(dbDir, 2, 3)
	a, b, err = readMetaData(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d %d", a, b)
	if a != 2 || b != 3 {
		t.Fatal("readMetaData error")
	}
}

func TestReadEmptyMetaData(t *testing.T) {
	dbDir, err := ioutil.TempDir(os.TempDir(), "lsmtree")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(dbDir)
	a, b, err := readMetaData(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d %d", a, b)
	if a != 0 || b != -1 {
		t.Fatal("readMetaData error")
	}
}
