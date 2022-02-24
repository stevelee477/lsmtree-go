package lsmtree

import (
	"bytes"
	"testing"
)

func TestEncodePut(t *testing.T) {
	buffer := bytes.Buffer{}
	key := []byte("key")
	value := []byte("value")

	n, err := encode(&buffer, key, value)
	if err != nil {
		t.Errorf("encode failed: %s", err)
	}
	if n != len(key)+len(value)+2*8 {
		t.Errorf("encode length not correct: %s", err)
	}

	keyDecoded, valueDecoded, err := decode(&buffer)
	if err != nil {
		t.Errorf("decode failed: %s", err)
	}
	if !bytes.Equal(key, keyDecoded) {
		t.Errorf("decode key not correct: %s", err)
	}
	if !bytes.Equal(value, valueDecoded) {
		t.Errorf("decode value not correct: %s", err)
	}
	t.Logf("key: %s, value: %s", keyDecoded, valueDecoded)
}

func TestEncodeDelete(t *testing.T) {
	buffer := bytes.Buffer{}
	key := []byte("key")

	n, err := encode(&buffer, key, nil)
	if err != nil {
		t.Errorf("encode failed: %s", err)
	}
	if n != len(key)+2*8 {
		t.Errorf("encode length not correct: %s", err)
	}

	keyDecoded, valueDecoded, err := decode(&buffer)
	if err != nil {
		t.Errorf("decode failed: %s", err)
	}
	if !bytes.Equal(key, keyDecoded) {
		t.Errorf("decode key not correct: %s", err)
	}
	if valueDecoded != nil {
		t.Errorf("decode value not correct: %s", err)
	}
	t.Logf("key: %s, value: %s", keyDecoded, valueDecoded)
}

func TestEncodeInt(t *testing.T) {
	testInt := 233333
	if decodeInt(encodeInt(testInt)) != testInt {
		t.Errorf("decodeInt failed: should be %v get %v", testInt, decodeInt(encodeInt(testInt)))
	}
}
