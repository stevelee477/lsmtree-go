package lsmtree

import (
	"encoding/binary"
	"io"
)

// encoding format:
// [key length][key][value length][value]

// encode encodes the Key-Value pair and uses the witer to write.
// Returns the number of bytes written and error if any.
func encode(w io.Writer, key, value []byte) (int, error) {
	// numbers of bytes written
	bytes := 0

	keyLenEncoded := encodeInt(len(key))
	valueLenEncoded := encodeInt(len(value))

	if n, err := w.Write(keyLenEncoded); err != nil {
		return n, err
	} else {
		bytes += n
	}

	if n, err := w.Write(key); err != nil {
		return n, err
	} else {
		bytes += n
	}

	if n, err := w.Write(valueLenEncoded); err != nil {
		return n, err
	} else {
		bytes += n
	}

	if n, err := w.Write(value); err != nil {
		return n, err
	} else {
		bytes += n
	}

	return bytes, nil
}

// decode decodes the Key-Value pair and uses the reader to read.
// Returns Key-Value pair and error if any.
// Value is nil if deleted
func decode(r io.Reader) ([]byte, []byte, error) {
	var keyLenEncoded [8]byte
	var valueLenEncoded [8]byte

	if _, err := r.Read(keyLenEncoded[:]); err != nil {
		return nil, nil, err
	}

	keyLen := decodeInt(keyLenEncoded[:])
	key := make([]byte, keyLen)

	if n, err := r.Read(key); err != nil {
		return nil, nil, err
	} else if n < keyLen {
		return nil, nil, io.ErrUnexpectedEOF
	}

	if _, err := r.Read(valueLenEncoded[:]); err != nil {
		return nil, nil, err
	}

	valueLen := decodeInt(valueLenEncoded[:])
	value := make([]byte, valueLen)

	if n, err := r.Read(value); err != nil {
		return nil, nil, err
	} else if n < valueLen {
		return nil, nil, io.ErrUnexpectedEOF
	} else if n == 0 {
		// deleted
		value = nil
	}

	return key, value, nil
}

// encodeInt encodes the int to slice of bytes.
func encodeInt(i int) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(i))

	return encoded[:]
}

// decodeInt decodes the int from slice of bytes.
func decodeInt(encoded []byte) int {
	return int(binary.BigEndian.Uint64(encoded))
}
