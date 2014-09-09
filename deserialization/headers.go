package deserialization

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

func checkRead(n int, err error, bytes []byte) error {
	if err != nil {
		return err
	}

	if n != len(bytes) {
		return fmt.Errorf("Read mismatch: length=%v, bytes=%v", n, bytes)
	}

	return nil
}

func readBytes(r io.ReadSeeker, length int) ([]byte, error) {
	b := make([]byte, length)
	n, err := r.Read(b)
	return b, checkRead(n, err, b);
}

func readUint32(r io.ReadSeeker) (uint32, error) {
	var n uint32
	b, err := readBytes(r, 4)
	if err != nil {
		return n, err
	}

	if err := binary.Read(bytes.NewBuffer(b), binary.LittleEndian, &n); err != nil {
		return n, err
	}

	return n, nil
}

func readByte(r io.ReadSeeker) (byte, error) {
	bytes, err := readBytes(r, 1)

	if err != nil {
		return byte(0), err
	}

	return bytes[0], nil
}

// This should probably return a time interface
func ReadTimestamp(r io.ReadSeeker) (uint32, error) {
	return readUint32(r)
}

func ReadType(r io.ReadSeeker) (byte, error) {
	return readByte(r)
}

func ReadServerId(r io.ReadSeeker) (uint32, error) {
	return readUint32(r)
}

func ReadLength(r io.ReadSeeker) (uint32, error) {
	return readUint32(r)
}

func ReadNextPosition(r io.ReadSeeker) (uint32, error) {
	return readUint32(r)
}

func ReadFlags(r io.ReadSeeker) ([]byte, error) {
	return readBytes(r, 2)
}
