package deserialization

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

/*
GENERAL PARSING INFO
====================

MySQL's binlog always stores numbers in 32-bit Little Endian and are unsigned.
(Only exception is XID, which is stored in Big Endian in some versions)

Timestamps in MySQL binlog are stored as as numbers and a UNIX epoch offsets.

*/

/*
PLEASE NOTE
===========

All functions in this file assume the passed reader is already seeked
to the first byte in whatever it is attempting to read. To read an entire
event header, execute them in this order:

ReadTimestamp
ReadType
ReadServerId
ReadLength
ReadNextPosition
ReadFlags
(Extended v4 fields coming soon)

*/

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
