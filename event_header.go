package main

import (
	"io"

	"github.com/nholland94/mysql-binlog-go/deserialization"
)

type EventHeader struct {
	Timestamp     uint32
	Type          byte
	ServerId      uint32
	Length        uint32
	NextPosition uint32
	Flag          []byte
}

// TODO: move this over to use encoding/binary with struct pointer
func deserializeEventHeader(r io.Reader) *EventHeader {
	h := new(EventHeader)

	h.Timestamp, err = ReadTimestamp(r)
	fatalErr(err)
	h.Type, err = ReadType(r)
	fatalErr(err)
	h.ServerId, err = ReadServerId(r)
	fatalErr(err)
	h.Length, err = ReadLength(r)
	fatalErr(err)
	h.NextPosition, err = ReadNextPosition(r)
	fatalErr(err)
	h.Flag, err = ReadFlag(r)
	fatalErr(err)

	return h
}
