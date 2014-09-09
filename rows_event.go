package main

import (
	"io"
)

type RowsEventDeserializer struct {
	reader io.ReadSeeker
}

type RowsEvent struct {
	dataType        byte
	TableId         uint64
	NumberOfColumns uint64
	UsedMap         Bitset
}

func uint64FromBuffer(b bytes.Buffer) uint64 {
	var value uint64
	fatalErr(binary.Read(b, binary.LittleEndian, &value))
	return value
}

func (d *RowsEventDeserializer) readTableId() uint64 {
	b, err := ReadBytes(d.reader, 6)
	fatalErr(err)

	return uint64FromBuffer(bytes.NewBuffer(b))
}

/*
MYSQL PACKED INTEGERS
=====================

MySQL contains a special format of packed integers
that (somehow unsurprisingly) has virtually no
documentation. After a lot of searching around
and reading other libraries/MySQL source code,
I have figured out how it works.

The number of bytes in the packed integer is variable.
To determine how long the packed integer is, we have to
read the first byte and then use it's value to determine
how long the integer is. However, if it is outside of a
certain range, it will just be used by itself. Here is 
how that is determined:

 <= 250: Range is 0-250. Just use this byte and don't read anymore.
  = 251: MySQL error code (not supposed to ever be used in binlogs).
  = 252: Range is 251-0xffff. Read 2 bytes.
  = 253: Range is 0xffff-0xffffff. Read 3 bytes.
  = 254: Range is 0xffffff-0xffffffffffffffff. Read 8 bytes.

It is significantly easier with Go's typing to just default
all values to uint64. As long as you don't store the events
in an array or anything, it shouldn't cause any issues though.

*/

func (d *RowsEventDeserializer) readPackedInt() uint64 {
	b, err := ReadByte(d.reader)
	fatalErr(err)

	var firstByte uint8
	fatalErr(binary.Read(bytes.Newbuffer(b), binary.LittleEndian, &firstByte))

	if firstByte <= 251 {
		return uint64(firstByte)
	}

	bytesToRead := 0

	switch firstByte {
	case 251:
		// MySQL error code
		// something is wrong
		log.Fatal("Packed integer invalid value:", firstByte)
	case 252:
		bytesToRead = 2
	case 253:
		bytesToRead = 3
	case 254:
		bytesToRead = 8
	case 255:
		log.Fatal("Packed integer invalid value:", firstByte)
	}

	b, err = ReadBytes(d.reader, bytesToRead)
	fatalErr(err)

	return uint64FromBuffer(bytes.NewBuffer(b))
}

/*
ROWS EVENT DATA
===============

Fixed:
6 bytes = table id
2 bytes = reserved (skip)


Let:
X = number determined by byte key (see above); can be 0, 2, 3, or 8
N = (7 + number of columns) / 8
J = (7 + number of bits in column used bitfield) / 8
K = number of false bits in null bitfield (not counting padding in last byte)
U = 2 if update event, 1 for any other ones
B = number of rows (determined by reading till data length reached)

Variable:
1 byte  = packed int byte key (see above)
X bytes = number of columns (see above)
N bytes = column used bitfield
U * B * (
	J bytes = null bitfield
	K bytes = row image
)


FOR ROW IMAGE CELL DESERIALIZATION:
http://bazaar.launchpad.net/~mysql/mysql-server/5.6/view/head:/sql/log_event.cc#L1942

*/

func (d *RowsEventDeserializer) Deserialize() *RowsEvent {
	e := new(RowsEvent)
	e.dataType = 'a' //TODO

	e.TableId = d.readTableId()
	d.reader.Skip(2, 1) // reserved
	e.NumberOfColumns = d.readPackedInt()

	usedMapBytes := int((e.NumberOfColumns + 7) / 8)

	var err error
	e.UsedMap, err = ReadBitset(d.reader, useMapBytes)

	// TODO
}

func (e *RowsEvent) Type() byte {
	return e.dataType
}

