package main

import (
	"io"
	"log"
	"fmt"
)

type TableMapEvent struct {
	TableId         uint64
	DatabaseName    string
	TableName       string
	NumberOfColumns uint64
	ColumnTypes     []byte
	Metadata        []*ColumnMetadata
	CanBeNull       Bitset
}

type TableMapEventDeserializer struct {}

/*
TABLE MAP DATA
==============

Fixed:
6 bytes = table id
2 bytes = reserved (skip)

Let:
X = database name length
Y = table name length
P = number determined by byte key; can be 0, 2, 3, or 8
C = number of columns
N = (7 + C) / 8
M = metadata length

Variable:
1 byte    = database name length
X+1 bytes = database name (null terminated)
1 byte    = table name length
Y+1 bytes = table name (null terminated)
1 byte    = packed int byte key (see ReadPackedInteger)
P bytes   = number of columns
C bytes   = column types
1 byty    = packed int byte key (see ReadPackedInteger)
P bytes   = metdata length 
M bytes   = metadata (skipping for now)
N bytes   = can be null bitset

*/

func (d *TableMapEventDeserializer) Deserialize(reader io.ReadSeeker, header *EventHeader) EventData {
	fmt.Println("Expected next pos: ", header.NextPosition)

	e := new(TableMapEvent)

	var err error

	e.TableId, err = ReadTableId(reader)
	fatalErr(err)

	_, err = reader.Seek(3, 1) // Skip 2 reserved and 1 database name length bytes
	fatalErr(err)

	e.DatabaseName, err = ReadNullTerminatedString(reader)
	fatalErr(err)

	_, err = reader.Seek(1, 1) // Skip table name length
	fatalErr(err)

	e.TableName, err = ReadNullTerminatedString(reader)
	fatalErr(err)

	/*
	OLD STYLE
	=========

	var nullTerm uint8

	_, err := reader.Seek(2, 1) // reserved
	fatalErr(err)

	databaseNameLength, err := ReadUint8(reader)
	fatalErr(err)

	databaseNameBytes, err := ReadBytes(reader, int(databaseNameLength))
	fatalErr(err)

	// String null terminator
	nullTerm, err = ReadUint8(reader)
	if nullTerm != 0 {
		log.Fatal("expected null terminator")
	}

	e.DatabaseName = string(databaseNameBytes)

	tableNameLength, err := ReadUint8(reader)
	fatalErr(err)

	tableNameBytes, err := ReadBytes(reader, int(tableNameLength))
	fatalErr(err)

	// String null terminator
	nullTerm, err = ReadUint8(reader)
	if nullTerm != 0 {
		log.Fatal("expected null terminator")
	}

	e.TableName = string(tableNameBytes)
	*/

	e.NumberOfColumns, err = ReadPackedInteger(reader)
	fatalErr(err)

	e.ColumnTypes, err = ReadBytes(reader, int(e.NumberOfColumns))
	fatalErr(err)

	metadataLength, err := ReadPackedInteger(reader)
	fatalErr(err)

	// skip for now
	// fatalErr(reader.Seek(metadataLength, 1))

	// This represents how much we have read to make sure we don't over read
	metadataRead := uint64(0)
	metadata := make([]*ColumnMetadata, len(e.ColumnTypes))

	for i, t :=  range e.ColumnTypes {
		metadata[i] = DeserializeColomnMetadata(reader, t)

		if metadata[i] != nil {
			metadataRead += uint64(len(metadata[i].data))
		}

		if metadataRead > metadataLength {
			log.Fatal("Exceeded metadata length while processing metadata")
		}
	}

	fmt.Printf("read: %v, total: %v\n", metadataRead, metadataLength)

	e.Metadata = metadata

	e.CanBeNull, err = ReadBitset(reader, int(e.NumberOfColumns))
	fatalErr(err)

	// Insert into tableMapCollectionInstance
	mapCollection := GetTableMapCollectionInstance()
	if _, ok := mapCollection[e.TableId]; !ok {
		mapCollection[e.TableId] = e
	}

	n, err := reader.Seek(0, 1)
	fatalErr(err)
	fmt.Println("Actual next pos:", n)
	fmt.Println("vardump:", *e)

	lastBytes, err := ReadBytes(reader, 4)
	fmt.Println("Remaining bytes:", lastBytes)

	for _, m := range e.Metadata {
		if m != nil {
			fmt.Println(*m)
		}
	}

	return e
}
