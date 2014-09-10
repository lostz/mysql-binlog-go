package main

import (
	"io"
)

type TableMapEvent struct {
	TableId         uint64
	DatabaseName    string
	TableName       string
	NumberOfColumns uint64
	ColumnTypes     []byte
	Metadata        []ColumnMetadata
	CanBeNull       Bitset
}

type TableMapEventDeserializer struct {
	reader io.ReadSeeker
}

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
1 byte  = database name length
X bytes = database name
1 byte  = table name length
Y bytes = table name
1 byte  = packed int byte key (see ReadPackedInteger)
P bytes = number of columns
C bytes = column types
1 byty  = packed int byte key (see ReadPackedInteger)
P bytes = metdata length 
M bytes = metadata (skipping for now)
N bytes = can be null bitset

*/

type (d *TableMapEventDeserializer) Deserialize() *RowsEvent {
	e := new(TableMapEvent)

	var err error
	e.TableId, err = ReadTableId(d.reader)
	fatalErr(err)

	d.reader.Seek(2, 1)

	databaseNameLength, err := ReadUint8(d.reader)
	fatalErr(err)

	b := make([]byte, databaseNameLength)
	fatalErr(checkRead(d.reader.Read(b)))
	e.DatabaseName = string(b)

	tableNameLength, err := ReadUint8(d.reader)
	fatalErr(err)

	e.TableName, err = ReadBytes(d.reader, tableNameLength)
	fatalErr(err)

	e.NumberOfColumns, err = ReadPackedInteger(d.reader)
	fatalErr(err)

	e.ColumTypes, err = ReadBytes(d.reader, e.NumberOfColumns)
	fatalErr(err)

	metadataLength, err = ReadPackedInteger(d.reader)
	fatalErr(err)

	// skip for now
	// fatalErr(d.reader.Seek(metadataLength, 1))

	// This represents how much we have read to make sure we don't over read
	metadataRead := 0
	metadata := make([]*ColumnMetadata, len(e.ColumnTypes))

	for i, t :=  range e.ColumnTypes {
		metadata[i] = DeserializeColomnMetadata(d.reader, t)
		metadataRead += len(metadata[i].data)

		if metadataRead > metadataLength {
			log.Fatal("Exceeded metadata length while processing metadata")
		}
	}

	e.Metadata = metadata

	canBeNullLength := int((e.NumberOfColumns + 7) / 8)
	e.CanBeNull, err = ReadBitset(d.reader, canBeNullLength)

	// Insert into tableMapCollectionInstance
	mapCollection := GetTableMapCollectionInstance()
	if _, ok mapCollection[e.TableId]; !ok {
		mapCollection[e.TableId] = e
	}

	return e
}
