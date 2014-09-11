package main

import (
	"bytes"
	"io"
	"log"
	"fmt"
)

type MetadataType byte

/*
METADATA FORMAT
===============

Metadata is stored in various different types depending
on the type stored in the associated column. For that relation,
refer to table_map_metadata.html.

Here are the categories of metadata and what is stored in them:

PACK_SIZE_METADATA
[1]byte: [uint8 packsize]

VARCHAR_METADATA
[2]byte: [uint16 max length]

STRING_METADATA
[2]byte: [byte realtype (mysql var type), uint8 packsize]

BITSET_METADATA
[2]byte: [uint8 number of bits, uint8 packsize]

NEW_DECIMAL_METADATA
[2]byte: [uint8 precision, uint8 number of decimals]

*/

const (
	PACK_SIZE_METADATA   MetadataType = iota
	VARCHAR_METADATA
	STRING_METADATA
	BITSET_METADATA
	NEW_DECIMAL_METADATA
	TIME_V2_METADATA
)

func fatalMetadataLengthMismatch() {
	log.Fatal("Mismatch of metadata length!")
}

type ColumnMetadata struct {
	data     []byte
	metaType MetadataType
}

func DeserializeColomnMetadata(r io.Reader, colType byte) *ColumnMetadata {
	switch colType {

	// 1 byte pack size cases
	case MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE, MYSQL_TYPE_BLOB, MYSQL_TYPE_GEOMETRY:
		data, err := ReadBytes(r, 1)
		fatalErr(err)

		return &ColumnMetadata{
			data: data,
			metaType: PACK_SIZE_METADATA,
		}
	
	case MYSQL_TYPE_TIMESTAMP_V2, MYSQL_TYPE_TIME_V2, MYSQL_TYPE_DATETIME_V2:
		data, err := ReadBytes(r, 1)
		fatalErr(err)

		return &ColumnMetadata{
			data: data,
			metaType: TIME_V2_METADATA,
		}

	// 2 byte cases
	case MYSQL_TYPE_VARCHAR, MYSQL_TYPE_BIT, MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING:
		data, err := ReadBytes(r, 2)
		fatalErr(err)

		var metaType MetadataType

		switch colType {
		case MYSQL_TYPE_VARCHAR:
			metaType = VARCHAR_METADATA

		case MYSQL_TYPE_BIT:
			metaType = BITSET_METADATA

		case MYSQL_TYPE_NEWDECIMAL:
			metaType = NEW_DECIMAL_METADATA

		case MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING:
			metaType = STRING_METADATA
		}

		return &ColumnMetadata{
			data: data,
			metaType: metaType,
		}
	}

	return nil
}

func (m *ColumnMetadata) PackSize() uint8 {
	var toRead []byte

	switch(m.metaType) {
	case PACK_SIZE_METADATA:
		if len(m.data) != 1 {
			fatalMetadataLengthMismatch()
		}

		toRead = m.data[:]

	case STRING_METADATA, BITSET_METADATA: // NOTE: may be big endian (see shyiko version)
		fmt.Println("!!! HEY, I JUST DECODED STRING METADATA. IF THERE IS AN ERROR BELOW, THIS COULD BE WHY.")
		if len(m.data) != 2 {
			fatalMetadataLengthMismatch()
		}

		toRead = m.data[1:]

	case NEW_DECIMAL_METADATA:
		log.Fatal("Cannot call PackSize() on NEW_DECIMAL_METADATA")

	case VARCHAR_METADATA:
		log.Fatal("Cannot call PackSize() on VARCHAR_METADATA")

	default:
		log.Fatal("Invalid metadata type!")
	}

	v, err := uint8FromBuffer(bytes.NewBuffer(toRead))
	fatalErr(err)
	return v
}

func (m *ColumnMetadata) RealType() byte {
	if m.metaType != STRING_METADATA {
		log.Fatal("Cannot call RealType() on metadata that is not STRING_METADATA")
	}

	if len(m.data) != 2 {
		fatalMetadataLengthMismatch()
	}

	return m.data[0]
}

func (m *ColumnMetadata) MaxLength() uint16 {
	if m.metaType != VARCHAR_METADATA {
		log.Fatal("Cannot call MaxLength() on metadata that is not VARCHAR_METADATA")
	}

	if len(m.data) != 2 {
		fatalMetadataLengthMismatch()
	}

	v, err := uint16FromBuffer(bytes.NewBuffer(m.data))
	fatalErr(err)
	return v
}

func (m *ColumnMetadata) Precision() uint8 {
	if m.metaType != NEW_DECIMAL_METADATA {
		log.Fatal("Cannot call Precision() on metadata that is not NEW_DECIMAL_METADATA")
	}

	if len(m.data) != 2 {
		fatalMetadataLengthMismatch()
	}

	v, err := uint8FromBuffer(bytes.NewBuffer(m.data[:1]))
	fatalErr(err)
	return v
}

func (m *ColumnMetadata) Decimals() uint8 {
	if m.metaType != NEW_DECIMAL_METADATA {
		log.Fatal("Cannot call Decimals() on metadata that is not NEW_DECIMAL_METADATA")
	}

	if len(m.data) != 2 {
		fatalMetadataLengthMismatch()
	}

	v, err := uint8FromBuffer(bytes.NewBuffer(m.data[1:]))
	fatalErr(err)
	return v
}

func (m *ColumnMetadata) BitsetLength() uint8 {
	if m.metaType != BITSET_METADATA {
		log.Fatal("Cannot call BitsetLength() on metadata that is not BITSET_METADATA")
	}

	if len(m.data) != 2 {
		fatalMetadataLengthMismatch()
	}

	v, err := uint8FromBuffer(bytes.NewBuffer(m.data[:1]))
	fatalErr(err)
	return v
}

func (m *ColumnMetadata) FractionalSecondsPrecision() uint8 {
	if m.metaType != TIME_V2_METADATA {
		log.Fatal("Cannot call FractionalSecondsPrecision() on metadata that is not TIME_V2_METADATA")
	}

	if len(m.data) != 2 {
		fatalMetadataLengthMismatch()
	}

	v, err := uint8FromBuffer(bytes.NewBuffer(m.data))
	fatalErr(err)
	return v
}
