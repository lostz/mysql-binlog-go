package main

import (
	"bytes"
	"io"
	"log"
	"time"
)

type RowImage []RowImageCell

// TODO: decide how to structure this relationally to table
type RowImageCell interface {}

type NullRowImageCell                byte
type NumberRowImageCell              uint64
type FloatingPointNumberRowImageCell float32
type ComplexNumberRowImageCell       complex64
type BlobRowImageCell                []byte
type StringRowImageCell              struct {
	mysqlType byte
	value     string
}
type DurationRowImageCell            time.Duration
type TimeRowImageCell                struct {
	mysqlType byte
	value     time.Time
}

func NewNullRowImageCell(mysqlType byte) NullRowImageCell {
	return NullRowImageCell(mysqlType)
}

func DeserializeRowImageCell(r io.Reader, tableMap *TableMapEvent, columnIndex int) RowImageCell {
	mysqlType := tableMap.ColumnTypes[columnIndex]

	switch mysqlType {
	// impossible cases
	case MYSQL_TYPE_ENUM, MYSQL_TYPE_NEWDATE, MYSQL_TYPE_SET,
	  MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB:
		log.Fatal("Impossible type found in binlog!")

	case MYSQL_TYPE_TINY:
		v, err := ReadUint8(r)
		fatalErr(err)

		return NumberRowImageCell(v)

	case MYSQL_TYPE_SHORT:
		v, err := ReadUint16(r)
		fatalErr(err)

		return NumberRowImageCell(v)

	case MYSQL_TYPE_INT24:
		b, err := ReadBytes(r, 3)
		fatalErr(err)

		v, err := uint32FromBuffer(bytes.NewBuffer(b))
		fatalErr(err)

		return NumberRowImageCell(v)

	case MYSQL_TYPE_LONG:
		v, err := ReadUint32(r)
		fatalErr(err)

		return NumberRowImageCell(v)

	case MYSQL_TYPE_LONGLONG:
		v, err := ReadUint64(r)
		fatalErr(err)

		return NumberRowImageCell(v)

	case MYSQL_TYPE_FLOAT:
		var v float32
		b, err := ReadBytes(r, 4)
		fatalErr(err)

		fatalErr(readFromBinaryBuffer(bytes.NewBuffer(b), &v))

		return FloatingPointNumberRowImageCell(v)

	case MYSQL_TYPE_DOUBLE:
		// Not sure if C doubles convert to Go complex64 properly
		var v complex64
		b, err := ReadBytes(r, 8)
		fatalErr(err)

		fatalErr(readFromBinaryBuffer(bytes.NewBuffer(b), &v))

		return ComplexNumberRowImageCell(v)

	case MYSQL_TYPE_NULL:
		return NewNullRowImageCell(mysqlType)

	case MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_DATE, MYSQL_TYPE_TIME, MYSQL_TYPE_DATETIME:
		log.Fatal("time fields disabled")

	case MYSQL_TYPE_TIME_V2:
		v, err := ReadTimeV2(r)
		fatalErr(err)

		return DurationRowImageCell(v)

	case MYSQL_TYPE_DATETIME_V2, MYSQL_TYPE_TIMESTAMP_V2:
		var fn func(io.Reader, *ColumnMetadata) (time.Time, error)

		if mysqlType == MYSQL_TYPE_DATETIME_V2 {
			fn = ReadDatetimeV2
		} else {
			fn = ReadTimestampV2
		}

		v, err := fn(r, tableMap.Metadata[columnIndex])
		fatalErr(err)

		return TimeRowImageCell{
			mysqlType: mysqlType,
			value:     v,
		}

	case MYSQL_TYPE_YEAR:
		v, err := ReadUint8(r)
		fatalErr(err)

		return NumberRowImageCell(1900 + uint64(v))

	case MYSQL_TYPE_BIT:
		log.Fatal("BIT currently disabled")
		// metadata := tableMap.Metadata[columnIndex]

	case MYSQL_TYPE_NEWDECIMAL:
		// Not currently supported, may never be supported
		log.Fatal("NEWDECIMAL values are not supported.")

	case MYSQL_TYPE_VARCHAR:
		log.Fatal("VARCHAR currently disabled")

	case MYSQL_TYPE_STRING, MYSQL_TYPE_VAR_STRING:
		log.Fatal("STRING/VAR_STRING currently disabled")

		/*
		metadata := tableMap.Metadata[columnIndex]
		lengthBytes, err := ReadBytes(r, metadata.PackSize())
		fatalErr(err)

		var length uint32
		fatalErr(readFromBinaryBuffer(bytes.NewBuffer(lengthBytes), &length))

		b, err := ReadBytes(r, length)
		fatalErr(err)

		return StringRowImageCell{
			mysqlType: metadata.RealType(),
			value:     string(b),
		}
		*/

	case MYSQL_TYPE_BLOB:
		metadata := tableMap.Metadata[columnIndex]
		b, err := ReadBytes(r, int(metadata.PackSize()))
		fatalErr(err)

		return BlobRowImageCell(b)

	case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_GEOMETRY:
		log.Fatal("Mysql type discovered but not supported at this time.")

	default:
		log.Fatal("Unsupported mysql type:", mysqlType)
	}

	return NewNullRowImageCell(mysqlType)
}
