package main

import (
	"bytes"
	"fmt"
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
			fmt.Println("datetime")
		} else {
			fn = ReadTimestampV2
			fmt.Println("timestamp")
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
		// If you see this and this has been working fine for a while, remove this
		tempErr := func(err error) {
			if err != nil {
				fmt.Println("!!! SOMETHING WENT WRONG IN VARCHAR")
				fmt.Println("Hint: lengthBytes var may be stored as packed int or based on max length (use metadata).")
				fatalErr(err)
			}
		}

		lengthBytes, err := ReadUint8(r)
		tempErr(err)

		fmt.Println("first:", lengthBytes)

		b, err := ReadBytes(r, int(lengthBytes))
		tempErr(err)

		fmt.Println("bytes:", string(b))

		return StringRowImageCell{
			mysqlType: mysqlType,
			value:     string(b),
		}

	case MYSQL_TYPE_STRING, MYSQL_TYPE_VAR_STRING:
		tempErr := func(err error) {
			if err != nil {
				fmt.Println("!!! SOMETHING WENT WRONG IN STRING/VAR_STRING")
				fatalErr(err)
			}
		}

		/*
		I'm still not completely sure if this is the correct way to do this.

		Here is a possible alternative for acquiring the length:
		
		length, err := ReadUint8(r)

		or

		metadata := tableMap.Metadata[columnIndex]
		packSize := metadata.PackSize()
		lengthBytes, err := ReadBytes(r, int(packSize))

		*/

		length, err := ReadPackedInteger(r)
		tempErr(err)

		b, err := ReadBytes(r, int(length))
		tempErr(err)

		return StringRowImageCell{
			mysqlType: mysqlType,
			value:     string(b),
		}

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
