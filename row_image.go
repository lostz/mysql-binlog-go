package main

import (
	"io"
)

// TODO: decide how to structure this relationally to table
type RowImageCell interface {
	Empty() bool
}

type NumberRowImageCell              uint64
type FloatingPointNumberRowImageCell float32
type ComplexNumberRowImageCell       complex64

func DeserializeRowImageCell(r io.Reader, tableMap *TableMapCollection, columnIndex int) RowImageCell {
	mysqlType, ok := tableMap.ColumnTypes[columnIndex]
	if !ok {
		log.Fatal("Could not find type for column index", columnIndex, "in table map")
	}

	switch mysqlType {
	// impossible cases
	case MYSQL_TYPE_ENUM, MYSQL_TYPE_NEWDATE, MYSQL_TYPE_SET,
	  MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB:

	case MYSQL_TYPE_DECIMAL:

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

	case MYSQL_TYPE_TIMESTAMP:

	case MYSQL_TYPE_DATE:

	case MYSQL_TYPE_TIME:

	case MYSQL_TYPE_DATETIME:

	case MYSQL_TYPE_YEAR:
		v, err := ReadUint8(r)
		fatalErr(err)

		return NumberRowImageCell(1900 + v)


	case MYSQL_TYPE_BIT:
		metadata := tableMap.Metadata[columnIndex]
		
		// TODO

	case MYSQL_TYPE_NEWDECIMAL:
		// Not currently supported, may never be supported
		log.Fatal("NEWDECIMAL values are not supported.")

	case MYSQL_TYPE_VARCHAR, MYSQL_TYPE_VAR_STRING:

	case MYSQL_TYPE_STRING:

	case MYSQL_TYPE_BLOB:

	case MYSQL_TYPE_GEOMETRY:

	default:
		log.Fatal("Unsupported mysql type:", mysqlType)
	}
}
