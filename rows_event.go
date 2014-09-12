package main

import (
	"fmt"
	"io"
	"log"
)

type RowsEvent struct {
	dataType        byte
	TableId         uint64
	NumberOfColumns uint64
	UsedSet         Bitset
	Rows            []RowImage
}

func (e *RowsEvent) Type() byte {
	return e.dataType
}

func (e *RowsEvent) UsedFields() int {
	used := 0

	for i := uint(0); i < uint(e.NumberOfColumns); i++ {
		if e.UsedSet.Bit(i) {
			used++
		}
	}

	return used
}

type RowsEventDeserializer struct {}

/*
ROWS EVENT DATA
===============

Let:
P = number determined by byte key; can be 0, 2, 3, or 8
N = (7 + number of columns) / 8
J = (7 + number of true bits in column used bitfield) / 8
K = number of false bits in null bitfield (not counting padding in last byte)
U = 2 if update event, 1 for any other ones
B = number of rows (determined by reading till data length reached)

Fixed Section:
6 bytes = table id
2 bytes = reserved (skip)

Variable Section:
1 byte  = packed int byte key (see ReadPackedInteger)
P bytes = number of columns
N bytes = column used bitfield
U * B * (
	J bytes = null bitfield
	K bytes = row image
)

FOR ROW IMAGE CELL DESERIALIZATION:
http://bazaar.launchpad.net/~mysql/mysql-server/5.6/view/head:/sql/log_event.cc#L1942

*/

func (d *RowsEventDeserializer) Deserialize(reader io.ReadSeeker, header *EventHeader) EventData {
	{
		b, err := ReadBytes(reader, 16)
		fatalErr(err)

		fmt.Println("rows data bytes:", b)

		_, err = reader.Seek(-16, 1)
		fatalErr(err)
	}

	e := new(RowsEvent)
	e.dataType = 'a' // TODO

	var err error
	e.TableId, err = ReadTableId(reader)
	fatalErr(err)

	_, err = reader.Seek(2, 1) // reserved
	fatalErr(err)

	// v2 row events
	switch header.Type {
		case WRITE_ROWS_EVENTv2, UPDATE_ROWS_EVENTv2, DELETE_ROWS_EVENTv2:
			extraInfoLength, err := ReadUint16(reader)
			fatalErr(err)

			_, err = reader.Seek(int64(extraInfoLength - 2), 1)
			fatalErr(err)
	}

	e.NumberOfColumns, err = ReadPackedInteger(reader)
	fatalErr(err)

	e.UsedSet, err = ReadBitset(reader, int(e.NumberOfColumns))
	fatalErr(err)

	numberOfFields := e.UsedFields()
	numberOfRows := 1 // TODO: pass in header so we can check if it is update
	e.Rows = make([]RowImage, numberOfRows)

	// TODO
	for r := 0; r < numberOfRows; r++ {
		nullSet, err := ReadBitset(reader, numberOfFields)
		fatalErr(err)

		numberOfCells := 0
		for i := 0; i < numberOfFields; i++ {
			if !nullSet.Bit(uint(i)) {
				numberOfCells++
			}
		}

		// TODO: fork this off into bitset.go in a way that makes sense
		if len(e.UsedSet) != len(nullSet) {
			log.Fatal("UsedSet and NullSet length mismatched", len(e.UsedSet), len(nullSet), numberOfFields, e.UsedSet, e.NumberOfColumns, e.TableId)
		}

		shouldDeserializeSet := MakeBitset(uint(e.NumberOfColumns))

		for i, _ := range e.UsedSet {
			shouldDeserializeSet[i] = e.UsedSet[i] & nullSet[i]
		}

		cells := make(RowImage, numberOfCells)
		tableMap, ok := GetTableMapCollectionInstance()[e.TableId]

		if !ok {
			log.Fatal("Never recieved table map event for table:", e.TableId)
		}

		for i := 0; i < int(e.NumberOfColumns); i++ {
			if shouldDeserializeSet.Bit(uint(i)) {
				cells[i] = DeserializeRowImageCell(reader, tableMap, i)
			} else if e.UsedSet.Bit(uint(i)) {
				cells[i] = NewNullRowImageCell(tableMap.ColumnTypes[i])
			} else {
				cells[i] = nil
			}
		}

		e.Rows[r] = cells
	}

	return e
}
