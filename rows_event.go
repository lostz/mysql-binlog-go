package main

import (
	"io"
)

type RowsEvent struct {
	dataType        byte
	TableId         uint64
	NumberOfColumns uint64
	UsedSet         Bitset
}

func (e *RowsEvent) Type() byte {
	return e.dataType
}

func (e *RowsEvent) UsedFields() int {
	used := 0

	for i := 0; i < e.NumberOfColumns; i++ {
		if e.UsedSet.Bit(i) {
			used++
		}
	}

	return used
}

type RowsEventDeserializer struct {
	reader	  io.ReadSeeker
	tableMaps *map[uint64]*TableMapEvent
}

/*
ROWS EVENT DATA
===============

Fixed:
6 bytes = table id
2 bytes = reserved (skip)


Let:
P = number determined by byte key; can be 0, 2, 3, or 8
N = (7 + number of columns) / 8
J = (7 + number of true bits in column used bitfield) / 8
K = number of false bits in null bitfield (not counting padding in last byte)
U = 2 if update event, 1 for any other ones
B = number of rows (determined by reading till data length reached)

Variable:
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

func (d *RowsEventDeserializer) Deserialize() *RowsEvent {
	e := new(RowsEvent)
	e.dataType = 'a' // TODO

	var err error
	e.TableId, err = ReadTableId(d.reader)
	fatalErr(err)

	fatalErr(d.reader.Seek(2, 1)) // reserved

	e.NumberOfColumns, err = ReadPackedInteger(d.reader)
	fatalErr(err)

	usedSetSize := int((e.NumberOfColumns + 7) / 8)

	e.UsedSet, err = ReadBitset(d.reader, usedSetSize)
	fatalErr(err)

	numberOfFields := e.UsedFields()
	nullSetSize := int((numberOfFields + 7) / 8)
	numberOfRows := 1 // TODO: pass in header so we can check if it is update

	// TODO
	for r := 0; r < numberOfRows; r++ {
		nullSet, err := ReadBitset(d.reader, nullSetSize)
		fatalErr(err)

		numberOfCells := 0
		for i := 0; i < numberOfFields; i++ {
			if !nullSet.Bit(i) {
				numberOfCells++
			}
		}

		shouldDeserializeSet := e.UsedSet & nullSet
		cells := make([]*RowImageCell, numberOfCells)
		tableMap, ok := GetTableMapCollectionInstance()[e.TableId]

		if !ok {
			log.Fatal("Never recieved table map event for table:", e.TableId)
		}

		for i := 0; i < e.NumberOfColumns; i++ {
			if shouldDeserializeSet.Bit(i) {
				cells[i] = DeserializeRowImageCell(d.reader, tableMap, i)
			} else if e.UsedSet.Bit(i) {
				cells[i] = NewNullRowImageCell(tableMap.ColumnTypes[i])
			} else {
				cells[i] = nil
			}
		}
	}
}
