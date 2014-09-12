package main

import (
	"fmt"
	"io"
)

type EventData interface {}

type EventDeserializer interface {
	Deserialize(io.ReadSeeker, *EventHeader) EventData
}

type Event struct {
	header *EventHeader
	data   EventData
}

func ReadEvent(r io.ReadSeeker) *Event {
	event := new(Event)

	event.header = deserializeEventHeader(r)
	fmt.Println("Event:")
	fmt.Println("  Head:", event.header)
	fmt.Println("  Type:", event.header.Type)
	event.data   = event.header.DataDeserializer().Deserialize(r, event.header)

	currentPos, err := r.Seek(0, 1)
	fatalErr(err)

	if currentPos != int64(event.header.NextPosition) {
		_, err = r.Seek(int64(event.header.NextPosition), 0)
		// Alternative, slightly faster:
		// _, err = r.Seek(int64(event.header.NextPosition) - currentPos, 1)
	}

	return event
}
