package main

import (
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
	event.data   = event.header.DataDeserializer().Deserialize(r, event.header)

	return event
}
