package main

import (
	"io"
)

type EventDeserializer interface {
	Deserialize() *EventData
}

type Event struct {
	header *EventHeader
	data   *EventData
}

func NewEvent(r io.Reader) *Event {
	event := new(Event)

	event.header = deserializeEventHeader(r.reader)
	event.data   = event.header.DataDeserializer().Deserialize()

	return event
}
