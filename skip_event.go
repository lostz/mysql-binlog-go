package main

import (
	"fmt"
	"io"
)

type SkipEvent struct {}

type SkipEventDeserializer struct {}

func (d *SkipEventDeserializer) Deserialize(reader io.ReadSeeker, header *EventHeader) EventData {
	// Inefficiency on large buffers?
	reader.Seek(int64(header.NextPosition), 0)
	fmt.Println("Skipping to", header.NextPosition)

	return &SkipEvent{}
}
