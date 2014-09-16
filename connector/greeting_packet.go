package connector

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

type GreetingPacket struct {
	ProtocolVersion    uint8
	ServerVersion      string
	ThreadId           uint32
	ServerCapabilities uint16
	ServerCollation    uint8
	ServerStatus       uint16
	Scramble           string
	PluginPovidedData  string
}

// temp functions
func ReadUint8(reader *bufio.Reader) (uint8, error) {
	b := make([]byte, 1)
	n, err := reader.Read(b)

	if err != nil {
		return uint8(0), err
	}
	if n != len(b) {
		return uint8(0), errors.New("Length mismatch in read!")
	}

	var v uint8
	err = binary.Read(bytes.NewBuffer(b), binary.LittleEndian, &v)

	if err != nil {
		return uint8(0), err
	}

	return v, nil
}

func ReadUint16(reader *bufio.Reader) (uint16, error) {
	b := make([]byte, 2)
	n, err := reader.Read(b)

	if err != nil {
		return uint16(0), err
	}
	if n != len(b) {
		return uint16(0), errors.New("Length mismatch in read!")
	}

	var v uint16
	err = binary.Read(bytes.NewBuffer(b), binary.LittleEndian, &v)

	if err != nil {
		return uint16(0), err
	}

	return v, nil
}

func ReadUint32(reader *bufio.Reader) (uint32, error) {
	b := make([]byte, 4)
	n, err := reader.Read(b)

	if err != nil {
		return uint32(0), err
	}
	if n != len(b) {
		return uint32(0), errors.New("Length mismatch in read!")
	}

	var v uint32
	err = binary.Read(bytes.NewBuffer(b), binary.LittleEndian, &v)

	if err != nil {
		return uint32(0), err
	}

	return v, nil
}

func ReadGreetingPacket(r io.Reader) (*GreetingPacket, error) {
	var err error
	packet := new(GreetingPacket)
	reader := bufio.NewReader(r)

	packet.ProtocolVersion, err = ReadUint8(reader)

	if err != nil {
		return nil, err
	}

	versionBytes, err := reader.ReadBytes(byte(0))

	if err != nil {
		return nil, err
	}

	packet.ServerVersion = string(versionBytes[:len(versionBytes)-1])

	if err != nil {
		return nil, err
	}

	packet.ThreadId, err = ReadUint32(reader)

	if err != nil {
		return nil, err
	}

	prefixBytes, err := reader.ReadBytes(byte(0))

	if err != nil {
		return nil, err
	}

	scramblePrefix := string(prefixBytes[:len(prefixBytes)-1])

	if err != nil {
		return nil, err
	}

	packet.ServerCapabilities, err = ReadUint16(reader)

	if err != nil {
		return nil, err
	}

	packet.ServerCollation, err = ReadUint8(reader)

	if err != nil {
		return nil, err
	}

	packet.ServerStatus, err = ReadUint16(reader)

	if err != nil {
		return nil, err
	}

	// reserved
	_, err = reader.Read(make([]byte, 13))

	if err != nil {
		return nil, err
	}

	scrambleBytes, err := reader.ReadBytes(byte(0))

	if err != nil {
		return nil, err
	}

	// TODO affix scramble prefix?
	packet.Scramble = scramblePrefix + string(scrambleBytes[:len(scrambleBytes)-1])

	if err != nil {
		return nil, err
	}

	if reader.Buffered() > 0 {
		pluginProvidedDataBytes, err := reader.ReadBytes(byte(0))

		if err != nil {
			return nil, err
		}

		packet.PluginPovidedData = string(pluginProvidedDataBytes[:len(pluginProvidedDataBytes)-1])
	}

	return packet, nil
}
