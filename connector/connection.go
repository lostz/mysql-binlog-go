package connector

import (
	"bytes"
	"fmt"
	"net"
)

func NewConnection(uri, username, password string) (*PacketListener, error) {
	fmt.Println("dialing")

	conn, err := net.Dial("tcp", uri)

	if err != nil {
		return nil, err
	}

	listener := &PacketListener{
		conn: conn,
	}

	fmt.Println("reading greeting")

	// TODO: move over to must passing bytes
	b, err := listener.Read()

	if err != nil {
		return nil, err
	}

	fmt.Println("parsing greeting")

	greeting, err := ReadGreetingPacket(bytes.NewBuffer(b))

	if err != nil {
		return nil, err
	}

	fmt.Println("Greeting:", greeting)

	fmt.Println("authenticating")

	// TODO: research schema stuff
	err = listener.authenticate(username, password, "", greeting.Scramble, greeting.ServerCollation, greeting.ServerCapabilities)

	return listener, err
}
