package connector

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"log"
	"fmt"
)

type AuthenticationCommand struct {
	schema             string
	username           string
	password           string
	salt               string
	clientCapabilities uint16
	collation          uint8
}

func (cmd *AuthenticationCommand) PacketNumber() uint8 {
	return uint8(1)
}

func (cmd *AuthenticationCommand) Body() ([]byte, error) {
	buf := new(bytes.Buffer)

	capabilities := uint(cmd.clientCapabilities)

	if cmd.clientCapabilities == 0 {
		capabilities = CAPABILITIES_LONG_FLAG | CAPABILITIES_PROTOCOL_41 | CAPABILITIES_SECURE_CONNECTION

		if cmd.schema != "" {
			capabilities |= CAPABILITIES_CONNECT_WITH_DB
		}
	}

	err := binary.Write(buf, binary.LittleEndian, uint32(capabilities))

	if err != nil {
		return []byte{}, err
	}

	// pad to 4 bytes
	for i := buf.Len(); i < 4; i++ {
		err = buf.WriteByte(byte(0))

		if err != nil {
			return []byte{}, err
		}
	}

	// maximum packet length (we don't want to set this)
	_, err = buf.Write(make([]byte, 4))

	if err != nil {
		return []byte{}, err
	}

	err = binary.Write(buf, binary.LittleEndian, &cmd.collation)

	if err != nil {
		return []byte{}, err
	}

	// skip these values
	for i := 0; i < 23; i++ {
		buf.WriteByte(byte(0))
	}

	_, err = buf.WriteString(cmd.username)

	if err != nil {
		return []byte{}, err
	}

	// null terminated
	err = buf.WriteByte(byte(0))

	if err != nil {
		return []byte{}, err
	}

	fmt.Println("1")

	passwordSha1 := cmd.passwordCompatibleWithMySql411()
	passwordSha1Length := uint8(len(passwordSha1))

	err = binary.Write(buf, binary.LittleEndian, passwordSha1Length)

	if err != nil {
		return []byte{}, err
	}

	fmt.Println("2")

	err = binary.Write(buf, binary.LittleEndian, passwordSha1)

	if err != nil {
		return []byte{}, err
	}

	fmt.Println("3")

	if cmd.schema != "" {
		_, err = buf.WriteString(cmd.schema)

		if err != nil {
			return []byte{}, err
		}

		// null terminated
		err = buf.WriteByte(byte(0))

		if err != nil {
			return []byte{}, err
		}
	}

	fmt.Println("4")

	return buf.Bytes(), nil
}

// password hashing/salting helpers
func (cmd *AuthenticationCommand) mustDigestSha1(b []byte) []byte {
	h := sha1.New()
	n, err := h.Write(b)

	if err != nil {
		log.Fatal("Failed to write to sha1 digester")
	}

	if n != len(b) {
		log.Fatal("Password digester write length mismatch")
	}

	return h.Sum(nil)
}

func xor(a, b []byte) []byte {
	r := make([]byte, len(a))
	
	for i := 0; i < len(r); i++ {
		r[i] = byte(a[i] ^ b[i])
	}

	return r
}

// TODO: rewrite this (it's super confusing and bad)
func (cmd *AuthenticationCommand) passwordCompatibleWithMySql411() []byte {
	passwordHash := cmd.mustDigestSha1([]byte(cmd.password))
	return xor(passwordHash, cmd.mustDigestSha1(append([]byte(cmd.salt), cmd.mustDigestSha1(passwordHash)...)))
}
