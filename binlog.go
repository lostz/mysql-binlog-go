package main

import (
	"fmt"
	"log"
	"io"
	"os"

	"github.com/nholland94/mysql-binlog-go/deserialization"
)

// Error macro
func fatalErr(err error) {
	if err != nil {
		log.Fatal("Generic fatal error:", err)
	}
}

// Determines the binlog version from the first event
// http://dev.mysql.com/doc/internals/en/determining-binary-log-version.html
func determineLogVersion(type_code byte, length uint32) uint8 {
	if type_code != START_EVENT_V3 && type_code != FORMAT_DESCRIPTION_EVENT {
		fmt.Printf("1: %b, %v\n", int(type_code), length)
		return 3
	} else if type_code == START_EVENT_V3 {
		if length < 75 {
			return 1
		} else {
			fmt.Println("2")
			return 3
		}
	} else if type_code == FORMAT_DESCRIPTION_EVENT {
		return 4
	} else {
		log.Fatal(fmt.Sprintf("Could not determine log version from: [%v, %v]", type_code, length))
	}

	return 0
}

type Binlog struct {
	reader     io.ReadSeeker
	logVersion uint8
}

func OpenBinlog(filepath string) (*Binlog, error) {
	file, err := os.OpenFile(filepath, os.O_RDONLY, 0)

	if err != nil {
		return nil, err
	}

	/*
	fmt.Println("unsigned")
	for i := 0; i < 19; i++ {
		b := make([]byte, 1)
		_, err := file.Read(b)
		fatalErr(err)
		fmt.Printf("%v =   %b - %v\n", i, uint8(b[0]), uint8(b[0]))
	}

	file.Seek(0, 0)

	log.Fatal("done")
	*/

	b := &Binlog{
		reader: file,
		logVersion: 0,
	}

	b.mustFindLogVersion()

	return b, nil
}

// Finds log version and move reader to end of first event
// assumes reader is still at beginning of file
func (b *Binlog) mustFindLogVersion() {
	magic := make([]byte, EVENT_TYPE_OFFSET)
	n, err := b.reader.Read(magic)

	if n != len(magic) || err != nil {
		log.Fatal("Something went wrong when reading magic number:", err)
	}

	if !checkBinlogMagic(magic) {
		log.Fatal("Binlog magic number was not correct. This is probably not a binlog.")
	}

	// Skip timestamp
	fatalErr(b.Skip(4))
	type_code, err := deserialization.ReadType(b.reader)

	if err != nil {
		log.Fatal("Failed to read type_byte:", err)
	}

	fatalErr(b.SetPosition(EVENT_LEN_OFFSET))
	length, err := deserialization.ReadLength(b.reader)

	if err != nil {
		log.Fatal("Failed to read event_length:", err)
	}

	b.logVersion = determineLogVersion(type_code, length)

	// From here on out, we assume v4 events (for now)
	// this just errors out if it isn't v4
	if b.logVersion != 4 {
		log.Fatal("Sorry, this only supports v4 logs right now.", b.logVersion)
	}

	nextPos, err := deserialization.ReadNextPosition(b.reader)

	if err != nil {
		log.Fatal("Failed to read event_length:", err)
	}

	fatalErr(b.SetPosition(int64(nextPos)))
}

func (b *Binlog) SetPosition(n int64) error {
	// does this need to be n - 1?
	_, err := b.reader.Seek(n, 0)
	return err
}

func (b *Binlog) Skip(n int64) error {
	_, err := b.reader.Seek(n, 1)
	return err
}

/*
func (b *Binlog) NextEvent() (*Event, error) {
}
*/
