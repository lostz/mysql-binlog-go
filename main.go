package main

import (
	"fmt"
)

func main() {
	binlog, err := OpenBinlog("/usr/local/var/mysql/mysql-bin.000070")

	fmt.Println("Error: ", err)

	fmt.Println("NextEvent()")

	for {
		event := binlog.NextEvent()

		fmt.Println("Event: ")
		fmt.Println("head:", *event.header)
	}
}
