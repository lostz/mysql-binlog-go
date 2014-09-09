package main

import (
	"fmt"
)

func main() {
	_, err := OpenBinlog("C:/ProgramData/MySQL/MySQL Server 5.6/data/mysql-bin.000005")

	fmt.Println("error:", err)
}
