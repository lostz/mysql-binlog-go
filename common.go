package main

import (
	"log"
)

// Error macro
// TODO: add backtrace
func fatalErr(err error) {
	if err != nil {
		log.Fatal("Generic fatal error:", err)
	}
}
