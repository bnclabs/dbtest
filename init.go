package main

import "github.com/prataprc/golog"

func init() {
	setts := map[string]interface{}{
		"log.level":      "info",
		"log.flags":      "lshortfile",
		"log.timeformat": "",
		"log.prefix":     "",
	}
	log.SetLogger(nil, setts)
}
