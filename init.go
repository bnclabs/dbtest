package main

import "github.com/bnclabs/golog"

func loginit(level string) {
	setts := map[string]interface{}{
		"log.level":      level,
		"log.flags":      "lshortfile",
		"log.timeformat": "",
		"log.prefix":     "",
	}
	log.SetLogger(nil, setts)
}
