package main

import (
	"os"

	whisper "github.com/go-graphite/go-whisper"
)

func main() {
	db, err := whisper.OpenWithOptions(os.Args[1], &whisper.Options{})
	if err != nil {
		panic(err)
	}

	db.Dump(true)
}
