package main

import (
	"log"
	"os"

	whisper "github.com/go-graphite/go-whisper"

	"github.com/kr/pretty"
)

var _ = pretty.Println

func init() {
	log.SetFlags(log.Lshortfile)
}

func main() {
	// whisper.Now = func() time.Time {
	// 	return time.Unix(1544478230, 0)
	// }

	file1 := os.Args[1]

	db1, err := whisper.OpenWithOptions(file1, &whisper.Options{})
	if err != nil {
		panic(err)
	}

	db1.CheckIntegrity()
}
