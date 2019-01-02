package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"sync"
	"time"

	whisper "github.com/go-graphite/go-whisper"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

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
	file2 := os.Args[2]

	db1, err := whisper.OpenWithOptions(file1, &whisper.Options{})
	if err != nil {
		panic(err)
	}
	db2, err := whisper.OpenWithOptions(file2, &whisper.Options{})
	if err != nil {
		panic(err)
	}

	for _, ret := range db1.Retentions() {
		now := int(whisper.Now().Unix())
		from := now - ret.MaxRetention()
		until := now

		// from = 1545151404
		// until = 1545324204

		fmt.Println(time.Second*time.Duration(ret.MaxRetention()), ret.SecondsPerPoint())
		log.Printf("from = %+v\n", from)
		log.Printf("until = %+v\n", until)

		var dps1, dps2 *whisper.TimeSeries
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()

			var err error
			dps1, err = db1.Fetch(from, until)
			if err != nil {
				panic(err)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			var err error
			dps2, err = db2.Fetch(from, until)
			if err != nil {
				panic(err)
			}
		}()

		wg.Wait()

		var vals1, vals2 int
		for _, p := range dps1.Values() {
			if !math.IsNaN(p) {
				vals1++
			}
		}
		for _, p := range dps2.Values() {
			if !math.IsNaN(p) {
				vals2++
			}
		}

		fmt.Printf("len1 = %d len2 = %d vals1 = %d vals2 = %d\n", len(dps1.Values()), len(dps2.Values()), vals1, vals2)

		if diff := cmp.Diff(dps1.Points(), dps2.Points(), cmp.AllowUnexported(whisper.TimeSeries{}), cmpopts.EquateNaNs()); diff != "" {
			fmt.Println(diff)
			pretty.Println(dps1.Points()[17471])
			pretty.Println(dps2.Points()[17471])
			pretty.Println(dps1.Points()[17519])
			pretty.Println(dps2.Points()[17519])
			fmt.Printf("error: does not match for %s\n", file1)
		}
		// return
	}
}
