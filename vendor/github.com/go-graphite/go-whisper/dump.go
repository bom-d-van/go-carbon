// +build ignore

package main

import (
	"os"
	"time"

	whisper "github.com/go-graphite/go-whisper"
	"github.com/kr/pretty"
)

func main() {
	whisper.Now = func() time.Time {
		return time.Unix(1544478230, 0)
	}

	db, err := whisper.OpenWithOptions(os.Args[1], &whisper.Options{Compressed: true, PointsPerBlock: 7200})
	if err != nil {
		panic(err)
	}

	// whisper := db
	// buf := make([]byte, whisper.archives[2].blockSize)
	// n, err := whisper.file.ReadAt(buf, int64(whisper.archives[1].blockOffset(1)))
	// log.Printf("n = %+v\n", n)
	// if err != nil {
	// 	panic(err)
	// }

	// log.Printf("buf = %0x\n", buf)

	// var dst []dataPoint
	// // start := Now().Add(time.Hour * -24 * 365 * 2)
	// // dst, err = whisper.archives[1].readFromBlock(buf, dst, int(start.Add(17520*time.Hour).Unix()), 1544478230+3600)
	// // dst, err = whisper.archives[1].readFromBlock(buf, dst, 1517407200, 1544478230+3600)
	// dst, err = whisper.archives[1].readFromBlock(buf, dst, int(time.Unix(1544478230, 0).Add(time.Hour*-24*28).Unix()), 1544478230)
	// if err != nil {
	// 	panic(err)
	// }
	// log.Printf("len(dst) = %+v\n", len(dst))
	// log.Printf("dst = %+v\n", dst[len(dst)-10:])
	// log.Printf("dst = %+v\n", dst[:10])

	// return

	// {
	// 	data, err := db.Fetch(
	// 		// int(time.Unix(1544478230, 0).Add(time.Hour*-24*365*2).Add(time.Hour*-72).Unix()),
	// 		// int(time.Unix(1544478230, 0).Add(time.Hour*-24*365*2).Add(time.Hour*17520).Unix()),

	// 		// 1481403600,
	// 		// 1544472000,

	// 		// int(time.Unix(1544478230, 0).Add(time.Hour*-24*28).Unix()),
	// 		// // int(time.Unix(1544478230, 0).Add(time.Hour*-24*28).Add(time.Minute*40320).Unix()),
	// 		// 1544478230,

	// 		int(time.Unix(1544478230, 0).Add(time.Hour*-24*2).Unix()),
	// 		int(time.Unix(1544478230, 0).Unix()),
	// 	)
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	// return

	// 	pretty.Println(data)
	// }
	// {
	// 	data, err := db.Fetch(
	// 		// int(time.Unix(1544478230, 0).Add(time.Hour*-24*365*2).Add(time.Hour*-72).Unix()),
	// 		// int(time.Unix(1544478230, 0).Add(time.Hour*-24*365*2).Add(time.Hour*17520).Unix()),

	// 		// 1481403600,
	// 		// 1544472000,

	// 		int(time.Unix(1544478230, 0).Add(time.Hour*-24*28).Unix()),
	// 		// int(time.Unix(1544478230, 0).Add(time.Hour*-24*28).Add(time.Minute*40320).Unix()),
	// 		1544478230,
	// 	)
	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	// return

	// 	pretty.Println(data)
	// }

	whisper.Debug(true)
	{
		data, err := db.Fetch(
			// int(time.Unix(1544789552, 0).Add(time.Hour*-24*2).Unix()),
			// int(time.Unix(1544789552, 0).Unix()),
			// 1510236480, 1510681980,
			1543598100, 1543601520,
		)
		if err != nil {
			panic(err)
		}

		// return

		pretty.Println(data)
	}

	// pretty.Println(db)
	// db.Dump()

	// fmt.Println(int(time.Unix(1544478230, 0).Add(time.Hour * -24 * 365 * 2).Add(time.Hour * -72).Unix()))
	// fmt.Println(int(time.Unix(1544478230, 0).Add(time.Hour * -24 * 365 * 2).Add(time.Hour * 17520).Unix()))
	// fmt.Println(1481403600)
	// fmt.Println(1544472000)
}
