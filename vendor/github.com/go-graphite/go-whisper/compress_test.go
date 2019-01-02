package whisper

import (
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/kr/pretty"
)

func TestBitWriter(t *testing.T) {
	var bw BitsWriter
	bw.buf = make([]byte, 8)
	bw.bitPos = 7

	bw.Write(1, 1)
	bw.Write(2, 1)
	bw.Write(3, 1)
	// bw.Write(2, 1)
	// for i := 0; i < 16; i++ {
	// 	bw.Write(1, 1)
	// }

	// fmt.Printf("-- %08b\n", bw.buf)

	bw.WriteUint(8, 0xaa)
	bw.WriteUint(12, 0x01aa)

	// 1010 01 0000 0000 1010 1010
	fmt.Printf("-- %08b\n", bw.buf)
	fmt.Printf("-- %08b\n", 12)
}

func TestBitReader(t *testing.T) {
	var br BitsReader
	br.buf = []byte{0xB3, 0x02, 0xFF, 0xFF, 0xFF}
	// br.buf = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x08}
	br.bitPos = 7

	fmt.Printf("%08b\n", br.buf)
	fmt.Printf("%08b\n", br.Read(1))
	fmt.Printf("%08b\n", br.Read(2))
	fmt.Printf("%08b\n", br.Read(3))
	fmt.Printf("%08b\n", br.Read(4))
	fmt.Printf("%08b\n", br.Read(16))
}

func TestBitsReadWrite(t *testing.T) {
	buf := make([]byte, 32)

	var bw BitsWriter
	bw.buf = buf
	bw.bitPos = 7

	var br BitsReader
	br.buf = buf
	br.bitPos = 7

	// fmt.Printf("%08b\n", 1)
	// fmt.Printf("%08b\n", 5)
	// fmt.Printf("%016b\n", 97)

	// // 1 0000010 1 01100001 00000000
	// // 1 0000010 1 01100001 0000000 00000000

	// bw.Write(1, 1)
	// bw.WriteUint(8, 5)
	// bw.WriteUint(16, 97)
	// bw.WriteUint(32, 123)
	// bw.WriteUint(64, math.Float64bits(95.1))
	// bw.WriteUint(64, 0xfffffffff1ffffff)

	// // br.Read(1)
	// fmt.Printf("br.Read(1) = %v\n", br.Read(1))
	// fmt.Printf("br.Read(8) = %v\n", br.Read(8))
	// fmt.Printf("br.Read(16) = %v\n", br.Read(16))
	// fmt.Printf("br.Read(32) = %v\n", br.Read(32))
	// fmt.Printf("br.Read(64) = %v\n", math.Float64frombits(br.Read(64)))
	// fmt.Printf("br.Read(64) = %x\n", br.Read(64))

	input := []struct {
		val uint64
		len int
	}{
		{len: 1, val: 1},
		{len: 8, val: 5},
		{len: 16, val: 97},
		{len: 32, val: 123},
		{len: 64, val: math.Float64bits(95.1)},
		{len: 64, val: 0xfffffffff1ffffff},
	}
	for _, d := range input {
		bw.WriteUint(d.len, d.val)
	}
	for i, d := range input {
		if got, want := br.Read(d.len), d.val; got != want {
			t.Errorf("%d: br.Read(%d) = %d; want %d", i, d.len, got, want)
		}
	}
}

func init() {
	log.SetFlags(log.Lshortfile)
}

func TestBlockReadWrite1(t *testing.T) {
	// debug = true

	for i := 0; i < 1; i++ {
		var acv archiveInfo
		acv.secondsPerPoint = 1
		acv.numberOfPoints = 64
		acv.cblock.lastByteBitPos = 7
		acv.blockSize = 64 * PointSize

		ts := 1543689630
		var delta int
		next := func(incs ...int) int {
			for _, i := range incs {
				delta += i
			}
			return ts + delta
		}

		// input := []dataPoint{
		// 	{interval: next(0), value: 12},
		// 	{interval: next(1), value: 24},
		// 	{interval: next(1), value: 15},

		// 	// // {interval: next(1), value: 1},
		// 	// // {interval: ts + 3, value: 1},

		// 	{interval: next(10), value: 1},
		// 	{interval: next(10), value: 2},
		// 	{interval: next(10), value: 3},
		// 	{interval: next(10), value: 4},

		// 	{interval: next(10), value: 15.5},
		// 	{interval: next(11), value: 14.0625},
		// 	{interval: next(11), value: 3.25},
		// 	{interval: next(11), value: 8.625},
		// 	{interval: next(11), value: 13.1},
		// }

		var input []dataPoint
		{
			rand.Seed(time.Now().Unix())
			input = append(input, dataPoint{interval: next(1), value: 1})
			input = append(input, dataPoint{interval: next(1), value: 1})
			input = append(input, dataPoint{interval: next(1), value: 1})
			for i := 0; i < 200; i++ {
				// input = append(input, dataPoint{interval: next(rand.Intn(60 * 60)), value: float64(rand.Intn(30))})
				input = append(input, dataPoint{interval: next(rand.Intn(10)), value: rand.NormFloat64()})
			}
		}

		buf := make([]byte, acv.blockSize)
		written, left, _ := acv.appendPointsToBlock(buf, input...)
		// fmt.Printf("%08b\n", buf[:8])
		// fmt.Printf("%08b\n", buf[8:16])
		// fmt.Printf("%08b\n", buf[16:24])

		// fmt.Printf("%08b\n", input[0].Bytes())

		// log.Printf("written = %+v\n", written)
		// pretty.Println(left)

		if true {
			for i := 0; i < written; i += 8 {
				fmt.Printf("%08b\n", buf[i:i+8])
			}

			acv.dumpInfo()
			fmt.Printf("compressd pctl: %.2f%%\n", (float64(acv.cblock.lastByteOffset)/float64(len(input)*PointSize))*100)
		}

		points := make([]dataPoint, 0, 200)

		if true {
			log.Printf("acv.cblock.lastByteOffset = %+v\n", acv.cblock.lastByteOffset)
			fmt.Println("read test ---")
		}

		points, _, err := acv.readFromBlock(buf, points, ts, ts+60*60*60)
		if err != nil {
			t.Error(err)
		}

		if !reflect.DeepEqual(input, append(points, left...)) {
			// pretty.Printf("%# v\n", input)
			// pretty.Printf("%# v\n", points)

			if diff := cmp.Diff(input, points, cmp.AllowUnexported(dataPoint{})); diff != "" {
				t.Error(diff)
			}

			t.FailNow()
		}
		// pretty.Printf("%# v\n", input)}
	}
}

func TestBlockReadWrite2(t *testing.T) {
	// debug = true

	for i := 0; i < 1; i++ {
		var acv archiveInfo
		acv.secondsPerPoint = 1
		acv.numberOfPoints = 100
		acv.cblock.lastByteBitPos = 7
		acv.blockSize = int(float64(acv.numberOfPoints) * avgCompressedPointSize)
		acv.blockRanges = make([]blockRange, 1)
		// acv.buffer

		ts := 1544456874

		var input []dataPoint = []dataPoint{
			0: {interval: 1544456874, value: 12},
			1: {interval: 1544456875, value: 24},
			2: {interval: 1544456876, value: 15},
			3: {interval: 1544456877, value: 1},
			4: {interval: 1544456878, value: 2},
			5: {interval: 1544456888, value: 3},
			6: {interval: 1544456889, value: 4},
			// 7:  {interval: 1544456890, value: 15.5},
			// 8:  {interval: 1544456891, value: 14.0625},
			// 9:  {interval: 1544456892, value: 3.25},
			// 10: {interval: 1544456893, value: 8.625},
			// 11: {interval: 1544456894, value: 13.1},
		}

		buf := make([]byte, acv.blockSize)
		var size int
		{
			// written, left :=
			written, _, _ := acv.appendPointsToBlock(buf, input[:1]...)
			log.Printf("acv.cblock.lastByteOffset = %+v\n", acv.cblock.lastByteOffset)

			size += written
			log.Printf("buf = %08b\n", buf[:30])
		}
		{
			// written, left :=
			written, _, _ := acv.appendPointsToBlock(buf[size-1:], input[1:5]...)
			log.Printf("acv.cblock.lastByteOffset = %+v\n", acv.cblock.lastByteOffset)
			log.Printf("buf = %08b\n", buf[:30])

			size += written - 1
		}
		{
			// written, left :=
			written, _, _ := acv.appendPointsToBlock(buf[size-1:], input[5:]...)
			log.Printf("acv.cblock.lastByteOffset = %+v\n", acv.cblock.lastByteOffset)
			log.Printf("buf = %08b\n", buf[:30])

			size += written - 1
		}

		log.Printf("buf = %x\n", buf)

		// if true {
		// 	for i := 0; i < written; i += 8 {
		// 		fmt.Printf("%08b\n", buf[i:i+8])
		// 	}

		// 	acv.dumpInfo()
		// 	fmt.Printf("compressd pctl: %.2f%%\n", (float64(acv.cblock.lastByteOffset)/float64(len(input)*PointSize))*100)
		// }

		points := make([]dataPoint, 0, 200)

		if true {
			log.Printf("acv.cblock.lastByteOffset = %+v\n", acv.cblock.lastByteOffset)
			fmt.Println("read test ---")
		}

		debugCompress = true

		points, _, err := acv.readFromBlock(buf, points, ts, ts+30)
		if err != nil {
			t.Error(err)
		}

		if !reflect.DeepEqual(input, points) {
			pretty.Printf("%# v\n", input)
			pretty.Printf("%# v\n", points)

			if diff := cmp.Diff(input, points, cmp.AllowUnexported(dataPoint{})); diff != "" {
				t.Error(diff)
			}

			t.FailNow()
		}
		// pretty.Printf("%# v\n", input)}
	}
}

func TestCompressedWhisperReadWrite1(t *testing.T) {
	// debug = true

	fpath := "comp.whisper"
	os.Remove(fpath)
	whisper, err := CreateWithOptions(
		fpath,
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 100},
			{secondsPerPoint: 5, numberOfPoints: 100},
		},
		Sum,
		0.7,
		&Options{Compressed: true, PointsPerBlock: 7200},
	)
	if err != nil {
		panic(err)
	}

	// Now = func() time.Time {
	// 	return time.Unix(1544478201, 0)
	// }

	ts := int(Now().Add(time.Second * -60).Unix())
	var delta int
	next := func(incs ...int) int {
		for _, i := range incs {
			delta += i
		}
		return ts + delta
	}
	input := []*TimeSeriesPoint{
		{Time: next(0), Value: 12},
		{Time: next(1), Value: 24},
		{Time: next(1), Value: 15},
		{Time: next(1), Value: 1},
		{Time: next(1), Value: 2},
		{Time: next(10), Value: 3},
		{Time: next(1), Value: 4},
		{Time: next(1), Value: 15.5},
		{Time: next(1), Value: 14.0625},
		{Time: next(1), Value: 3.25},
		{Time: next(1), Value: 8.625},
		{Time: next(1), Value: 13.1},
	}

	if err := whisper.UpdateMany(input); err != nil {
		t.Error(err)
	}
	whisper.Close()

	// pretty.Println(whisper)

	// return

	whisper, err = OpenWithOptions(fpath, &Options{Compressed: true, PointsPerBlock: 7200})
	if err != nil {
		t.Fatal(err)
	}

	// pretty.Println(whisper)

	log.Printf("ts = %+v\n", ts)
	log.Printf("ts+30 = %+v\n", ts+30)
	if ts, err := whisper.Fetch(ts, ts+300); err != nil {
		t.Error(err)
	} else {
		pretty.Println(ts)
	}
}

func TestCompressedWhisperReadWrite2(t *testing.T) {
	// debug = true

	fpath := "comp.whisper"
	os.Remove(fpath)
	whisper, err := CreateWithOptions(
		fpath,
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 100},
			{secondsPerPoint: 5, numberOfPoints: 100},
		},
		Sum,
		0.7,
		&Options{Compressed: true, PointsPerBlock: 7200},
	)
	if err != nil {
		panic(err)
	}

	Now = func() time.Time {
		return time.Unix(1544478230, 0)
	}

	ts := int(Now().Unix())
	// var delta int
	// next := func(incs ...int) int {
	// 	for _, i := range incs {
	// 		delta += i
	// 	}
	// 	return ts + delta
	// }
	// _ = next(1)
	input := []*TimeSeriesPoint{
		// {Time: next(0), Value: 12},
		// {Time: next(10), Value: 24},
		// {Time: next(1), Value: 15},
		// {Time: next(1), Value: 1},
		// {Time: next(1), Value: 2},
		// {Time: next(10), Value: 3},
		// {Time: next(1), Value: 4},
		// {Time: next(1), Value: 15.5},
		// {Time: next(1), Value: 14.0625},
		// {Time: next(1), Value: 3.25},
		// {Time: next(1), Value: 8.625},
		// {Time: next(1), Value: 13.1},
		{Time: 1544478230 - 300, Value: 666},

		{Time: 1544478201, Value: 12},

		{Time: 1544478211, Value: 24},
		{Time: 1544478212, Value: 15},
		{Time: 1544478213, Value: 1},
		{Time: 1544478214, Value: 2},

		{Time: 1544478224, Value: 3},
		{Time: 1544478225, Value: 4},
		{Time: 1544478226, Value: 15.5},
		{Time: 1544478227, Value: 14.0625},
		{Time: 1544478228, Value: 3.25},
		{Time: 1544478229, Value: 8.625},
		{Time: 1544478230, Value: 13.1},
	}

	for _, p := range input {
		fmt.Println("")
		fmt.Println("")
		if err := whisper.UpdateMany([]*TimeSeriesPoint{p}); err != nil {
			t.Error(err)
		}
	}
	whisper.Close()

	// pretty.Println(whisper)

	// return

	whisper, err = OpenWithOptions(fpath, &Options{Compressed: true, PointsPerBlock: 7200})
	if err != nil {
		t.Fatal(err)
	}

	// pretty.Println(whisper)

	log.Printf("ts = %+v\n", ts-30)
	log.Printf("ts+30 = %+v\n", ts)
	if ts, err := whisper.Fetch(1544478230-310, 1544478230-290); err != nil {
		t.Error(err)
	} else {
		pretty.Println(ts)
	}
	{
		if ts, err := whisper.Fetch(1544478230-30, 1544478230); err != nil {
			t.Error(err)
		} else {
			pretty.Println(ts)
		}
	}
	// buf := make([]byte, 200)
	// n, err := whisper.file.ReadAt(buf, int64(whisper.archives[1].blockOffset(0)))
	// log.Printf("n = %+v\n", n)
	// if err != nil {
	// 	panic(err)
	// }

	// var dst []dataPoint
	// dst, err = whisper.archives[1].readFromBlock(buf, dst, ts, ts+1000)
	// if err != nil {
	// 	panic(err)
	// }
	// log.Printf("dst = %+v\n", dst)
}

var batch = flag.Bool("batch", false, "batch mode for TestCompressedWhisperReadWrite3")

func TestCompressedWhisperReadWrite3(t *testing.T) {
	fpath := "test3.wsp"
	os.Remove(fpath)
	os.Remove(fpath + ".cwsp")

	cwhisper, err := CreateWithOptions(
		fpath+".cwsp",
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 172800},   // 1s:2d
			{secondsPerPoint: 60, numberOfPoints: 40320},   // 1m:28d
			{secondsPerPoint: 3600, numberOfPoints: 17520}, // 1h:2y
		},
		Sum,
		0,
		&Options{Compressed: true, PointsPerBlock: 7200},
	)
	if err != nil {
		panic(err)
	}
	ncwhisper, err := CreateWithOptions(
		fpath,
		[]*Retention{
			{secondsPerPoint: 1, numberOfPoints: 172800},   // 1s:2d
			{secondsPerPoint: 60, numberOfPoints: 40320},   // 1m:28d
			{secondsPerPoint: 3600, numberOfPoints: 17520}, // 1h:2y
		},
		Sum,
		0,
		&Options{Compressed: false, PointsPerBlock: 7200},
	)
	if err != nil {
		panic(err)
	}

	Now = func() time.Time {
		return time.Unix(1544478230, 0)
	}

	{
		start := Now().Add(time.Hour * -24 * 10)
		// for i := 0; i < 172800; {
		var ps []*TimeSeriesPoint
		for i := 0; i < 10*24*60*60; i++ {
			if *batch {
				ps = append(ps, &TimeSeriesPoint{
					Time:  int(start.Add(time.Duration(i) * time.Second).Unix()),
					Value: float64(i),
					// Value: 2000.0 + float64(rand.Intn(100000))/100.0,
					// Value: rand.NormFloat64(),
					// Value: float64(rand.Intn(100000)),
				})

				if len(ps) >= 300 {
					if err := cwhisper.UpdateMany(ps); err != nil {
						t.Error(err)
					}
					if err := ncwhisper.UpdateMany(ps); err != nil {
						t.Error(err)
					}
					ps = ps[:0]
				}

			} else {
				ps := []*TimeSeriesPoint{{
					Time:  int(start.Add(time.Duration(i) * time.Second).Unix()),
					Value: float64(i),
					// Value: 2000.0 + float64(rand.Intn(100000))/100.0,
					// Value: rand.NormFloat64(),
					// Value: float64(rand.Intn(100000)),
				}}

				if err := cwhisper.UpdateMany(ps); err != nil {
					t.Error(err)
				}
				if err := ncwhisper.UpdateMany(ps); err != nil {
					t.Error(err)
				}
			}
		}
	}

	// {
	// 	start := Now().Add(time.Hour * -24 * 28)
	// 	log.Printf("start = %+v\n", start.Unix())
	// 	log.Printf("end   = %+v\n", int(start.Add(time.Duration(40319)*time.Minute).Unix()))
	// 	for i := 0; i < 40320; i++ {
	// 		ps := []*TimeSeriesPoint{{
	// 			Time: int(start.Add(time.Duration(i) * time.Minute).Unix()),
	// 			// Value: float64(i),
	// 			// Value: rand.NormFloat64(),
	// 			Value: float64(rand.Intn(100)),
	// 		}}
	// 		if err := cwhisper.UpdateMany(ps); err != nil {
	// 			t.Error(err)
	// 		}
	// 		if err := ncwhisper.UpdateMany(ps); err != nil {
	// 			t.Error(err)
	// 		}
	// 	}
	// }

	// {
	// 	start := Now().Add(time.Hour * -24 * 365 * 2)
	// 	end := start.Add(time.Duration(17519) * time.Hour).Unix()
	// 	log.Printf("start = %+v\n", start.Unix())
	// 	log.Printf("end = %+v\n", end)
	// 	for i := 0; i < 17520; i++ {
	// 		ps := []*TimeSeriesPoint{{
	// 			Time: int(start.Add(time.Duration(i) * time.Hour).Unix()),
	// 			// Value: float64(i),
	// 			// Value: rand.NormFloat64(),
	// 			Value: float64(rand.Intn(100)),
	// 		}}
	// 		// log.Printf("ps[0] = %+v\n", *ps[0])
	// 		if err := cwhisper.UpdateMany(ps); err != nil {
	// 			t.Error(err)
	// 		}
	// 		if err := ncwhisper.UpdateMany(ps); err != nil {
	// 			t.Error(err)
	// 		}
	// 	}
	// }

	cwhisper.Close()
	ncwhisper.Close()

	fmt.Println("go", "run", "bin/verify.go", fpath, fpath+".cwsp")
	output, err := exec.Command("go", "run", "bin/verify.go", fpath, fpath+".cwsp").CombinedOutput()
	fmt.Fprint(os.Stdout, string(output))
	if err != nil {
		t.Fatal(err)
	}

	// cwhisper, err = OpenWithOptions(fpath, &Options{Compressed: true, PointsPerBlock: 7200})
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// log.Printf("whisper.archives[1].blockRanges = %+v\n", whisper.archives[1].blockRanges)
	// log.Println("start:", int(time.Unix(1544478230, 0).Add(time.Hour*-24*28).Unix()))
	// log.Println("end:  ", 1544478230)

	// buf := make([]byte, whisper.archives[2].blockSize)
	// n, err := whisper.file.ReadAt(buf, int64(whisper.archives[1].blockOffset(1)))
	// log.Printf("n = %+v\n", n)
	// if err != nil {
	// 	panic(err)
	// }
}

var whisperFile = flag.String("file", "", "whipser filepath")

func TestCompressedWhisperReadWrite4(t *testing.T) {
	src, err := OpenWithOptions(*whisperFile, &Options{})
	if err != nil {
		t.Fatal(err)
	}

	var rets []*Retention
	for _, arc := range src.archives {
		rets = append(rets, &Retention{secondsPerPoint: arc.secondsPerPoint, numberOfPoints: arc.numberOfPoints})
	}

	os.Remove(*whisperFile + ".cwsp")
	cdst, err := CreateWithOptions(
		*whisperFile+".cwsp", rets,
		src.aggregationMethod, src.xFilesFactor,
		&Options{Compressed: true, PointsPerBlock: 7200},
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := len(src.archives) - 1; i >= 0; i-- {
		archive := src.archives[i]

		b := make([]byte, archive.Size())
		err := src.fileReadAt(b, archive.Offset())
		if err != nil {
			t.Fatal(err)
		}
		points := unpackDataPoints(b)
		sort.Slice(points, func(i, j int) bool {
			return points[i].interval < points[j].interval
		})
		var index int
		for i := 0; i < len(points); i++ {
			if points[i].interval > 0 {
				points[index] = points[i]
				index++
			}
		}
		points = points[:index]

		if err := cdst.archives[i].appendToBlockAndRotate(points); err != nil {
			t.Fatal(err)
		}
	}

	if err := cdst.WriteHeaderCompressed(); err != nil {
		t.Fatal(err)
	}
	cdst.Close()
}
