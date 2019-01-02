package whisper

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"math/bits"

	"github.com/kr/pretty"
)

var debugCompress bool
var debugBitsWrite bool

func Debug(compress, bitsWrite bool) {
	debugCompress = compress
	debugBitsWrite = bitsWrite
}

// TODO:
// 	1. review buffer usage in read/write
// 	2. unify/simplify bits read/write api

// Timestamp:
// 1. The block header stores the starting time stamp, t−1,
// which is aligned to a two hour window; the first time
// stamp, t0, in the block is stored as a delta from t−1 in
// 14 bits. 1
// 2. For subsequent time stamps, tn:
// (a) Calculate the delta of delta:
// D = (tn − tn−1) − (tn−1 − tn−2)
// (b) If D is zero, then store a single ‘0’ bit
// (c) If D is between [-63, 64], store ‘10’ followed by
// the value (7 bits)
// (d) If D is between [-255, 256], store ‘110’ followed by
// the value (9 bits)
// (e) if D is between [-2047, 2048], store ‘1110’ followed
// by the value (12 bits)
// (f) Otherwise store ‘1111’ followed by D using 32 bits
//
// Value:
// 1. The first value is stored with no compression
// 2. If XOR with the previous is zero (same value), store
// single ‘0’ bit
// 3. When XOR is non-zero, calculate the number of leading
// and trailing zeros in the XOR, store bit ‘1’ followed
// by either a) or b):
// 	(a) (Control bit ‘0’) If the block of meaningful bits
// 	    falls within the block of previous meaningful bits,
// 	    i.e., there are at least as many leading zeros and
// 	    as many trailing zeros as with the previous value,
// 	    use that information for the block position and
// 	    just store the meaningful XORed value.
// 	(b) (Control bit ‘1’) Store the length of the number
// 	    of leading zeros in the next 5 bits, then store the
// 	    length of the meaningful XORed value in the next
// 	    6 bits. Finally store the meaningful bits of the
// 	    XORed value.

// TODO: drop ...dataPoint
//
// 7 6 5 4 3 2 1 0
func (a *archiveInfo) appendPointsToBlock(buf []byte, ps ...dataPoint) (written int, left []dataPoint, rotate bool) {
	var bw BitsWriter
	bw.buf = buf
	bw.bitPos = a.cblock.lastByteBitPos

	// set and clean possible end-of-block maker
	bw.buf[0] = a.cblock.lastByte
	bw.buf[0] &= 0xFF ^ (1<<uint(a.cblock.lastByteBitPos+1) - 1)
	bw.buf[1] = 0

	defer func() {
		a.cblock.lastByte = bw.buf[bw.index]
		a.cblock.lastByteBitPos = int(bw.bitPos)
		a.cblock.lastByteOffset += bw.index
		written = bw.index + 1

		// write end-of-block marker if there is enough space
		bw.WriteUint(4, 0x0f)
		bw.WriteUint(32, 0)

		// exclude last byte from crc32 unless block is full
		if rotate {
			a.cblock.crc32 = crc32(buf[:written], a.cblock.crc32)
		} else if written-1 > 0 {
			a.cblock.crc32 = crc32(buf[:written-1], a.cblock.crc32)
		}

		a.blockRanges[a.cblock.index].start = a.cblock.p0.interval
		a.blockRanges[a.cblock.index].end = a.cblock.pn1.interval
		a.blockRanges[a.cblock.index].count = a.cblock.count
		a.blockRanges[a.cblock.index].crc32 = a.cblock.crc32

		if debugCompress {
			log.Printf("bw.buf[bw.index-10:bw.index+10] = %08b\n", bw.buf[bw.index-10:bw.index+10])
		}
	}()

	if debugCompress {
		fmt.Println(bw.index)
		fmt.Println(a.cblock.lastByteOffset)
		fmt.Println(a.blockSize)
	}

	// TODO: return error if interval is not monotonically increasing

	for i, p := range ps {
		if p.interval == 0 {
			continue
		}

		oldBwIndex := bw.index
		oldBwBitPos := bw.bitPos
		oldBwLastByte := bw.buf[bw.index]

		var delta1, delta2 int
		if a.cblock.p0.interval == 0 {
			a.cblock.p0 = p
			a.cblock.pn1 = p
			a.cblock.pn2 = p

			copy(buf, p.Bytes())
			bw.index += PointSize

			if debugCompress {
				fmt.Printf("begin\n")
				fmt.Printf("%d: %f\n", p.interval, p.value)
			}

			continue
		}

		delta1 = p.interval - a.cblock.pn1.interval
		delta2 = a.cblock.pn1.interval - a.cblock.pn2.interval
		delta := (delta1 - delta2) / a.secondsPerPoint

		if debugCompress {
			fmt.Printf("%d %d: %v\n", i, p.interval, p.value)
		}

		// TODO: use two's complement instead to extend delta range?
		if delta == 0 {
			bw.Write(1, 0)

			if debugCompress {
				fmt.Printf("\tbuf.index = %d/%d delta = %d: %0s\n", bw.bitPos, bw.index, delta, dumpBits(1, 0))
			}

			a.stats.interval.len1++
		} else if -63 < delta && delta < 64 {
			bw.Write(2, 2)
			if delta < 0 {
				delta *= -1
				delta |= 64
			}
			bw.WriteUint(7, uint64(delta))

			if debugCompress {
				fmt.Printf("\tbuf.index = %d/%d delta = %d: %0s\n", bw.bitPos, bw.index, delta, dumpBits(2, 2, 7, uint64(delta)))
			}

			a.stats.interval.len9++
		} else if -255 < delta && delta < 256 {
			bw.Write(3, 6)
			if delta < 0 {
				delta *= -1
				delta |= 256
			}
			bw.WriteUint(9, uint64(delta))

			if debugCompress {
				fmt.Printf("\tbuf.index = %d/%d delta = %d: %0s\n", bw.bitPos, bw.index, delta, dumpBits(3, 6, 9, uint64(delta)))
			}

			a.stats.interval.len12++
		} else if -2047 < delta && delta < 2048 {
			bw.Write(4, 14)
			if delta < 0 {
				delta *= -1
				delta |= 2048
			}
			bw.WriteUint(12, uint64(delta))

			if debugCompress {
				fmt.Printf("\tbuf.index = %d/%d delta = %d: %0s\n", bw.bitPos, bw.index, delta, dumpBits(4, 14, 12, uint64(delta)))
			}

			a.stats.interval.len16++
		} else {
			bw.Write(4, 15)
			bw.WriteUint(32, uint64(p.interval))

			if debugCompress {
				fmt.Printf("\tbuf.index = %d/%d delta = %d: %0s\n", bw.bitPos, bw.index, delta, dumpBits(4, 15, 32, uint64(delta)))
			}

			a.stats.interval.len36++
		}

		pn1val := math.Float64bits(a.cblock.pn1.value)
		pn2val := math.Float64bits(a.cblock.pn2.value)
		val := math.Float64bits(p.value)
		pxor := pn1val ^ pn2val
		xor := pn1val ^ val

		if debugCompress {
			fmt.Printf("  %v %016x\n", a.cblock.pn2.value, pn2val)
			fmt.Printf("  %v %016x\n", a.cblock.pn1.value, pn1val)
			fmt.Printf("  %v %016x\n", p.value, val)
			fmt.Printf("  pxor: %016x (%064b)\n  xor:  %016x (%064b)\n", pxor, pxor, xor, xor)
		}

		if xor == 0 {
			bw.Write(1, 0)
			if debugCompress {
				fmt.Printf("\tsame, write 0\n")
			}

			a.stats.value.same++
		} else {
			plz := bits.LeadingZeros64(pxor)
			lz := bits.LeadingZeros64(xor)
			ptz := bits.TrailingZeros64(pxor)
			tz := bits.TrailingZeros64(xor)
			if plz <= lz && ptz <= tz {
				mlen := 64 - plz - ptz // meaningful block size
				bw.Write(2, 2)
				bw.WriteUint(mlen, xor>>uint64(ptz))
				if debugCompress {
					// fmt.Printf("mlen = %d %b\n", mlen, xor>>uint64(ptz))
					fmt.Printf("\tsame-length meaningful block: %0s\n", dumpBits(2, 2, uint64(mlen), xor>>uint(ptz)))
				}

				a.stats.value.sameLen++
			} else {
				if lz >= 1<<5 {
					lz = 31 // 11111
				}
				mlen := 64 - lz - tz // meaningful block size
				wmlen := mlen

				if mlen == 64 {
					mlen = 63
				} else if mlen == 63 {
					wmlen = 64
				} else {
					xor >>= uint64(tz)
				}

				if debugCompress {
					log.Printf("lz = %+v\n", lz)
					log.Printf("mlen = %+v\n", mlen)
					log.Printf("xor mblock = %08b\n", xor)
				}

				bw.Write(2, 3)
				bw.WriteUint(5, uint64(lz))
				bw.WriteUint(6, uint64(mlen))
				bw.WriteUint(wmlen, xor)
				if debugCompress {
					fmt.Printf("\tvaried-length meaningful block: %0s\n", dumpBits(2, 3, 5, uint64(lz), 6, uint64(mlen), uint64(wmlen), xor))
				}

				a.stats.value.variedLen++
			}
		}

		// TODO: fix it
		if bw.isFull() || bw.index+a.cblock.lastByteOffset+endOfBlockSize >= a.blockOffset(a.cblock.index)+a.blockSize {
			// TODO: optimize
			rotate = bw.index+a.cblock.lastByteOffset+endOfBlockSize >= a.blockOffset(a.cblock.index)+a.blockSize

			// reset dirty buffer tail
			bw.buf[oldBwIndex] = oldBwLastByte
			for i := oldBwIndex + 1; i <= bw.index; i++ {
				bw.buf[i] = 0
			}

			bw.index = oldBwIndex
			bw.bitPos = oldBwBitPos
			left = ps[i:]

			if debugCompress {
				fmt.Printf("buffer is full, write aborted\n")
			}

			break
		}

		a.cblock.pn2 = a.cblock.pn1
		a.cblock.pn1 = p
		a.cblock.count++

		if debugCompress {
			start := bw.index - 8
			end := bw.index + 8
			if start < 0 {
				start = 0
			}
			if end > len(bw.buf) {
				end = len(bw.buf) - 1
			}
			fmt.Printf("%d/%d/%d: %08b\n", bw.index, start, end, bw.buf[start:end])
		}
	}

	return
}

type BitsWriter struct {
	buf    []byte
	index  int // index
	bitPos int // 0 indexed
}

// 7 6 5 4 3 2 1 0
//
// 7 6 5 4 3 2 1 0
//
// 0 0 0 0 4 3 2 1
func (bw *BitsWriter) WriteUint(lenb int, data uint64) {
	buf := make([]byte, 8)
	switch {
	case lenb <= 8:
		buf[0] = byte(data)
	case lenb <= 16:
		binary.LittleEndian.PutUint16(buf, uint16(data))
	case lenb <= 32:
		binary.LittleEndian.PutUint32(buf, uint32(data))
	case lenb <= 64:
		binary.LittleEndian.PutUint64(buf, data)
	default:
		panic(fmt.Sprintf("invalid int size: %d", lenb))
	}

	bw.Write(lenb, buf...)
}

func (bw *BitsWriter) isFull() bool {
	return bw.index+1 >= len(bw.buf)
}

func mask(l int) uint {
	return (1 << uint(l)) - 1
}

func (bw *BitsWriter) Write(lenb int, data ...byte) {
	index := bw.index
	end := bw.index + 5
	if debugBitsWrite {
		if end >= len(bw.buf) {
			end = len(bw.buf) - 1
		}
		log.Printf("bw.bitPos = %+v\n", bw.bitPos)
		log.Printf("bw.buf = %08b\n", bw.buf[bw.index:end])
	}

	for _, b := range data {
		if lenb <= 0 || bw.isFull() {
			break
		}

		if bw.bitPos+1 > lenb {
			bw.buf[bw.index] |= b << uint(bw.bitPos+1-lenb)
			bw.bitPos -= lenb
			lenb = 0
		} else {
			var left int
			if lenb < 8 {
				left = lenb - 1 - bw.bitPos
				lenb = 0
			} else {
				left = 7 - bw.bitPos
				lenb -= 8
			}
			bw.buf[bw.index] |= b >> uint(left)

			if bw.index == len(bw.buf)-1 {
				break
			}
			bw.index++
			bw.buf[bw.index] |= (b & byte(mask(left))) << uint(8-left)
			bw.bitPos = 7 - left
		}
	}
	if debugBitsWrite {
		log.Printf("bw.buf = %08b\n", bw.buf[index:end])
	}
}

func (a *archiveInfo) readFromBlock(buf []byte, dst []dataPoint, start, end int) ([]dataPoint, int, error) {
	var br BitsReader
	br.buf = buf
	br.bitPos = 7
	br.current = PointSize

	p := unpackDataPoint(buf)
	if start <= p.interval && p.interval <= end {
		dst = append(dst, p)
	}

	var pn1, pn2 *dataPoint = &p, &p
	var debugindex int
	var exitByEOB bool

readloop:
	for {
		if br.current >= len(br.buf) {
			break
		}

		var p dataPoint

		if debugCompress {
			end := br.current + 8
			if end >= len(br.buf) {
				end = len(br.buf) - 1
			}
			fmt.Printf("new point %d:\n  br.index = %d/%d br.bitPos = %d byte = %08b peek(1) = %08b peek(2) = %08b peek(3) = %08b peek(4) = %08b buf[%d:%d] = %08b\n", len(dst), br.current, len(br.buf), br.bitPos, br.buf[br.current], br.Peek(1), br.Peek(2), br.Peek(3), br.Peek(4), br.current, end, br.buf[br.current:end])
		}

		var skip, toRead int
		switch {
		case br.Peek(1) == 0: //  0xxx
			skip = 0
			toRead = 1
		case br.Peek(2) == 2: //  10xx
			skip = 2
			toRead = 7
		case br.Peek(3) == 6: //  110x
			skip = 3
			toRead = 9
		case br.Peek(4) == 14: // 1110
			skip = 4
			toRead = 12
		case br.Peek(4) == 15: // 1111
			skip = 4
			toRead = 32
		default:
			log.Printf("br.current = %+v\n", br.current)
			log.Printf("br.bitPos = %+v\n", br.bitPos)
			log.Printf("len(buf) = %+v\n", len(buf))
			if br.current >= len(buf)-1 {
				break readloop
			}
			start, end, data := br.trailingDebug()
			return dst, br.current, fmt.Errorf("unknown timestamp prefix (archive[%d]): %04b at %d@%d, context[%d-%d] = %08b len(dst) = %d", a.secondsPerPoint, br.Peek(4), br.current, br.bitPos, start, end, data, len(dst))
		}

		br.Read(skip)
		delta := int(br.Read(toRead))

		if debugCompress {
			fmt.Printf("\tskip = %d toRead = %d delta = %d\n", skip, toRead, delta)
		}

		switch toRead {
		case 0:
			if debugCompress {
				fmt.Println("\tended by 0 bits to read")
			}
			break readloop
		case 32:
			if delta == 0 {
				exitByEOB = true
				break readloop
			}
			p.interval = delta

			if debugCompress {
				fmt.Printf("\tfull interval read: %d\n", delta)
			}
		default:
			// TODO: incorrect?
			if skip > 0 && delta&(1<<uint(toRead-1)) > 0 { // POC: toRead-1
				delta &= (1 << uint(toRead-1)) - 1
				delta *= -1
			}
			delta *= a.secondsPerPoint
			p.interval = 2*pn1.interval + delta - pn2.interval

			if debugCompress {
				fmt.Printf("\tp.interval = 2*%d + %d - %d = %d\n", pn1.interval, delta, pn2.interval, p.interval)
			}
		}

		if debugCompress {
			fmt.Printf("  br.index = %d/%d br.bitPos = %d byte = %08b peek(1) = %08b peek(2) = %08b\n", br.current, len(br.buf), br.bitPos, br.buf[br.current], br.Peek(1), br.Peek(2))
		}

		switch {
		case br.Peek(1) == 0: // 0x
			br.Read(1)
			p.value = pn1.value

			if debugCompress {
				fmt.Printf("\tsame as previous value %016x (%f)\n", math.Float64bits(pn1.value), p.value)
			}
		case br.Peek(2) == 2: // 10
			br.Read(2)
			xor := math.Float64bits(pn1.value) ^ math.Float64bits(pn2.value)
			lz := bits.LeadingZeros64(xor)
			tz := bits.TrailingZeros64(xor)
			val := br.Read(64 - lz - tz)
			p.value = math.Float64frombits(math.Float64bits(pn1.value) ^ (val << uint(tz)))

			if debugCompress {
				fmt.Printf("\tsame-length meaningful block\n")
				fmt.Printf("\txor: %016x val: %016x (%f)\n", val<<uint(tz), math.Float64bits(p.value), p.value)
			}
		case br.Peek(2) == 3: // 11
			br.Read(2)
			lz := br.Read(5)
			mlen := br.Read(6)
			rmlen := mlen
			if mlen == 63 {
				rmlen = 64
			}
			xor := br.Read(int(rmlen))
			if mlen < 63 {
				xor <<= uint(64 - lz - mlen)
			}
			p.value = math.Float64frombits(math.Float64bits(pn1.value) ^ xor)

			if debugCompress {
				fmt.Printf("\tvaried-length meaningful block\n")
				fmt.Printf("\txor: %016x mlen: %d val: %016x (%f)\n", xor, mlen, math.Float64bits(p.value), p.value)
			}
		}

		if br.badRead {
			break
		}

		pn2 = pn1
		pn1 = &p

		if start <= p.interval && p.interval <= end {
			dst = append(dst, p)
		}
		if p.interval >= end {
			break
		}
	}

	if debugindex > 0 {
		pretty.Println(dst[debugindex-10 : debugindex+10])
	}

	endOffset := br.current
	if exitByEOB && endOffset > endOfBlockSize {
		endOffset -= endOfBlockSize - 1
	}

	return dst, endOffset, nil
}

type BitsReader struct {
	buf     []byte
	current int
	bitPos  int // 0 indexed
	badRead bool
}

func (br *BitsReader) trailingDebug() (start, end int, data []byte) {
	start = br.current - 1
	if br.current == 0 {
		start = 0
	}
	end = br.current + 1
	if end >= len(br.buf) {
		end = len(br.buf) - 1
	}
	data = br.buf[start : end+1]
	return
}

// 7 6 5 4 3 2 1 0
//
// 7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0

func (br *BitsReader) Peek(c int) byte {
	if br.current >= len(br.buf) {
		return 0
	}
	if br.bitPos+1 >= c {
		return (br.buf[br.current] & (1<<uint(br.bitPos+1) - 1)) >> uint(br.bitPos+1-c)
	}
	if br.current+1 >= len(br.buf) {
		return 0
	}
	var b byte
	left := c - br.bitPos - 1
	b = (br.buf[br.current] & (1<<uint(br.bitPos+1) - 1)) << uint(left)
	b |= br.buf[br.current+1] >> uint(8-left)
	return b
}

func (br *BitsReader) Read(c int) uint64 {
	if c > 64 {
		panic("BitsReader can't read more than 64 bits")
	}

	var data uint64
	oldc := c
	for {
		if br.badRead = br.current >= len(br.buf); br.badRead || c <= 0 {
			// TODO: should reset data?
			// data = 0
			break
		}

		if c < br.bitPos+1 {
			data <<= uint(c)
			data |= (uint64(br.buf[br.current]>>uint(br.bitPos+1-c)) & ((1 << uint(c)) - 1))
			br.bitPos -= c
			break
		}

		data <<= uint(br.bitPos + 1)
		data |= (uint64(br.buf[br.current] & ((1 << uint(br.bitPos+1)) - 1)))
		c -= br.bitPos + 1
		br.current++
		br.bitPos = 7
		continue
	}

	// log.Printf("c = %d data = %064b\n", oldc, data)
	// 7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0
	switch {
	case oldc <= 8:
	case oldc <= 16:
		diff := oldc - 8
		return ((data >> uint(diff)) & 0xFF) |
			(data&(1<<uint(diff)-1))<<8
	case oldc <= 24:
		diff := oldc - 16
		return ((data >> uint(diff+8)) & 0xFF) |
			(((data >> uint(diff)) & 0xFF) << 8) |
			(data&(1<<uint(diff)-1))<<16
	case oldc <= 32:
		diff := oldc - 24
		return ((data >> uint(diff+16)) & 0xFF) |
			(((data >> uint(diff+8)) & 0xFF) << 8) |
			(((data >> uint(diff)) & 0xFF) << 16) |
			(data&(1<<uint(diff)-1))<<24
	case oldc <= 40:
		diff := oldc - 32
		return ((data >> uint(diff+24)) & 0xFF) |
			(((data >> uint(diff+16)) & 0xFF) << 8) |
			(((data >> uint(diff+8)) & 0xFF) << 16) |
			(((data >> uint(diff)) & 0xFF) << 24) |
			(data&(1<<uint(diff)-1))<<32
	case oldc <= 48:
		diff := oldc - 40
		return ((data >> uint(diff+32)) & 0xFF) |
			(((data >> uint(diff+24)) & 0xFF) << 8) |
			(((data >> uint(diff+16)) & 0xFF) << 16) |
			(((data >> uint(diff+8)) & 0xFF) << 24) |
			(((data >> uint(diff)) & 0xFF) << 32) |
			(data&(1<<uint(diff)-1))<<40
	case oldc <= 56:
		diff := oldc - 48
		return ((data >> uint(diff+40)) & 0xFF) |
			(((data >> uint(diff+32)) & 0xFF) << 8) |
			(((data >> uint(diff+24)) & 0xFF) << 16) |
			(((data >> uint(diff+16)) & 0xFF) << 24) |
			(((data >> uint(diff+8)) & 0xFF) << 32) |
			(((data >> uint(diff)) & 0xFF) << 40) |
			(data&(1<<uint(diff)-1))<<48
	case oldc <= 64:
		diff := oldc - 56
		return ((data >> uint(diff+48)) & 0xFF) |
			(((data >> uint(diff+40)) & 0xFF) << 8) |
			(((data >> uint(diff+32)) & 0xFF) << 16) |
			(((data >> uint(diff+24)) & 0xFF) << 24) |
			(((data >> uint(diff+16)) & 0xFF) << 32) |
			(((data >> uint(diff+8)) & 0xFF) << 40) |
			(((data >> uint(diff)) & 0xFF) << 48) |
			(data&(1<<uint(diff)-1))<<56
	default:
		panic("read count > 64")
	}
	return data
}

func dumpBits(data ...uint64) string {
	var bw BitsWriter
	bw.buf = make([]byte, 16)
	bw.bitPos = 7
	var l uint64
	for i := 0; i < len(data); i += 2 {
		// reflect.ValueOf(data[i]).Uint()
		bw.WriteUint(int(data[i]), data[i+1])
		l += data[i]
	}
	return fmt.Sprintf("%08b len(%d) bit_pos(%d)", bw.buf[:bw.index+1], l, bw.bitPos)
}
