package whisper

import (
	"fmt"
	"time"
)

func (whisper *Whisper) CheckIntegrity() {
	meta := make([]byte, whisper.MetadataSize())
	if err := whisper.fileReadAt(meta, 0); err != nil {
		panic(err)
	}
	empty := [4]byte{}
	copy(meta[whisper.crc32Offset():], empty[:])
	metacrc := crc32(meta, 0)
	if metacrc != whisper.crc32 {
		fmt.Printf("error: header crc: disk: %08x cal: %08x\n", whisper.crc32, metacrc)
	}

	for _, arc := range whisper.archives {
		for _, block := range arc.blockRanges {
			// fmt.Printf("archive.%d %s block %d\n", arc.secondsPerPoint, time.Duration(int(time.Second)*arc.secondsPerPoint*arc.numberOfPoints), block.index)
			if block.start == 0 {
				// fmt.Printf("    [empty]\n")
				continue
			}

			buf := make([]byte, arc.blockSize)
			if err := whisper.fileReadAt(buf, int64(arc.blockOffset(block.index))); err != nil {
				panic(err)
			}

			_, endOffset, err := arc.readFromBlock(buf, []dataPoint{}, block.start, block.end)
			if err != nil {
				panic(err)
			}

			if block.index != arc.cblock.index {
				endOffset += 1
			}
			crc := crc32(buf[:endOffset], 0)
			if crc != block.crc32 {

				fmt.Printf("error: archive.%d.block.%d crc32: %08x check: %08x endOffset: %d/%d\n", arc.secondsPerPoint, block.index, block.crc32, crc, endOffset, int(arc.blockOffset(block.index))+endOffset)
			}
		}
	}
}

func (whisper *Whisper) Dump(full bool) {
	if !whisper.compressed {
		panic("Dump works only for compressed whisper files.")
	}

	fmt.Printf("aggregation_method:        %d\n", whisper.aggregationMethod)
	fmt.Printf("max_retention:             %d\n", whisper.maxRetention)
	fmt.Printf("x_files_factor:            %f\n", whisper.xFilesFactor)
	fmt.Printf("compressed:                %t\n", whisper.compressed)
	fmt.Printf("comp_version:              %d\n", whisper.compVersion)
	fmt.Printf("points_per_block:          %d\n", whisper.pointsPerBlock)
	fmt.Printf("avg_compressed_point_size: %f\n", whisper.avgCompressedPointSize)
	fmt.Printf("crc32:                     %X\n", whisper.crc32)

	for _, arc := range whisper.archives {
		arc.dumpInfo()
	}

	if !full {
		return
	}

	for _, arc := range whisper.archives {
		fmt.Println("")

		if arc.hasBuffer() {
			fmt.Printf("archive %s buffer[%d]:\n", time.Duration(int(time.Second)*arc.secondsPerPoint*arc.numberOfPoints), len(arc.buffer)/PointSize)
			dps := unpackDataPoints(arc.buffer)
			for i, p := range dps {
				fmt.Printf("  % 4d %d: %f\n", i, p.interval, p.value)
			}
		}

		for _, block := range arc.blockRanges {
			fmt.Printf("archive.%d %s block %d\n", arc.secondsPerPoint, time.Duration(int(time.Second)*arc.secondsPerPoint*arc.numberOfPoints), block.index)
			if block.start == 0 {
				fmt.Printf("    [empty]\n")
				continue
			}

			buf := make([]byte, arc.blockSize)
			if err := whisper.fileReadAt(buf, int64(arc.blockOffset(block.index))); err != nil {
				panic(err)
			}
			dps, endOffset, err := arc.readFromBlock(buf, []dataPoint{}, block.start, block.end)
			if err != nil {
				panic(err)
			}

			if block.index != arc.cblock.index {
				endOffset += 1
			}
			crc := crc32(buf[:endOffset], 0)
			fmt.Printf("crc32: %08x check: %08x endOffset: %d\n", block.crc32, crc, int(arc.blockOffset(block.index))+endOffset)

			for i, p := range dps {
				// continue
				fmt.Printf("  % 4d %d: %f\n", i, p.interval, p.value)
			}
		}
	}
}

// TODO: check if block ranges match data in blocks
func (archive *archiveInfo) dumpInfo() {
	fmt.Println("")
	fmt.Printf("number_of_points:  %d %s\n", archive.numberOfPoints, time.Duration(int(time.Second)*archive.secondsPerPoint*archive.numberOfPoints))
	fmt.Printf("seconds_per_point: %d %s\n", archive.secondsPerPoint, time.Duration(int(time.Second)*archive.secondsPerPoint))
	fmt.Printf("buffer_size:       %d\n", archive.bufferSize)
	fmt.Printf("block_size:        %d\n", archive.blockSize)
	fmt.Printf("point_size:        %f\n", archive.avgCompressedPointSize)
	fmt.Printf("block_count:       %d\n", archive.blockCount)
	fmt.Printf("cblock\n")
	fmt.Printf("  index:     %d\n", archive.cblock.index)
	fmt.Printf("  p[0].interval:     %d\n", archive.cblock.p0.interval)
	fmt.Printf("  p[n-2].interval:   %d\n", archive.cblock.pn2.interval)
	fmt.Printf("  p[n-1].interval:   %d\n", archive.cblock.pn1.interval)
	fmt.Printf("  last_byte:         %08b\n", archive.cblock.lastByte)
	fmt.Printf("  last_byte_offset:  %d\n", archive.cblock.lastByteOffset)
	fmt.Printf("  last_byte_bit_pos: %d\n", archive.cblock.lastByteBitPos)
	fmt.Printf("  crc32:             %08x\n", archive.cblock.crc32)
	fmt.Printf("  stats:\n")
	// fmt.Printf("     interval.len1:    %d\n", archive.stats.interval.len1)
	// fmt.Printf("     interval.len9:    %d\n", archive.stats.interval.len9)
	// fmt.Printf("     interval.len12:   %d\n", archive.stats.interval.len12)
	// fmt.Printf("     interval.len16:   %d\n", archive.stats.interval.len16)
	// fmt.Printf("     interval.len36:   %d\n", archive.stats.interval.len36)
	// fmt.Printf("     value.same:       %d\n", archive.stats.value.same)
	// fmt.Printf("     value.sameLen:    %d\n", archive.stats.value.sameLen)
	// fmt.Printf("     value.variedLen:  %d\n", archive.stats.value.variedLen)
	fmt.Printf("     extend.block:     %d\n", archive.stats.extend.block)
	fmt.Printf("     extend.pointSize: %d\n", archive.stats.extend.pointSize)
	fmt.Printf("     discard.oldInterval: %d\n", archive.stats.discard.oldInterval)

	// archive.sortBlockRanges()

	for _, block := range archive.getSortedBlockRanges() {
		// fmt.Printf("%d: %d-%d %d %d\n", block.index, block.start, block.end, (block.end-block.start)/archive.secondsPerPoint+1, archive.blockOffset(block.index))
		fmt.Printf("%02d: %10d - %10d count:%5d offset:%d crc32:%08x\n", block.index, block.start, block.end, block.count, archive.blockOffset(block.index), block.crc32)
	}
}
