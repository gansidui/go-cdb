package cdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"log"
	"os"
	"path/filepath"
)

var BadFormatError = errors.New("bad format")
var logger = log.New(os.Stderr, "cdb ", log.LstdFlags|log.Lshortfile)

type Element struct {
	Key []byte
	Data []byte
}

func MakeFromChan(w io.WriteSeeker, c <-chan Element, d chan<- error) {
	defer func() { // Centralize error handling.
		if e := recover(); e != nil {
			logger.Panicf("error: %s", e)
			d <- e.(error)
		}
	}()

	var (
		n int
		err error
		elt Element
		klen, dlen uint32
		)

	if _, err = w.Seek(int64(headerSize), 0); err != nil {
		logger.Panicf("cannot seek to %d of %s: %s", headerSize, w, err)
	}
	buf := make([]byte, 8)
	wb := bufio.NewWriter(w)
	hash := cdbHash()
	hw := io.MultiWriter(hash, wb) // Computes hash when writing record key.
	htables := make(map[uint32][]slot)
	pos := headerSize
	// Read all records and write to output.
	for {
		elt = <-c
		// logger.Printf("recv: %+s", elt)
		if elt.Key == nil || len(elt.Key) == 0 { // end of records
			break
		}
		klen, dlen = uint32(len(elt.Key)), uint32(len(elt.Data))
		writeNums(wb, klen, dlen, buf)
		hash.Reset()
		if n, err = hw.Write(elt.Key); err == nil && uint32(n) != klen {
			logger.Printf("klen=%d written=%d", klen, n)
		} else if err != nil {
			logger.Panicf("error writing key %s: %s", elt.Key, err)
		}
		if n, err = wb.Write(elt.Data); err == nil && uint32(n) != dlen {
			logger.Printf("dlen=%d written=%d", dlen, n)
		} else if err != nil {
			logger.Panicf("error writing data: %s", err)
		}
		h := hash.Sum32()
		tableNum := h % 256
		htables[tableNum] = append(htables[tableNum], slot{h, pos})
		pos += 8 + klen + dlen
	}
	wb.Flush()
	if p, err := w.Seek(0, 1); err != nil || int64(pos) != p {
		logger.Panicf("pos=%d p=%d: %s", pos, p, err)
	}

	// Write hash tables and header.

	// Create and reuse a single hash table.
	maxSlots := 0
	for _, slots := range htables {
		if len(slots) > maxSlots {
			maxSlots = len(slots)
		}
	}
	slotTable := make([]slot, maxSlots*2)

	header := make([]byte, headerSize)
	// Write hash tables.
	for i := uint32(0); i < 256; i++ {
		slots := htables[i]
		if slots == nil {
			putNum(header[i*8:], pos)
			continue
		}

		nslots := uint32(len(slots) * 2)
		hashSlotTable := slotTable[:nslots]
		// Reset table slots.
		for j := 0; j < len(hashSlotTable); j++ {
			hashSlotTable[j].h = 0
			hashSlotTable[j].pos = 0
		}

		for _, slot := range slots {
			slotPos := (slot.h / 256) % nslots
			for hashSlotTable[slotPos].pos != 0 {
				slotPos++
				if slotPos == uint32(len(hashSlotTable)) {
					slotPos = 0
				}
			}
			hashSlotTable[slotPos] = slot
		}

		if err = writeSlots(wb, hashSlotTable, buf); err != nil {
			logger.Panicf("cannot write slots: %s", err)
		}

		putNum(header[i*8:], pos)
		putNum(header[i*8+4:], nslots)
		pos += 8 * nslots
	}

	if err = wb.Flush(); err != nil {
		logger.Panicf("error flushing %s: %s", wb, err)
	}

	if _, err = w.Seek(0, 0); err != nil {
		logger.Panicf("error seeking to begin of %s: %s", w, err)
	}

	_, err = w.Write(header)
	//p, _ := w.Seek(0, 1); logger.Printf("pos: %d", p)

	d <- err
}

// Make reads cdb-formatted records from r and writes a cdb-format database
// to w.  See the documentation for Dump for details on the input record format.
func Make(w io.WriteSeeker, r io.Reader) (err error) {
	defer func() { // Centralize error handling.
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()

	rb := bufio.NewReader(r)
	rr := &recReader{rb}
	c := make(chan Element, 1)
	d := make(chan error)

	go MakeFromChan(w, c, d)
	// Read all records and write to output.
	for {
		// Record format is "+klen,dlen:key->data\n"
		x := rr.readByte()
		if x == '\n' { // end of records
			break
		}
		if x != '+' {
			return BadFormatError
		}
		klen, dlen := rr.readNum(','), rr.readNum(':')
		key := rr.readBytesN(klen)
		rr.eatByte('-')
		rr.eatByte('>')
		data := rr.readBytesN(dlen)
		rr.eatByte('\n')
		c <- Element{key, data}
	}
	c <- Element{nil, nil}
	err = <-d

	return
}

type recReader struct {
	*bufio.Reader
}

func (rr *recReader) readByte() byte {
	c, err := rr.ReadByte()
	if err != nil {
		panic(err)
	}

	return c
}

func (rr *recReader) readBytesN(n uint32) []byte {
	buf := make([]byte, n)
	for i := uint32(0); i < n; i++ {
		buf[i] = rr.readByte()
	}
	return buf
}

func (rr *recReader) eatByte(c byte) {
	if rr.readByte() != c {
		panic(errors.New("unexpected character"))
	}
}

func (rr *recReader) readNum(delim byte) uint32 {
	s, err := rr.ReadString(delim)
	if err != nil {
		panic(err)
	}

	s = s[:len(s)-1] // Strip delim
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		panic(err)
	}

	return uint32(n)
}

func (rr *recReader) copyn(w io.Writer, n uint32) {
	if _, err := io.CopyN(w, rr, int64(n)); err != nil {
		panic(err)
	}
}

func putNum(buf []byte, x uint32) {
	binary.LittleEndian.PutUint32(buf, x)
}

func writeNums(w io.Writer, x, y uint32, buf []byte) {
	putNum(buf, x)
	putNum(buf[4:], y)
	if _, err := w.Write(buf[:8]); err != nil {
		panic(err)
	}
}

type slot struct {
	h, pos uint32
}

func writeSlots(w io.Writer, slots []slot, buf []byte) (err error) {
	for _, np := range slots {
		putNum(buf, np.h)
		putNum(buf[4:], np.pos)
		if _, err = w.Write(buf[:8]); err != nil {
			return
		}
	}

	return nil
}

type CdbWriter struct {
	w chan Element
	e chan error
	tempfh *os.File
	tempfn string
	Filename string
}

func NewWriter(cdb_fn string) (*CdbWriter, error) {
	var cw CdbWriter
	var err error
	dir, ofn := filepath.Split(cdb_fn)
	cw.tempfn = dir + "." + ofn
	cw.tempfh, err = os.OpenFile(cw.tempfn, os.O_CREATE | os.O_TRUNC | os.O_WRONLY, 0640)
	if err != nil {
		return nil, err
	}
	cw.w = make(chan Element, 1)
	cw.e = make(chan error, 0)
	cw.Filename = cdb_fn
	go MakeFromChan(cw.tempfh, cw.w, cw.e)
	return &cw, nil
}

func (cw *CdbWriter) PutPair(key []byte, val []byte) {
	cw.w <- Element{key, val}
}
func (cw *CdbWriter) Put(elt Element) {
	cw.w <- elt
}

func (cw *CdbWriter) Close() error {
	cw.w <- Element{}
	close(cw.w)
	cw.tempfh.Close()
	err, _ := <-cw.e
	if err != nil {
		return err
	}
	return os.Rename(cw.tempfn, cw.Filename)
}