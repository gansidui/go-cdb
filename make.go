package cdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

// MaxCdbSize is the maximum CDB size: 4Gb
const MaxCdbSize = 1 << 32

// BadFormatError is the "bad format" error
var BadFormatError = errors.New("bad format")
var logger = log.New(os.Stderr, "cdb ", log.LstdFlags|log.Lshortfile)

// Element is the Key:Data pair
type Element struct {
	Key  []byte
	Data []byte
}

//MakeFromChan makes CDB reading elements from the channel, does not close it!
func MakeFromChan(w io.WriteSeeker, c <-chan Element) error {
	adder, closer, err := MakeFactory(w)
	if err != nil {
		logger.Printf("cannot create factory: %s", err)
		return err
	}
	for elt := range c {
		if err = adder(elt); err != nil {
			logger.Printf("error adding %s: %s", elt, err)
			return err
		}
	}
	if err = closer(); err != nil {
		logger.Printf("error closing cdb: %s", err)
		return err
	}
	return nil
}

// AdderFunc is the element appender
type AdderFunc func(Element) error

// CloserFunc is the Close
type CloserFunc func() error

type posHolder struct {
	pos uint32
}

//MakeFactory creates CDB and returns an adder function which should be called
//with each Element, and a closer, which finalizes the CDB.
func MakeFactory(w io.WriteSeeker) (adder AdderFunc, closer CloserFunc, err error) {
	defer func() { // Centralize error handling.
		if e := recover(); e != nil {
			logger.Panicf("error: %s", e)
			err = e.(error)
		}
	}()

	if _, err = w.Seek(int64(headerSize), 0); err != nil {
		logger.Panicf("cannot seek to %d of %s: %s", headerSize, w, err)
	}
	buf := make([]byte, 8)
	wb := bufio.NewWriter(w)
	hash := cdbHash()
	hw := io.MultiWriter(hash, wb) // Computes hash when writing record key.
	htables := make(map[uint32][]slot)
	poshold := &posHolder{headerSize}

	// Read all records and write to output.
	adder = func(elt Element) error {
		var (
			err        error
			klen, dlen uint32
			n          int
		)
		klen, dlen = uint32(len(elt.Key)), uint32(len(elt.Data))
		writeNums(wb, klen, dlen, buf)
		hash.Reset()
		if n, err = hw.Write(elt.Key); err == nil && uint32(n) != klen {
			logger.Printf("klen=%d written=%d", klen, n)
		} else if err != nil {
			logger.Panicf("error writing key %s: %s", elt.Key, err)
			return err
		}
		if n, err = wb.Write(elt.Data); err == nil && uint32(n) != dlen {
			logger.Printf("dlen=%d written=%d", dlen, n)
		} else if err != nil {
			logger.Panicf("error writing data: %s", err)
			return err
		}
		h := hash.Sum32()
		tableNum := h % 256
		htables[tableNum] = append(htables[tableNum], slot{h, poshold.pos})
		poshold.pos += 8 + klen + dlen
		return nil
	}

	closer = func() error {
		var err error
		if err = wb.Flush(); err != nil {
			logger.Panicf("cannot flush %+v: %s", wb, err)
			return err
		}
		//if p, err := w.Seek(0, 1); err != nil || int64(pos) != p {
		//	logger.Panicf("Thought I've written pos=%d bytes, but the actual position is %d! (error? %s)", pos, p, err)
		//}

		// Write hash tables and header.

		// Create and reuse a single hash table.
		pos := poshold.pos
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

		if _, err = w.Write(header); err != nil {
			logger.Panicf("cannot write header: %s", err)
		}
		return err
	}

	return adder, closer, nil
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

	adder, closer, err := MakeFactory(w)
	if err != nil {
		logger.Panicf("cannot create factory: %s", err)
	}
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
		if err = adder(Element{key, data}); err != nil {
			break
		}
	}
	if e := closer(); e != nil {
		if err == nil {
			err = e
		}
	}

	return err
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

// CdbWriter is the CDB writer
type CdbWriter struct {
	w        chan Element
	e        chan error
	tempfh   *os.File
	tempfn   string
	Filename string
}

//NewWriter returns a CdbWriter which writes to the given filename
func NewWriter(cdb_fn string) (*CdbWriter, error) {
	var cw CdbWriter
	var err error
	dir, ofn := filepath.Split(cdb_fn)
	cw.tempfn = dir + "." + ofn
	cw.tempfh, err = os.OpenFile(cw.tempfn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0640)
	if err != nil {
		return nil, err
	}
	_, err = cw.tempfh.Seek(int64(2048), 0)
	if err != nil {
		logger.Panicf("cannot seek to %d of %s: %s", 2048, cw.tempfh, err)
	}
	//logger.Printf("COULD seek to %d of %s", n, cw.tempfh)
	cw.w = make(chan Element, 1)
	cw.e = make(chan error, 1)
	cw.Filename = cdb_fn
	go func() {
		cw.e <- MakeFromChan(cw.tempfh, cw.w)
	}()
	return &cw, nil
}

// PutPair puts a key, val pair to the writer
func (cw *CdbWriter) PutPair(key []byte, val []byte) {
	//logger.Printf("PutPair(%s)", key)
	cw.w <- Element{key, val}
}

// Put puts an Element into the writer
func (cw *CdbWriter) Put(elt Element) {
	cw.w <- elt
}

// Close closes (finalizes) the writer
func (cw *CdbWriter) Close() error {
	//logger.Printf("closing %s", cw.w)
	close(cw.w)
	//logger.Printf("waiting for %s", cw.e)
	err, _ := <-cw.e
	if err != nil {
		return err
	}
	_ = cw.tempfh.Close()
	return os.Rename(cw.tempfn, cw.Filename)
}
