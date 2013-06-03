package cdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

// Dump reads the cdb-formatted data in r and dumps it as a series of formatted
// records (+klen,dlen:key->data\n) and a final newline to w.
// The output of Dump is suitable as input to Make.
// See http://cr.yp.to/cdb/cdbmake.html for details on the record format.
func Dump(w io.Writer, r io.Reader) (err error) {
	defer func() { // Centralize exception handling.
		if e := recover(); e != nil {
			err = e.(error)
		}
	}()

	rw := &recWriter{bufio.NewWriter(w)}
	err = DumpMap(r, func(elt Element) error {
		rw.writeString(fmt.Sprintf("+%d,%d:%s->%s\n",
			len(elt.Key), len(elt.Data), elt.Key, elt.Data))
		return nil
	})
	rw.writeString("\n")
	err2 := rw.Flush()
	if err != nil {
		return err
	}
	return err2
}

// DumpMap calls work function for every element in the CDB
// if the function returns error, then quits with that error
func DumpMap(r io.Reader, work func(Element) error) error {
	rb := bufio.NewReader(r)
	readNum := makeNumReader(rb)

	eod := readNum()
	// Read rest of header.
	for i := 0; i < 511; i++ {
		readNum()
	}

	pos := headerSize
	//logger.Printf("pos=%d eod=%d", pos, eod)
	for pos < eod {
		klen, dlen := readNum(), readNum()
		//logger.Printf("klen=%d dlen=%d pos=%d eod=%d", klen, dlen, pos, eod)
		if err := work(Element{readn(rb, klen), readn(rb, dlen)}); err != nil {
			return err
		}
		pos += 8 + klen + dlen
	}
	return nil
}

// DumpToChan dumps elements into the given channel, does not close it!
func DumpToChan(c chan<- Element, r io.Reader) error {
	return DumpMap(r, func(elt Element) error { c <- elt; return nil })
}

func makeNumReader(r io.Reader) func() uint32 {
	return func() uint32 {
		return binary.LittleEndian.Uint32(readn(r, 4))
	}
}

func readn(r io.Reader, n uint32) []byte {
	buf := make([]byte, n)
	length, err := io.ReadFull(r, buf)
	if err != nil {
		panic(err)
	}
	//logger.Printf("read %d: %s", length, buf)
	if uint32(length) != n {
		logger.Panicf("wanted to read %d, got %d from %s", n, length, r)
	}
	return buf
}

type recWriter struct {
	*bufio.Writer
}

func (rw *recWriter) writeString(s string) {
	if _, err := rw.WriteString(s); err != nil {
		panic(err)
	}
}

func (rw *recWriter) copyn(r io.Reader, n uint32) {
	if _, err := io.CopyN(rw, r, int64(n)); err != nil {
		panic(err)
	}
}
