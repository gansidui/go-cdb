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

	c := make(chan Element, 1)
	go DumpToChan(c, r)
	rw := &recWriter{bufio.NewWriter(w)}
	/*
	rb := bufio.NewReader(r)
	readNum := makeNumReader(rb)

	eod := readNum()
	// Read rest of header.
	for i := 0; i < 511; i++ {
		readNum()
	}

	pos := headerSize
	for pos < eod {
		klen, dlen := readNum(), readNum()
		rw.writeString(fmt.Sprintf("+%d,%d:", klen, dlen))
		rw.copyn(rb, klen)
		rw.writeString("->")
		rw.copyn(rb, dlen)
		rw.writeString("\n")
		pos += 8 + klen + dlen
	}
	*/
	for {
		elt, ok := <-c
		//logger.Printf("elt=%+v ok=%+v", elt, ok)
		if !ok {
			break
		}
		rw.writeString(fmt.Sprintf("+%d,%d:%s->%s\n",
			len(elt.Key), len(elt.Data), elt.Key, elt.Data))
	}
	rw.writeString("\n")

	return rw.Flush()
}

func makeNumReader(r io.Reader) func() uint32 {
	buf := make([]byte, 4)
	return func() uint32 {
		if _, err := r.Read(buf); err != nil {
			panic(err)
		}
		return binary.LittleEndian.Uint32(buf)
	}
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

func DumpToChan(c chan<- Element, r io.Reader) {
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
		c <- Element{readn(rb, klen), readn(rb, dlen)}
		pos += 8 + klen + dlen
	}
	close(c)
}

func readn(r io.Reader, n uint32) []byte {
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	if err != nil {
		panic(err)
	}
	return b
}