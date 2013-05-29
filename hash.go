/*
   Copyright 2013 Tamás Gulácsi

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package cdb

import "hash"

const (
	start = 5381 // Initial cdb checksum value.
)

// digest represents the partial evaluation of a checksum.
type digest struct {
	h uint32
}

func (d *digest) Reset() { d.h = start }

// New returns a new hash computing the cdb checksum.
func cdbHash() hash.Hash32 {
	d := new(digest)
	d.Reset()
	return d
}

func (d *digest) Size() int { return 4 }

func update(h uint32, p []byte) uint32 {
	for i := 0; i < len(p); i++ {
		h = ((h << 5) + h) ^ uint32(p[i])
	}
	return h
}

func (d *digest) Write(p []byte) (int, error) {
	d.h = update(d.h, p)
	return len(p), nil
}

func (d *digest) Sum32() uint32 { return d.h }

func (d *digest) Sum(in []byte) []byte {
	s := d.Sum32()
	in = append(in, byte(s>>24))
	in = append(in, byte(s>>16))
	in = append(in, byte(s>>8))
	in = append(in, byte(s))
	return in
}

func (d *digest) BlockSize() int { return 1 }

func checksum(data []byte) uint32 { return update(start, data) }
