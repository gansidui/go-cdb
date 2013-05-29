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
package multilevel

import (
	"encoding/binary"
	"fmt"
	"github.com/tgulacsi/go-cdb"
	"io"
	"os"
	"testing"
)

func TestCreateAndCompact(t *testing.T) {
	path := "/tmp/.multilevel"
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("cannot create test directory %s: %s", path, err)
	}
	for i := 0; i < 17; i++ {
		if fn, err := createCdb(path, i*1000, (i+1)*1000); err != nil {
			t.Fatalf("cannot create cdb %s: %s", fn, err)
		}
	}
	m, err := Open(path)
	if err != nil {
		t.Fatalf("error opening %s: %s", path, err)
	}
	check(t, m, []byte{1}, []byte("a"))
	if err = Compact(path, 2); err != nil {
		t.Fatalf("error while compacting %s: %s", path, err)
	}
	t.Logf("merge done successfully")
	check(t, m, []byte{2}, []byte("b"))
}

func check(t *testing.T, m *Multi, exists, notExists []byte) {
	if val, err := m.Data(notExists); err != io.EOF {
		t.Logf("query for %s resulted in %s: %s", notExists, val, err)
		t.Fail()
	}
	if val, err := m.Data(exists); err != nil {
		t.Logf("query for %s resulted in error %s", exists, err)
	} else {
		t.Logf("query for %s: %s", exists, val)
	}
}

func createCdb(path string, from, to int) (string, error) {
	fn := newFn(path)
	fh, err := os.Create(fn)
	if err != nil {
		return fn, err
	}
	defer fh.Close()
	b := make([]byte, 4)
	ch := make(chan cdb.Element, 1)
	go func() {
		for i := from; i < to; i++ {
			binary.LittleEndian.PutUint32(b, uint32(i))
			ch <- cdb.Element{b, []byte(fmt.Sprintf("%d", i))}
		}
		close(ch)
	}()
	err = cdb.MakeFromChan(fh, ch)
	if err != nil {
		return fn, err
	}
	return fn, nil
}
