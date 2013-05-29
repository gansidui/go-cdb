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

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

type rec struct {
	key    string
	values []string
}

var records []rec

var data []byte // set by init()

func TestCdb(t *testing.T) {
	tmp, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatalf("Failed to create temp file: %s", err)
	}

	defer os.Remove(tmp.Name())

	// Test Make
	err = Make(tmp, bytes.NewBuffer(data))
	if err != nil {
		t.Fatalf("Make failed: %s", err)
	}

	// Test reading records
	c, err := Open(tmp.Name())
	if err != nil {
		t.Fatalf("Error opening %s: %s", tmp.Name(), err)
	}
	for _, rec := range records {
		key := []byte(rec.key)
		values := rec.values

		v, err := c.Data(key)
		if err != nil {
			t.Fatalf("Record read failed: %s", err)
		}

		if !bytes.Equal(v, []byte(values[0])) {
			t.Fatal("Incorrect value returned")
		}

		c.FindStart()
		for _, value := range values {
			sr, err := c.FindNext(key)
			if err != nil {
				t.Fatalf("Record read failed: %s", err)
			}

			data := make([]byte, sr.Size())
			_, err = sr.Read(data)
			if err != nil {
				t.Fatalf("Record read failed: %s", err)
			}

			if !bytes.Equal(data, []byte(value)) {
				t.Fatal("value mismatch")
			}
		}
		// Read all values, so should get EOF
		_, err = c.FindNext(key)
		if err != io.EOF {
			t.Fatalf("Expected EOF, got %s", err)
		}
	}

	// Test Dump
	if _, err = tmp.Seek(0, 0); err != nil {
		t.Fatal(err)
	}

	buf := bytes.NewBuffer(nil)
	err = Dump(buf, tmp)
	if err != nil {
		t.Fatalf("Dump failed: %s", err)
	}
	// fmt.Printf("data=%s\ndumped=%s", data, buf.Bytes())

	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("Dump round-trip failed")
	}
}

func init() {
	names := map[int]string{1: "one", 2: "two", 3: "three", 4: "four", 5: "five", 6: "six", 7: "seven"}
	m := len(names)
	loads := make(map[int][]string, m)
	for i := 1; i < m+1; i++ {
		load := make([]string, i)
		for j := 0; j < i; j++ {
			load[j] = fmt.Sprintf("%d", i)
		}
		loads[i] = load
	}
	// fmt.Printf("names=%s\n", names)
	// fmt.Printf("loads=%s\n", loads)
	n := m * 13000
	// n := 13
	records := make([]rec, n)
	for i := 0; i < n; i++ {
		j := (i % (m - 1)) + 1
		records[i] = rec{fmt.Sprintf("%s-%d", names[j], i), loads[j]}
	}
	// fmt.Printf("records=%s\n", records)
	b := bytes.NewBuffer(nil)
	for _, rec := range records {
		key := rec.key
		for _, value := range rec.values {
			b.WriteString(fmt.Sprintf("+%d,%d:%s->%s\n", len(key), len(value), key, value))
		}
	}
	b.WriteByte('\n')
	data = b.Bytes()
}
