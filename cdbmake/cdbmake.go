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
package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/jbarham/cdb.go"
	"io/ioutil"
	"log"
	"os"
	"path"
)

func exitOnErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, "usage: cdbmake f [ftmp]\n")
	os.Exit(2)
}

func main() {
	var tmp *os.File
	var err error

	flag.Parse()
	args := flag.Args()
	if len(args) == 1 {
		dir, _ := path.Split(args[0])
		tmp, err = ioutil.TempFile(dir, "")
		exitOnErr(err)
	} else if len(args) == 2 {
		tmp, err = os.OpenFile(args[1], os.O_RDWR|os.O_CREATE, 0644)
		exitOnErr(err)
	} else {
		usage()
	}

	fname := args[0]
	tmpname := tmp.Name()

	exitOnErr(cdb.Make(tmp, bufio.NewReader(os.Stdin)))
	exitOnErr(tmp.Sync())
	exitOnErr(tmp.Close())
	exitOnErr(os.Rename(tmpname, fname))
}
