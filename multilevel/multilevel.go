package multilevel

import (
	"fmt"
	"github.com/tgulacsi/go-cdb"
	"github.com/tgulacsi/go-locking"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// CdbSizeThreshold is 1Gb
const CdbSizeThreshold = 1 << 30

// Multi is a type for the opened cdbs
type Multi struct {
	channels [](chan Question)
	mtx      sync.Mutex
}

// Question is the key to query for and the channel to answer on
type Question struct {
	Key    []byte
	Answch chan Result
}

// Result is an answer can be nice data or io.EOF if not found (or other error)
type Result struct {
	Data []byte
	Err  error
}

var openedDirs = make(map[string]*Multi, 1)

// Open opens the path
func Open(path string) (*Multi, error) {
	files, err := listDir(path, 0, isCdb)
	if err != nil {
		return nil, err
	}
	if old, ok := openedDirs[path]; ok {
		delete(openedDirs, path)
		log.Printf("reopening %s", path)
		old.Close()
	}
	var askch chan Question
	cdbs := &Multi{channels: make([](chan Question), 0, len(files))}
	for _, fi := range files {
		fn := filepath.Join(path, fi.Name())
		if askch, err = startOracle(fn); err != nil {
			return nil, fmt.Errorf("error opening %s: %s", fn, err)
		}
		cdbs.channels = append(cdbs.channels, askch)
	}
	openedDirs[path] = cdbs
	return cdbs, nil
}

func startOracle(fn string) (chan Question, error) {
	ch, err := cdb.Open(fn)
	if err != nil {
		return nil, fmt.Errorf("error opening %s: %s", fn, err)
	}
	askch := make(chan Question)
	go func(ch *cdb.Cdb, askch <-chan Question) {
		defer ch.Close()
		var data []byte
		var err error
		// log.Printf("starting waiting for questions for %s", askch)
		for qry := range askch {
			data, err = ch.Data(qry.Key)
			qry.Answch <- Result{Data: data, Err: err}
		}
		// log.Printf("closing %s", ch)
	}(ch, askch)
	return askch, nil
}

// Close closes the Multi
func (m *Multi) Close() {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	for _, ch := range m.channels {
		if ch != nil {
			// log.Printf("closing channel %s", ch)
			close(ch)
		}
	}
	m.channels = m.channels[:0]
}

// Data returns the data for the key
func (m *Multi) Data(key []byte) ([]byte, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	results := make(chan Result, 1)
	qry := Question{Key: key, Answch: results}
	n := len(m.channels)
	for _, ch := range m.channels {
		// log.Printf("sending on %s", ch)
		ch <- qry
	}
	for i := 0; i < n; i++ {
		res := <-results
		if res.Err == nil {
			return res.Data, nil
		}
		if res.Err != io.EOF {
			return nil, res.Err
		}
	}
	return nil, io.EOF
}

func isCdb(fi os.FileInfo) bool {
	return strings.HasSuffix(fi.Name(), ".cdb")
}

// Compact compacts path directory, if number of cdb files greater than threshold
func Compact(path string, threshold int) error {
    {
	locks, err := locking.FLockDirs(path)
    if err != nil {
		return err
	}
		defer locks.Unlock()
	}
	files, err := listDir(path, 'S', isCdb)
	if err != nil {
		return fmt.Errorf("cannot list dir %s: %s", path, err)
	}
	if len(files) < threshold {
		return nil
	}
	size := int64(0)
	bucket := make([]string, 0, 16)
	for _, fi := range files {
		fs := fi.Size()
		if fs+size > CdbSizeThreshold {
			if err = MergeCdbs(newFn(path), bucket...); err != nil {
				return fmt.Errorf("error merging cdbs (%s): %s", strings.Join(bucket, ", "), err)
			}
			size = 0
			bucket = bucket[:0]
		}
		bucket = append(bucket, filepath.Join(path, fi.Name()))
	}
	err = nil
	if len(bucket) > 1 {
		if err = MergeCdbs(newFn(path), bucket...); err != nil {
			return err
		}
	}
	if _, ok := openedDirs[path]; ok {
		_, err = Open(path)
	}
	return err
}

// MergeCdbs merges the cdbs, dumping filenames to newfn
func MergeCdbs(newfn string, filenames ...string) error {
	fh, err := os.Create(newfn)
	if err != nil {
		return err
	}
	defer fh.Close()
	donech := make(chan error, len(filenames))
	eltch := make(chan cdb.Element, len(filenames)|16)
	errch := make(chan error, 1)
	go func(w io.WriteSeeker) {
		errch <- cdb.MakeFromChan(w, eltch)
	}(fh)
	for _, fn := range filenames {
		ifh, err := os.Open(fn)
		if err != nil {
			return fmt.Errorf("cannot open %s: %s", fn, err)
		}
		go func(r io.Reader) {
			donech <- cdb.DumpToChan(eltch, r)
		}(ifh)
	}
	for i := 0; i < len(filenames); i++ {
		select {
		case err = <-errch:
			if err != nil {
				return fmt.Errorf("error making %s: %s", fh, err)
			}
			i--
		case err = <-donech:
			if err != nil {
				return fmt.Errorf("error dumping: %s", err)
				i--
			}
		}
	}
	close(eltch)
	err = <-errch
	if err != nil {
		return fmt.Errorf("error while making %s: %s", fh, err)
	}
	for _, fn := range filenames {
		os.Remove(fn)
	}
	return nil
}

func newFn(path string) string {
	return filepath.Join(path, fmt.Sprintf("%d.cdb", time.Now().UnixNano()))
}

type fileInfos []os.FileInfo

func listDir(path string, orderBy byte, filter func(os.FileInfo) bool) (files fileInfos, err error) {
	dh, e := os.Open(path)
	if e != nil {
		err = fmt.Errorf("cannot open dir %s: %s", path, e)
		return
	}
	defer dh.Close()

	files = make(fileInfos, 0, 16)
	for {
		flist, e := dh.Readdir(1024)
		if e == io.EOF {
			break
		} else if e != nil {
			err = fmt.Errorf("erro listing dir %s: %s", dh, e)
			return
		}
		for _, fi := range flist {
			if filter(fi) {
				files = append(files, fi)
			}
		}
	}

	switch orderBy {
	case 's':
		sort.Sort(growingSize(files))
	case 'S':
		sort.Sort(shrinkingSize(files))
	}
	return
}

// for sorting by growing size
type growingSize fileInfos

func (gfi growingSize) Len() int           { return len(gfi) }
func (gfi growingSize) Swap(i, j int)      { gfi[i], gfi[j] = gfi[j], gfi[i] }
func (gfi growingSize) Less(i, j int) bool { return gfi[i].Size() < gfi[j].Size() }

// for sorting by shrinking size
type shrinkingSize fileInfos

func (gfi shrinkingSize) Len() int           { return len(gfi) }
func (gfi shrinkingSize) Swap(i, j int)      { gfi[i], gfi[j] = gfi[j], gfi[i] }
func (sfi shrinkingSize) Less(i, j int) bool { return sfi[i].Size() > sfi[j].Size() }
