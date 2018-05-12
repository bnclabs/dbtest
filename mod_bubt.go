package main

import "io"
import "os"
import "fmt"
import "sync"
import "time"
import "bytes"
import "strconv"
import "sync/atomic"
import "path/filepath"
import "math/rand"

import "github.com/bnclabs/gostore/api"
import "github.com/bnclabs/gostore/bubt"
import "github.com/bnclabs/gostore/llrb"

func testbubt() error {
	setts := llrb.Defaultsettings()
	mindex := llrb.NewLLRB("dbtest", setts)
	defer mindex.Destroy()

	paths := bubtpaths(options.npaths)

	name := "dbtest"
	rnd := rand.New(rand.NewSource(int64(options.seed)))
	msizes := []int64{4096, 8192, 12288}
	msize := msizes[rnd.Intn(10000)%len(msizes)]
	zsizes := []int64{0, 0, msize, msize * 2, msize * 4}
	zsize := zsizes[rnd.Intn(10000)%len(zsizes)]
	vsizes := []int64{0, 0, zsize, zsize * 2, zsize * 4}
	vsize := vsizes[rnd.Intn(10000)%len(vsizes)]
	mmap := []bool{true, false}[rnd.Intn(10000)%2]
	bt, err := bubt.NewBubt(name, paths, msize, zsize, vsize)
	if err != nil {
		panic(err)
	}

	klen, vlen := int64(options.keylen), int64(options.vallen)
	seed := int64(options.seed)
	iter := makeiterator(klen, vlen, int64(options.load), delmod, mindex)
	md := generatemeta(seed)

	fmsg := "msize: %v zsize:%v vsize: %v mmap:%v mdsize:%v\n"
	fmt.Printf(fmsg, msize, zsize, vsize, mmap, len(md))

	now := time.Now()
	bt.Build(iter, md)
	took := time.Since(now).Round(time.Second)
	fmt.Printf("Took %v to build %v entries\n", took, options.load)
	bt.Close()

	index, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		panic(err)
	}
	defer index.Destroy()
	defer index.Close()

	index.Validate()
	index.Log()

	if index.Count() != int64(options.load) {
		panic(fmt.Errorf("expected %v, got %v", options.load, index.Count()))
	} else if index.ID() != name {
		panic(fmt.Errorf("expected %v, got %v", name, index.ID()))
	}

	fin := make(chan struct{})

	var vwg sync.WaitGroup

	vwg.Add(1)
	go bubtvalidator(mindex, index, &vwg, fin)

	var rwg sync.WaitGroup

	for i := 0; i < options.cpu; i++ {
		go bubtGetter(index, int64(options.load), seed, &rwg)
		go bubtRanger(index, int64(options.load), seed, &rwg)
		rwg.Add(2)
	}
	rwg.Wait()
	close(fin)
	vwg.Wait()

	fmt.Printf("BUBT total indexed %v items\n", index.Count())

	return nil
}

func bubtvalidator(
	mindex *llrb.LLRB, index *bubt.Snapshot,
	wg *sync.WaitGroup, fin chan struct{}) {

	defer wg.Done()

	n := uint64(0)
	validate := func() {
		n++
		now := time.Now()

		mview, view := mindex.View(n), index.View(n)

		mcur, err := mview.OpenCursor(nil)
		if err != nil {
			panic(err)
		}
		cur, err := view.OpenCursor(nil)
		if err != nil {
			panic(err)
		}

		count := 0
		mkey, mvalue, _, mdel, merr := mcur.YNext(false /*fin*/)
		key, value, _, del, err := cur.YNext(false /*fin*/)
		for mkey != nil && key != nil {
			if err == io.EOF && merr == io.EOF {
				break
			} else if err != nil {
				panic(err)
			} else if merr != nil {
				panic(merr)
			} else if bytes.Compare(mkey, key) != 0 {
				panic(fmt.Errorf("%s != %s", mkey, key))
			} else if del != mdel {
				panic(fmt.Errorf("for %s : %v != %v", key, mdel, del))
			} else if del == false && bytes.Compare(mvalue, value) != 0 {
				panic(fmt.Errorf("for %s : %s != %s", key, mvalue, value))
			}
			mkey, mvalue, _, mdel, merr = mcur.YNext(false /*fin*/)
			key, value, _, del, err = cur.YNext(false /*fin*/)
			count++
		}

		if mkey != nil {
			panic(fmt.Errorf("mindex key remaining %s", mkey))
		} else if key != nil {
			panic(fmt.Errorf("index key remaining %s", key))
		}

		mview.Abort()
		view.Abort()

		took := time.Since(now).Round(time.Second)
		fmt.Printf("Took %v to validate index\n", took)
	}

	tick := time.NewTicker(10 * time.Second)
	for {
		<-tick.C
		select {
		case <-fin:
			validate()
			return
		default:
		}
		validate()
	}
}

var bubtgets = []func(x *bubt.Snapshot, k, v []byte) ([]byte, uint64, bool, bool){
	bubtGet1, bubtGet2,
}

func bubtGetter(index *bubt.Snapshot, n, seed int64, wg *sync.WaitGroup) {
	defer wg.Done()

	var ngets, nmisses int64
	var key []byte
	var del bool
	g := Generatereadseq(int64(options.keylen), n, seed)

	rnd := rand.New(rand.NewSource(seed))
	epoch, now, markercount := time.Now(), time.Now(), int64(10000000)
	value := make([]byte, 16)
loop:
	for {
		ngets++
		key = g(key, 0)
		ln := len(bubtgets)
		value, _, del, _ = bubtgets[rnd.Intn(1000000)%ln](index, key, value)
		//fmt.Printf("bubtGetter %q %q %v\n", key, value, del)
		if x, xerr := strconv.Atoi(Bytes2str(key)); xerr != nil {
			panic(xerr)
		} else if (int64(x) % 2) != delmod {
			if del {
				panic(fmt.Errorf("unexpected deleted"))
			}
			comparekeyvalue(key, value, options.vallen)
		} else {
			nmisses++
		}

		if ngm := (ngets + nmisses); ngm%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "bubtGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
		}

		if atomic.AddInt64(&totalreads, 1) > int64(options.reads) {
			break loop
		}
	}
	took := time.Since(epoch).Round(time.Second)
	fmsg := "at exit, bubtGetter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, took)
}

func bubtGet1(
	index *bubt.Snapshot, key, value []byte) ([]byte, uint64, bool, bool) {

	return index.Get(key, value)
}

func bubtGet2(
	index *bubt.Snapshot, key, value []byte) ([]byte, uint64, bool, bool) {

	view := index.View(0x1235)
	value, _, del, ok := view.Get(key, value)
	if ok == true {
		cur, err := view.OpenCursor(key)
		if err != nil {
			panic(err)
		}
		ckey, cdel := cur.Key()
		if bytes.Compare(ckey, key) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, ckey))
		} else if cdel != del {
			panic(fmt.Errorf("%q expected %v, got %v", key, del, cdel))
		}
		cv := cur.Value()
		if del == false && bytes.Compare(cv, value) != 0 {
			panic(fmt.Errorf("%q expected %q, got %q", key, value, cv))
		}
	}
	view.Abort()
	return value, 0, del, ok
}

var bubtrngs = []func(index *bubt.Snapshot, key, val []byte) int64{
	bubtRange1, bubtRange2,
}

func bubtRanger(index *bubt.Snapshot, n, seed int64, wg *sync.WaitGroup) {
	defer wg.Done()

	var nranges int64
	var key []byte
	g := Generatereadseq(int64(options.keylen), n, seed)

	rnd := rand.New(rand.NewSource(seed))
	epoch, value := time.Now(), make([]byte, 16)
loop:
	for {
		key = g(key, 0)
		ln := len(bubtrngs)
		n := bubtrngs[rnd.Intn(1000000)%ln](index, key, value)
		nranges += n

		if atomic.AddInt64(&totalreads, 1) > int64(options.reads) {
			break loop
		}
	}
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf("at exit, bubtRanger %v items in %v\n", nranges, took)
}

func bubtRange1(index *bubt.Snapshot, key, value []byte) (n int64) {
	//fmt.Printf("bubtRange1 %q\n", key)
	view := index.View(0x1236)
	cur, err := view.OpenCursor(key)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 100; i++ {
		key, value, del, err := cur.GetNext()
		if err == io.EOF {
			continue
		} else if err != nil {
			panic(err)
		}
		if x, xerr := strconv.Atoi(Bytes2str(key)); xerr != nil {
			panic(xerr)
		} else if (int64(x)%2) != delmod && del == true {
			panic("unexpected delete")
		} else if del == false {
			comparekeyvalue(key, value, options.vallen)
		}
		n++
	}
	view.Abort()
	return
}

func bubtRange2(index *bubt.Snapshot, key, value []byte) (n int64) {
	//fmt.Printf("bubtRange2 %q\n", key)
	view := index.View(0x1237)
	cur, err := view.OpenCursor(key)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 100; i++ {
		key, value, _, del, err := cur.YNext(false /*fin*/)
		if err == io.EOF {
			continue
		} else if err != nil {
			panic(err)
		}
		if x, xerr := strconv.Atoi(Bytes2str(key)); xerr != nil {
			panic(xerr)
		} else if (int64(x)%2) != delmod && del == true {
			panic("unexpected delete")
		} else if del == false {
			comparekeyvalue(key, value, options.vallen)
		}
		n++
	}
	view.Abort()
	return
}

func makeiterator(
	klen, vlen, entries, mod int64, mindex *llrb.LLRB) api.EntryIterator {

	g := Generateloads(klen, vlen, entries)
	entry := &indexentry{
		key: make([]byte, 0, 16), value: make([]byte, 0, 16),
		seqno: 0, deleted: false, err: nil,
	}

	return func(fin bool) api.IndexEntry {
		opaque := atomic.AddUint64(&seqno, 1)
		entry.key, entry.value = g(entry.key, entry.value, opaque)
		entry.seqno = opaque
		if entry.key != nil {
			x, _ := strconv.Atoi(Bytes2str(entry.key))
			entry.deleted = false
			mindex.Set(entry.key, entry.value, nil)
			if (int64(x) % 2) == mod {
				entry.deleted = true
				mindex.Delete(entry.key, nil, true /*lsm*/)
			}
			//fmsg := "iterate %q %q %v %v\n"
			//fmt.Printf(, entry.key, entry.value, opaque, deleted)
			entry.err = nil
			return entry
		}
		entry.key, entry.value = nil, nil
		entry.seqno, entry.deleted, entry.err = 0, false, io.EOF
		return entry
	}
}

func generatemeta(seed int64) []byte {
	rnd := rand.New(rand.NewSource(seed))
	md := make([]byte, rnd.Intn(20000))
	for i := range md {
		md[i] = byte(97 + rnd.Intn(26))
	}
	return md
}

func bubtpaths(npaths int) []string {
	path, paths := os.TempDir(), []string{}
	if npaths == 0 {
		npaths = (rand.Intn(10000) % 3) + 1
	}
	for i := 0; i < npaths; i++ {
		base := fmt.Sprintf("%v", i+1)
		path := filepath.Join(path, base)
		paths = append(paths, path)
		fmt.Printf("Path %v %q\n", i+1, path)
		if err := os.RemoveAll(path); err != nil {
			panic(err)
		}
		if err := os.MkdirAll(path, 0755); err != nil {
			panic(err)
		}
	}
	return paths
}

type indexentry struct {
	key     []byte
	value   []byte
	seqno   uint64
	deleted bool
	err     error
}

func (entry *indexentry) ID() string {
	return ""
}

func (entry *indexentry) Key() ([]byte, uint64, bool, error) {
	return entry.key, entry.seqno, entry.deleted, entry.err
}

func (entry *indexentry) Value() []byte {
	return entry.value
}

func (entry *indexentry) Valueref() (valuelen uint64, vpos int64) {
	return 0, -1
}
