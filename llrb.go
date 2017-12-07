package main

import "io"
import "fmt"
import "sync"
import "time"
import "bytes"
import "runtime"
import "strconv"
import "sync/atomic"
import "math/rand"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/llrb"

func testllrb() error {
	setts := llrb.Defaultsettings()
	index := llrb.NewLLRB("dbtest", setts)
	defer index.Destroy()

	seedl, seedc := int64(options.seed), int64(options.seed)+100
	fmt.Printf("Seed for load: %v, for ops: %v\n", seedl, seedc)
	if err := llrbLoad(index, seedl); err != nil {
		return err
	}

	var wwg, rwg sync.WaitGroup
	fin := make(chan struct{})

	go llrbvalidator(index, true /*log*/, &rwg, fin)
	rwg.Add(1)

	// writer routines
	n := atomic.LoadInt64(&numentries)
	go llrbCreater(index, n, seedc, &wwg)
	go llrbUpdater(index, n, seedl, seedc, &wwg)
	go llrbDeleter(index, n, seedl, seedc, &wwg)
	wwg.Add(3)
	// reader routines
	for i := 0; i < options.cpu; i++ {
		go llrbGetter(index, n, seedl, seedc, fin, &rwg)
		go llrbRanger(index, n, seedl, seedc, fin, &rwg)
		rwg.Add(2)
	}
	wwg.Wait()
	close(fin)
	rwg.Wait()

	count, n := index.Count(), atomic.LoadInt64(&numentries)
	fmt.Printf("LLRB write conflicts %v\n", conflicts)
	fmt.Printf("LLRB total indexed %v items, expected %v\n", count, n)

	return nil
}

func llrbvalidator(
	index *llrb.LLRB, log bool, wg *sync.WaitGroup, fin chan struct{}) {

	defer wg.Done()

	tick := time.NewTicker(10 * time.Second)
	for {
		<-tick.C
		select {
		case <-fin:
			return
		default:
		}

		if log {
			index.Log()
		}

		now := time.Now()
		index.Validate()
		fmt.Printf("Took %v to validate index\n", time.Since(now))
	}
}

func llrbLoad(index *llrb.LLRB, seedl int64) error {
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateloadr(klen, vlen, int64(options.load), int64(seedl))

	key, value := make([]byte, 16), make([]byte, 16)
	now, oldvalue := time.Now(), make([]byte, 16)
	for key, value = g(key, value); key != nil; key, value = g(key, value) {
		//fmt.Printf("load %q\n", key)
		oldvalue, _ := index.Set(key, value, oldvalue)
		if len(oldvalue) > 0 {
			panic(fmt.Errorf("unexpected %q", oldvalue))
		}
	}
	atomic.AddInt64(&numentries, int64(options.load))
	atomic.AddInt64(&totalwrites, int64(options.load))

	fmt.Printf("Loaded %v items in %v\n", options.load, time.Since(now))
	return nil
}

var llrbsets = []func(index *llrb.LLRB, k, v, ov []byte) (uint64, []byte){
	llrbSet1, llrbSet2, llrbSet3, llrbSet4,
}

func llrbCreater(index *llrb.LLRB, n, seedc int64, wg *sync.WaitGroup) {
	defer wg.Done()

	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generatecreate(klen, vlen, n, seedc)

	key, value := make([]byte, 16), make([]byte, 16)
	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		key, value = g(key, value)
		setidx := rnd.Intn(1000000) % 4
		refcas, _ := llrbsets[setidx](index, key, value, oldvalue)
		oldvalue, cas, del, ok := index.Get(key, oldvalue)
		if ok == false {
			panic("unexpected false")
		} else if del == true {
			panic("unexpected delete")
		} else if refcas > 0 && cas != refcas {
			panic(fmt.Errorf("expected %v, got %v", refcas, cas))
		} else if bytes.Compare(key, oldvalue) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
		}

		atomic.AddInt64(&numentries, 1)
		atomic.AddInt64(&totalwrites, 1)
		if nc := atomic.AddInt64(&ncreates, 1); nc%markercount == 0 {
			count := index.Count()
			x, y := time.Since(now).Round(time.Second), time.Since(epoch)
			fmsg := "llrbCreated {%v items in %v} {%v items in %v} count:%v\n"
			fmt.Printf(fmsg, markercount, x, nc, y.Round(time.Second), count)
			now = time.Now()
		}
		runtime.Gosched()
	}
	fmsg := "at exit, llrbCreated %v items in %v\n"
	fmt.Printf(fmsg, atomic.LoadInt64(&ncreates), time.Since(epoch))
}

func vllrbupdater(
	key, oldvalue []byte, refcas, cas uint64, i int, del, ok bool) string {

	var err error
	if ok == false {
		err = fmt.Errorf("unexpected false")
	} else if del == true {
		err = fmt.Errorf("unexpected delete")
	} else if refcas > 0 && cas != refcas {
		err = fmt.Errorf("expected %v, got %v", refcas, cas)
	} else if bytes.Compare(key, oldvalue) != 0 {
		err = fmt.Errorf("expected %q, got %q", key, oldvalue)
	}
	if err != nil && i == 0 {
		panic(err)
	} else if err != nil {
		atomic.AddInt64(&conflicts, 1)
		return "repeat"
	}
	return "ok"
}

func llrbUpdater(index *llrb.LLRB, n, seedl, seedc int64, wg *sync.WaitGroup) {
	defer wg.Done()

	var nupdates int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateupdate(klen, vlen, n, seedl, seedc, -1)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		key, value = g(key, value)
		setidx := rnd.Intn(1000000) % 4
		for i := 2; i >= 0; i-- {
			refcas, oldvalue := llrbsets[setidx](index, key, value, oldvalue)
			if len(oldvalue) == 0 {
				atomic.AddInt64(&numentries, 1)
			}
			oldvalue, cas, del, ok := index.Get(key, oldvalue)
			if vllrbupdater(key, oldvalue, refcas, cas, i, del, ok) == "ok" {
				break
			}
		}

		atomic.AddInt64(&totalwrites, 1)
		if nupdates = nupdates + 1; nupdates%markercount == 0 {
			count := index.Count()
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "llrbUpdated {%v items in %v} {%v items in %v} count:%v\n"
			fmt.Printf(fmsg, markercount, x, nupdates, y, count)
			now = time.Now()
		}
		runtime.Gosched()
	}
	fmsg := "at exit, llrbUpdated %v items in %v\n"
	fmt.Printf(fmsg, nupdates, time.Since(epoch))
}

func llrbSet1(index *llrb.LLRB, key, value, oldvalue []byte) (uint64, []byte) {
	oldvalue, cas := index.Set(key, value, oldvalue)
	//fmt.Printf("update1 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	}
	return cas, oldvalue
}

func llrbverifyset2(err error, i int, key, oldvalue []byte) string {
	if err != nil {
	} else if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		err = fmt.Errorf("expected %q, got %q", key, oldvalue)
	}
	if err != nil && i == 0 {
		panic(err)
	} else if err != nil {
		atomic.AddInt64(&conflicts, 1)
		return "repeat"
	}
	return "ok"
}

func llrbSet2(index *llrb.LLRB, key, value, oldvalue []byte) (uint64, []byte) {
	for i := 2; i >= 0; i-- {
		oldvalue, oldcas, deleted, ok := index.Get(key, oldvalue)
		if deleted || ok == false {
			oldcas = 0
		} else if oldcas == 0 {
			panic(fmt.Errorf("unexpected %v", oldcas))
		} else if bytes.Compare(key, oldvalue) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
		}
		oldvalue, cas, err := index.SetCAS(key, value, oldvalue, oldcas)
		//fmt.Printf("update2 %q %q %q \n", key, value, oldvalue)
		if llrbverifyset2(err, i, key, oldvalue) == "ok" {
			return cas, oldvalue
		}
	}
	panic("unreachable code")
}

func llrbSet3(index *llrb.LLRB, key, value, oldvalue []byte) (uint64, []byte) {
	txn := index.BeginTxn(0xC0FFEE)
	oldvalue = txn.Set(key, value, oldvalue)
	//fmt.Printf("update3 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	}
	if err := txn.Commit(); err != nil {
		panic(err)
	}
	return 0, oldvalue
}

func llrbSet4(index *llrb.LLRB, key, value, oldvalue []byte) (uint64, []byte) {
	txn := index.BeginTxn(0xC0FFEE)
	cur, err := txn.OpenCursor(key)
	if err != nil {
		panic(err)
	}
	oldvalue = cur.Set(key, value, oldvalue)
	//fmt.Printf("update4 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	}
	if err := txn.Commit(); err != nil {
		panic(err)
	}
	return 0, oldvalue
}

var llrbdels = []func(*llrb.LLRB, []byte, []byte, bool) (uint64, bool){
	llrbDel1, llrbDel2, llrbDel3, llrbDel4,
}

func vllrbdel(
	index interface{}, key, oldvalue []byte, refcas uint64,
	i int, lsm, ok bool) string {

	var err error
	var cur api.Cursor
	if lsm == false {
		if ok == true {
			err = fmt.Errorf("unexpected true when lsm is false")
		} else if len(oldvalue) > 0 {
			err = fmt.Errorf("unexpected %q when lsm is false", oldvalue)
		}

	} else {
		var view api.Transactor
		switch idx := index.(type) {
		case *llrb.LLRB:
			view = idx.View(0x1234)
		case *llrb.MVCC:
			view = idx.View(0x1234)
		}

		cur, err = view.OpenCursor(key)
		if err == nil {
			_, oldvalue, cas, del, err := cur.YNext(false)

			if err != nil {
			} else if del == false {
				err = fmt.Errorf("expected delete")
			} else if refcas > 0 && cas != refcas {
				err = fmt.Errorf("expected %v, got %v", refcas, cas)
			}
			if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
				err = fmt.Errorf("expected %q, got %q", key, oldvalue)
			}
		}
		view.Abort()
	}

	if err != nil && i == 0 {
		panic(err)
	} else if err != nil {
		atomic.AddInt64(&conflicts, 1)
		return "repeat"
	}
	return "ok"
}

func llrbDeleter(index *llrb.LLRB, n, seedl, seedc int64, wg *sync.WaitGroup) {
	defer wg.Done()

	var ndeletes, xdeletes int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generatedelete(klen, vlen, n, seedl, seedc, delmod)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	lsmmap := map[int]bool{0: true, 1: false}
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		key, value = g(key, value)
		//fmt.Printf("delete %q\n", key)
		ln := len(llrbdels)
		delidx, lsm := rnd.Intn(1000000)%ln, lsmmap[rnd.Intn(1000000)%2]
		if lsm {
			delidx = delidx % 2
		}
		for i := 2; i >= 0; i-- {
			refcas, ok1 := llrbdels[delidx](index, key, value, lsm)
			oldvalue, _, _, ok2 := index.Get(key, oldvalue)
			if vllrbdel(index, key, oldvalue, refcas, i, lsm, ok2) == "ok" {
				if ok1 == false && lsm == false {
					xdeletes++
				} else if ok1 == false && lsm == true {
					ndeletes++
					atomic.AddInt64(&totalwrites, 1)
				} else if ok1 == true && lsm == false {
					ndeletes++
					atomic.AddInt64(&totalwrites, 1)
					atomic.AddInt64(&numentries, -1)
				} else if ok1 == true && lsm == true {
					ndeletes++
					atomic.AddInt64(&totalwrites, 1)
				}
				break
			}
		}

		if x := ndeletes + xdeletes; x > 0 && (x%markercount) == 0 {
			count := index.Count()
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "llrbDeleted {%v items %v} {%v:%v items in %v} count:%v\n"
			fmt.Printf(fmsg, markercount, x, ndeletes, xdeletes, y, count)
			now = time.Now()
		}
		runtime.Gosched()
	}
	fmsg := "at exit, llrbDeleter %v:%v items in %v\n"
	fmt.Printf(fmsg, ndeletes, xdeletes, time.Since(epoch))
}

func llrbDel1(index *llrb.LLRB, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	oldvalue, cas := index.Delete(key, oldvalue, lsm)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %s", key, oldvalue))
	} else if len(oldvalue) > 0 {
		ok = true
	}
	return cas, ok
}

func llrbDel2(index *llrb.LLRB, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	txn := index.BeginTxn(0xC0FFEE)
	oldvalue = txn.Delete(key, oldvalue, lsm)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	} else if len(oldvalue) > 0 {
		ok = true
	}
	if err := txn.Commit(); err != nil {
		panic(err)
	}
	return 0, ok
}

func llrbDel3(index *llrb.LLRB, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	txn := index.BeginTxn(0xC0FFEE)
	cur, err := txn.OpenCursor(key)
	if err != nil {
		panic(err)
	}
	oldvalue = cur.Delete(key, oldvalue, lsm)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	} else if len(oldvalue) > 0 {
		ok = true
	}
	if err := txn.Commit(); err != nil {
		panic(err)
	}
	return 0, ok
}

func llrbDel4(index *llrb.LLRB, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	txn := index.BeginTxn(0xC0FFEE)
	cur, err := txn.OpenCursor(key)
	if err != nil {
		panic(err)
	}
	curkey, _ := cur.Key()
	if bytes.Compare(key, curkey) == 0 {
		cur.Delcursor(lsm)
		ok = true
	}
	if err := txn.Commit(); err != nil {
		panic(err)
	}
	return 0, ok
}

var llrbgets = []func(x *llrb.LLRB, k, v []byte) ([]byte, uint64, bool, bool){
	llrbGet1, llrbGet2, llrbGet3,
}

func llrbGetter(
	index *llrb.LLRB, n, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	defer wg.Done()

	var ngets, nmisses int64
	var key []byte
	var del bool
	g := Generateread(int64(options.keylen), n, seedl, seedc)

	rnd := rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(10000000)
	value := make([]byte, 16)
loop:
	for {
		ngets++
		time.Sleep(10 * time.Microsecond)
		key = g(key, atomic.LoadInt64(&ncreates))
		ln := len(llrbgets)
		value, _, del, _ = llrbgets[rnd.Intn(1000000)%ln](index, key, value)
		if x, xerr := strconv.Atoi(Bytes2str(key)); xerr != nil {
			panic(xerr)
		} else if (int64(x) % 2) != delmod {
			if del {
				panic(fmt.Errorf("unexpected deleted"))
			} else if bytes.Compare(key, value) != 0 {
				panic(fmt.Errorf("expected %q, got %q", key, value))
			}
		} else {
			nmisses++
		}

		select {
		case <-fin:
			break loop
		default:
		}
		if ngm := ngets + nmisses; ngm%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "llrbGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
		}
	}
	duration := time.Since(epoch)
	<-fin
	fmsg := "at exit, llrbGetter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, duration)
}

func llrbGet1(
	index *llrb.LLRB, key, value []byte) ([]byte, uint64, bool, bool) {

	//fmt.Printf("llrbGet1 %q\n", key)
	//defer fmt.Printf("llrbGet1-abort %q\n", key)
	return index.Get(key, value)
}

func llrbGet2(
	index *llrb.LLRB, key, value []byte) ([]byte, uint64, bool, bool) {

	//fmt.Printf("llrbGet2\n")
	txn := index.BeginTxn(0xC0FFEE)
	value, _, del, ok := txn.Get(key, value)
	if ok == true {
		cur, err := txn.OpenCursor(key)
		if err != nil {
			panic(err)
		}
		if ckey, cdel := cur.Key(); cdel != del {
			panic(fmt.Errorf("expected %v, got %v", del, cdel))
		} else if bytes.Compare(ckey, key) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, ckey))
		} else if cvalue := cur.Value(); bytes.Compare(cvalue, value) != 0 {
			panic(fmt.Errorf("expected %q, got %q", value, cvalue))
		}
	}
	//fmt.Printf("llrbGet2-abort\n")
	txn.Abort()
	return value, 0, del, ok
}

func llrbGet3(
	index *llrb.LLRB, key, value []byte) ([]byte, uint64, bool, bool) {

	view := index.View(0x1235)
	value, _, del, ok := view.Get(key, value)
	if ok == true {
		cur, err := view.OpenCursor(key)
		if err != nil {
			panic(err)
		}
		if ckey, cdel := cur.Key(); cdel != del {
			panic(fmt.Errorf("expected %v, got %v", del, cdel))
		} else if bytes.Compare(ckey, key) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, ckey))
		} else if cvalue := cur.Value(); bytes.Compare(cvalue, value) != 0 {
			panic(fmt.Errorf("expected %q, got %q", value, cvalue))
		}
	}
	view.Abort()
	return value, 0, del, ok
}

var llrbrngs = []func(index *llrb.LLRB, key, val []byte) int64{
	llrbRange1, llrbRange2, llrbRange3, llrbRange4,
}

func llrbRanger(
	index *llrb.LLRB, n, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	defer wg.Done()

	var nranges int64
	var key []byte
	g := Generateread(int64(options.keylen), n, seedl, seedc)

	rnd := rand.New(rand.NewSource(seedc))
	epoch, value := time.Now(), make([]byte, 16)
loop:
	for {
		time.Sleep(10 * time.Microsecond)
		key = g(key, atomic.LoadInt64(&ncreates))
		ln := len(llrbrngs)
		n := llrbrngs[rnd.Intn(1000000)%ln](index, key, value)
		nranges += n
		select {
		case <-fin:
			break loop
		default:
		}
	}
	duration := time.Since(epoch)
	<-fin
	fmt.Printf("at exit, llrbRanger %v items in %v\n", nranges, duration)
}

func llrbRange1(index *llrb.LLRB, key, value []byte) (n int64) {
	//fmt.Printf("llrbRange1 %q\n", key)
	txn := index.BeginTxn(0xC0FFEE)
	cur, err := txn.OpenCursor(key)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 100; i++ {
		key, value, del, err := cur.GetNext()
		if err == io.EOF {
		} else if err != nil {
			panic(err)
		} else if x, xerr := strconv.Atoi(Bytes2str(key)); xerr != nil {
			panic(xerr)
		} else if (int64(x)%2) != delmod && del == true {
			panic("unexpected delete")
		} else if del == false && bytes.Compare(key, value) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, value))
		}
		n++
	}
	txn.Abort()
	return
}

func llrbRange2(index *llrb.LLRB, key, value []byte) (n int64) {
	txn := index.BeginTxn(0xC0FFEE)
	cur, err := txn.OpenCursor(key)
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
		} else if del == false && bytes.Compare(key, value) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, value))
		}
		n++
	}
	txn.Abort()
	return
}

func llrbRange3(index *llrb.LLRB, key, value []byte) (n int64) {
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
		} else if del == false && bytes.Compare(key, value) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, value))
		}
		n++
	}
	view.Abort()
	return
}

func llrbRange4(index *llrb.LLRB, key, value []byte) (n int64) {
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
		} else if del == false && bytes.Compare(key, value) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, value))
		}
		n++
	}
	view.Abort()
	return
}
