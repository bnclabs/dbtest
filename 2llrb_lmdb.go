package main

import "io"
import "os"
import "fmt"
import "sync"
import "time"
import "bytes"
import "unsafe"
import "runtime"
import "strings"
import "strconv"
import "sync/atomic"
import "math/rand"

import "github.com/bnclabs/golog"
import s "github.com/bnclabs/gosettings"
import "github.com/bnclabs/gostore/api"
import "github.com/bnclabs/gostore/llrb"
import "github.com/bmatsuo/lmdb-go/lmdb"

// manage global llrb-index and older copy of the llrb-index.
var llrbold *llrb.LLRB
var llrbindex unsafe.Pointer // *llrb.LLRB
func loadllrbindex() *llrb.LLRB {
	return (*llrb.LLRB)(atomic.LoadPointer(&llrbindex))
}
func storellrbindex(index *llrb.LLRB) {
	atomic.StorePointer(&llrbindex, unsafe.Pointer(index))
}

func testllrblmdb() error {
	seedl, seedc := int64(options.seed), int64(options.seed)+100
	fmt.Printf("Seed for load: %v, for ops: %v\n\n", seedl, seedc)

	lmdbpath := makelmdbpath()
	defer func() {
		if err := os.RemoveAll(lmdbpath); err != nil {
			log.Errorf("%v", err)
		}
	}()

	// new lmdb instance.
	lmdbenv, lmdbdbi, err := initlmdb(lmdbpath, lmdb.NoSync|lmdb.NoMetaSync)
	if err != nil {
		return err
	}
	defer lmdbenv.Close()
	// new llrb instance.
	llrbname, llrbsetts := "dbtest", llrb.Defaultsettings()
	index := llrb.NewLLRB(llrbname, llrbsetts)
	storellrbindex(index)

	// load index and reference with initial data.
	dollrblmdbload(lmdbenv, lmdbdbi)
	// test index and reference read / write
	dollrblmdbrw(llrbname, llrbsetts, index, lmdbenv, lmdbdbi)

	if llrbold != nil {
		llrbold.Close()
		llrbold.Destroy()
	}
	if index := loadllrbindex(); index != nil {
		index.Close()
		index.Destroy()
	}

	count, n := index.Count(), atomic.LoadInt64(&numentries)
	fmt.Printf("LLRB write conflicts %v\n", conflicts)
	fmt.Printf("LLRB total indexed %v items, expected %v\n", count, n)

	return nil
}

func dollrblmdbload(lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {
	seedl := int64(options.seed)
	if err := llrbLmdbLoad(seedl); err != nil {
		panic(err)
	}
	seqno = 0
	if err := lmdbLoad(lmdbenv, lmdbdbi, seedl); err != nil {
		panic(err)
	}
	atomic.AddInt64(&numentries, -int64(options.load))
	atomic.AddInt64(&totalwrites, -int64(options.load))
}

func dollrblmdbrw(
	llrbname string, llrbsetts s.Settings,
	index *llrb.LLRB, lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {

	seedl, seedc := int64(options.seed), int64(options.seed)+100

	var wwg, rwg sync.WaitGroup
	fin := make(chan struct{})

	// validator
	go llrblmdbvalidator(
		lmdbenv, lmdbdbi, true, seedl, llrbname, llrbsetts, &rwg, fin,
	)
	rwg.Add(1)
	// writer routines
	n := atomic.LoadInt64(&numentries)
	go llrbLmdbCreater(lmdbenv, lmdbdbi, n, seedc, &wwg)
	go llrbLmdbUpdater(lmdbenv, lmdbdbi, n, seedl, seedc, &wwg)
	go llrbLmdbDeleter(lmdbenv, lmdbdbi, n, seedl, seedc, &wwg)
	wwg.Add(3)
	// reader routines
	for i := 0; i < options.cpu; i++ {
		go llrbLmdbGetter(lmdbenv, lmdbdbi, n, seedl, seedc, fin, &rwg)
		go llrbLmdbRanger(n, seedl, seedc, fin, &rwg)
		rwg.Add(2)
	}
	wwg.Wait()
	close(fin)
	rwg.Wait()
}

var llrbrw sync.RWMutex

func llrblmdbvalidator(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI, log bool, seedl int64,
	llrbname string, llrbsetts s.Settings,
	wg *sync.WaitGroup, fin chan struct{}) {

	defer wg.Done()

	rnd := rand.New(rand.NewSource(seedl))

	loadllrb := func(index *llrb.LLRB) *llrb.LLRB {
		now := time.Now()
		iter := index.Scan()
		newindex := llrb.LoadLLRB(llrbname, llrbsetts, iter)
		iter(true /*fin*/)
		storellrbindex(newindex)
		thisseqno := index.Getseqno()
		newindex.Setseqno(thisseqno)
		fmsg := "Took %v to LoadLLRB index @ %v \n\n"
		took := time.Since(now).Round(time.Second)
		fmt.Printf(fmsg, took, thisseqno)

		if llrbold != nil {
			llrbold.Close()
			llrbold.Destroy()
			llrbold = index
		}

		return newindex
	}

	do := func() {
		index := loadllrbindex()
		if err := lmdbenv.Sync(true /*force*/); err != nil {
			panic(err)
		}

		if log {
			fmt.Println()
			index.Log()
		}

		now := time.Now()
		index.Validate()
		took := time.Since(now).Round(time.Second)
		fmt.Printf("Took %v to validate index\n\n", took)

		func() {
			llrbrw.Lock()
			defer llrbrw.Unlock()

			compareLlrbLmdb(index, lmdbenv, lmdbdbi)
			if (rnd.Intn(100000) % 3) == 0 {
				loadllrb(index)
			}
		}()
	}

	defer func() {
		if r := recover(); r == nil {
			do()
		} else {
			panic(r)
		}
	}()

	tick := time.NewTicker(100 * time.Second)
	for {
		select {
		case <-tick.C:
			do()
		case <-fin:
			return
		}
	}
}

func llrbLmdbLoad(seedl int64) error {
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateloadr(klen, vlen, int64(options.load), int64(seedl))

	now, oldvalue := time.Now(), make([]byte, 16)
	opaque := atomic.AddUint64(&seqno, 1)
	key, value := g(make([]byte, 16), make([]byte, 16), opaque)
	for ; key != nil; key, value = g(key, value, opaque) {
		index := loadllrbindex()
		oldvalue, _ := index.Set(key, value, oldvalue)
		if len(oldvalue) > 0 {
			panic(fmt.Errorf("unexpected %q", oldvalue))
		}
		opaque = atomic.AddUint64(&seqno, 1)
	}
	atomic.AddInt64(&numentries, int64(options.load))
	atomic.AddInt64(&totalwrites, int64(options.load))

	index := loadllrbindex()
	took := time.Since(now).Round(time.Second)
	fmt.Printf("Loaded LLRB %v items in %v\n\n", index.Count(), took)
	return nil
}

var llrblmdbsets = []func(index *llrb.LLRB, k, v, ov []byte) (uint64, []byte){
	llrbLmdbSet1, llrbLmdbSet2, llrbLmdbSet3, llrbLmdbSet4,
}

func llrbLmdbCreater(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	n, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generatecreate(klen, vlen, n, seedc)

	key, value := make([]byte, 16), make([]byte, 16)
	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)

	do := func() error {
		llrbrw.RLock()
		defer llrbrw.RUnlock()

		index := loadllrbindex()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		setidx := rnd.Intn(1000000) % 4
		refcas, _ := llrblmdbsets[setidx](index, key, value, oldvalue)
		oldvalue, cas, del, ok := index.Get(key, oldvalue)
		if ok == false {
			panic("unexpected false")
		} else if del == true {
			panic("unexpected delete")
		} else if refcas > 0 && cas != refcas {
			panic(fmt.Errorf("expected %v, got %v", refcas, cas))
		}
		comparekeyvalue(key, oldvalue, options.vallen)

		atomic.AddInt64(&numentries, 1)
		atomic.AddInt64(&totalwrites, 1)
		if nc := atomic.AddInt64(&ncreates, 1); nc%markercount == 0 {
			count := index.Count()
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "llrbLmdbCreated {%v items in %v} {%v items in %v} " +
				"count:%v\n"
			fmt.Printf(fmsg, markercount, x, nc, y, count)
			now = time.Now()
		}
		// update the lmdb object
		if err := lmdbDocreate(lmdbenv, lmdbdbi, key, value); err != nil {
			panic(err)
		}
		return nil
	}

	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		if err := do(); err != nil {
			return
		}
		runtime.Gosched()
	}
	fmsg := "at exit, llrbLmdbCreated %v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, atomic.LoadInt64(&ncreates), took)
}

func vllrblmdbupdater(
	key, oldvalue []byte, refcas, cas uint64, i int, del, ok bool) string {

	var err error
	if ok == false {
		err = fmt.Errorf("unexpected false")
	} else if del == true {
		err = fmt.Errorf("unexpected delete")
	} else if refcas > 0 && cas != refcas {
		err = fmt.Errorf("expected %v, got %v", refcas, cas)
	}
	comparekeyvalue(key, oldvalue, options.vallen)
	if err != nil && i == 0 {
		panic(err)
	} else if err != nil {
		atomic.AddInt64(&conflicts, 1)
		return "repeat"
	}
	return "ok"
}

func llrbLmdbUpdater(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	var nupdates int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateupdate(klen, vlen, n, seedl, seedc, -1)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)

	do := func() error {
		llrbrw.RLock()
		defer llrbrw.RUnlock()

		index := loadllrbindex()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		setidx := rnd.Intn(1000000) % 4
		for i := 2; i >= 0; i-- {
			set := llrblmdbsets[setidx]
			refcas, oldvalue := set(index, key, value, oldvalue)
			if len(oldvalue) == 0 {
				atomic.AddInt64(&numentries, 1)
			}
			oldvalue, cas, del, ok := index.Get(key, oldvalue)
			rc := vllrblmdbupdater(key, oldvalue, refcas, cas, i, del, ok)
			if rc == "ok" {
				break
			}
		}

		atomic.AddInt64(&totalwrites, 1)
		if nupdates = nupdates + 1; nupdates%markercount == 0 {
			count := index.Count()
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "llrbLmdbUpdated {%v items in %v} {%v items in %v} " +
				"count:%v\n"
			fmt.Printf(fmsg, markercount, x, nupdates, y, count)
			now = time.Now()
		}
		// update the lmdb updater.
		_, err := lmdbDoupdate(lmdbenv, lmdbdbi, key, value)
		if err != nil {
			panic(err)
		}
		return nil
	}

	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		if err := do(); err != nil {
			return
		}
		runtime.Gosched()
	}
	fmsg := "at exit, llrbLmdbUpdated %v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, nupdates, took)
}

func llrbLmdbSet1(
	index *llrb.LLRB, key, value, oldvalue []byte) (uint64, []byte) {

	oldvalue, cas := index.Set(key, value, oldvalue)
	//fmt.Printf("update1 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 {
		comparekeyvalue(key, oldvalue, options.vallen)
	}
	return cas, oldvalue
}

func llrblmdbverifyset2(err error, i int, key, oldvalue []byte) string {
	if err != nil {
	} else if len(oldvalue) > 0 {
		comparekeyvalue(key, oldvalue, options.vallen)
	}
	if err != nil && i == 0 {
		panic(err)
	} else if err != nil {
		atomic.AddInt64(&conflicts, 1)
		return "repeat"
	}
	return "ok"
}

func llrbLmdbSet2(
	index *llrb.LLRB, key, value, oldvalue []byte) (uint64, []byte) {

	for i := 2; i >= 0; i-- {
		oldvalue, oldcas, deleted, ok := index.Get(key, oldvalue)
		if deleted || ok == false {
			oldcas = 0
		} else if oldcas == 0 {
			panic(fmt.Errorf("unexpected %v", oldcas))
		}
		comparekeyvalue(key, oldvalue, options.vallen)
		oldvalue, cas, err := index.SetCAS(key, value, oldvalue, oldcas)
		if ok == false && len(oldvalue) > 0 {
			panic(fmt.Errorf("unexpected %q", oldvalue))
		}
		//fmt.Printf("update2 %q %q %q \n", key, value, oldvalue)
		if llrblmdbverifyset2(err, i, key, oldvalue) == "ok" {
			return cas, oldvalue
		}
	}
	panic("unreachable code")
}

func llrbLmdbSet3(
	index *llrb.LLRB, key, value, oldvalue []byte) (uint64, []byte) {

	txn := index.BeginTxn(0xC0FFEE)
	oldvalue = txn.Set(key, value, oldvalue)
	//fmt.Printf("update3 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 {
		comparekeyvalue(key, oldvalue, options.vallen)
	}
	if err := txn.Commit(); err != nil {
		panic(err)
	}
	return 0, oldvalue
}

func llrbLmdbSet4(
	index *llrb.LLRB, key, value, oldvalue []byte) (uint64, []byte) {

	txn := index.BeginTxn(0xC0FFEE)
	cur, err := txn.OpenCursor(key)
	if err != nil {
		panic(err)
	}
	oldvalue = cur.Set(key, value, oldvalue)
	//fmt.Printf("update4 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 {
		comparekeyvalue(key, oldvalue, options.vallen)
	}
	if err := txn.Commit(); err != nil {
		panic(err)
	}
	return 0, oldvalue
}

var llrblmdbdels = []func(*llrb.LLRB, []byte, []byte, bool) (uint64, bool){
	llrbLmdbDel1, llrbLmdbDel2, llrbLmdbDel3, llrbLmdbDel4,
}

func vllrblmdbdel(
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
			if len(oldvalue) > 0 {
				comparekeyvalue(key, oldvalue, options.vallen)
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

func llrbLmdbDeleter(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()
	time.Sleep(1 * time.Second) // delay start for llrbUpdater to catchup.

	var ndeletes, xdeletes int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generatedelete(klen, vlen, n, seedl, seedc, delmod)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	lsmmap := map[int]bool{0: true, 1: false}

	do := func() error {
		llrbrw.RLock()
		defer llrbrw.RUnlock()

		index := loadllrbindex()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		//fmt.Printf("delete %q\n", key)
		ln := len(llrblmdbdels)
		delidx, lsm := rnd.Intn(1000000)%ln, lsmmap[rnd.Intn(1000000)%2]
		if lsm {
			delidx = delidx % 2
		}
		for i := 2; i >= 0; i-- {
			refcas, ok1 := llrblmdbdels[delidx](index, key, value, lsm)
			oldvalue, _, _, ok2 := index.Get(key, oldvalue)
			rc := vllrblmdbdel(index, key, oldvalue, refcas, i, lsm, ok2)
			if rc == "ok" {
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
			fmsg := "llrbLmdbDeleted {%v items %v} {%v:%v items in %v} " +
				"count:%v\n"
			fmt.Printf(fmsg, markercount, x, ndeletes, xdeletes, y, count)
			now = time.Now()
		}

		// update lmdb
		if _, err := lmdbDodelete(lmdbenv, lmdbdbi, key, value); err != nil {
			if strings.Contains(err.Error(), lmdbmissingerr) {
				return nil
			}
			panic(err)
		}
		return nil
	}

	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		if err := do(); err != nil {
			return
		}
		runtime.Gosched()
	}
	fmsg := "at exit, llrbLmdbDeleter %v:%v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, ndeletes, xdeletes, took)
}

func llrbLmdbDel1(
	index *llrb.LLRB, key, oldval []byte, lsm bool) (uint64, bool) {

	var ok bool

	oldval, cas := index.Delete(key, oldval, lsm)
	if len(oldval) > 0 {
		comparekeyvalue(key, oldval, options.vallen)
		ok = true
	}
	return cas, ok
}

func llrbLmdbDel2(
	index *llrb.LLRB, key, oldval []byte, lsm bool) (uint64, bool) {

	var ok bool

	txn := index.BeginTxn(0xC0FFEE)
	oldval = txn.Delete(key, oldval, lsm)
	if len(oldval) > 0 {
		comparekeyvalue(key, oldval, options.vallen)
		ok = true
	}
	if err := txn.Commit(); err != nil {
		panic(err)
	}
	return 0, ok
}

func llrbLmdbDel3(
	index *llrb.LLRB, key, oldval []byte, lsm bool) (uint64, bool) {

	var ok bool

	txn := index.BeginTxn(0xC0FFEE)
	cur, err := txn.OpenCursor(key)
	if err != nil {
		panic(err)
	}
	oldval = cur.Delete(key, oldval, lsm)
	if len(oldval) > 0 {
		comparekeyvalue(key, oldval, options.vallen)
		ok = true
	}
	if err := txn.Commit(); err != nil {
		panic(err)
	}
	return 0, ok
}

func llrbLmdbDel4(
	index *llrb.LLRB, key, oldval []byte, lsm bool) (uint64, bool) {

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

var llrblmdbgets = []func(*lmdb.Env, lmdb.DBI, *llrb.LLRB, []byte, []byte) ([]byte, uint64, bool, bool){
	llrbLmdbGet1, llrbLmdbGet2, llrbLmdbGet3,
}

func llrbLmdbGetter(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	n, seedl, seedc int64, fin chan struct{}, wg *sync.WaitGroup) {

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
		index := loadllrbindex()
		ngets++
		key = g(key, atomic.LoadInt64(&ncreates))
		get := llrblmdbgets[(rnd.Intn(1000000) % len(llrblmdbgets))]
		value, _, del, _ = get(lmdbenv, lmdbdbi, index, key, value)
		if x, xerr := strconv.Atoi(Bytes2str(key)); xerr != nil {
			panic(xerr)

		} else if (int64(x) % updtdel) != delmod {
			if del {
				panic(fmt.Errorf("unexpected deleted"))
			}
			comparekeyvalue(key, value, options.vallen)

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
			fmsg := "llrbLmdbGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
		}

		runtime.Gosched()
	}
	took := time.Since(epoch).Round(time.Second)
	<-fin
	fmsg := "at exit, llrbLmdbGetter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, took)
}

func llrbLmdbGet1(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *llrb.LLRB, key, value []byte) ([]byte, uint64, bool, bool) {

	llrbval, seqno, del, ok := index.Get(key, value)

	get := func(txn *lmdb.Txn) (err error) {
		lmdbval, err := txn.Get(lmdbdbi, key)
		if del == false && options.vallen > 0 {
			if bytes.Compare(llrbval, lmdbval) != 0 {
				fmsg := "retry: expected %q, got %q"
				return fmt.Errorf(fmsg, lmdbval, llrbval)
			}
		}
		return nil
	}
	trylmdbget(lmdbenv, 5000, get)

	return llrbval, seqno, del, ok
}

func llrbLmdbGet2(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *llrb.LLRB, key, value []byte) ([]byte, uint64, bool, bool) {

	var llrbval []byte
	var del, ok bool
	var llrbtxn api.Transactor

	get := func(txn *lmdb.Txn) (err error) {
		llrbtxn = index.BeginTxn(0xC0FFEE)
		llrbval, _, del, ok = llrbtxn.Get(key, value)

		lmdbval, err := txn.Get(lmdbdbi, key)
		if del == false && options.vallen > 0 {
			if bytes.Compare(llrbval, lmdbval) != 0 {
				llrbtxn.Abort()
				fmsg := "retry: expected %q, got %q"
				return fmt.Errorf(fmsg, lmdbval, llrbval)
			}
		}
		return nil
	}
	trylmdbget(lmdbenv, 5000, get)

	if ok == true {
		cur, err := llrbtxn.OpenCursor(key)
		if err != nil {
			panic(err)
		}
		if ckey, cdel := cur.Key(); cdel != del {
			panic(fmt.Errorf("expected %v, got %v", del, cdel))
		} else if bytes.Compare(ckey, key) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, ckey))
		} else if cvalue := cur.Value(); bytes.Compare(cvalue, llrbval) != 0 {
			panic(fmt.Errorf("expected %q, got %q", llrbval, cvalue))
		}
	}
	llrbtxn.Abort()

	return llrbval, 0, del, ok
}

func llrbLmdbGet3(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *llrb.LLRB, key, value []byte) ([]byte, uint64, bool, bool) {

	var llrbval []byte
	var del, ok bool
	var view api.Transactor

	get := func(txn *lmdb.Txn) (err error) {
		view = index.View(0x1235)
		llrbval, _, del, ok = view.Get(key, value)

		lmdbval, err := txn.Get(lmdbdbi, key)
		if del == false && options.vallen > 0 {
			if bytes.Compare(llrbval, lmdbval) != 0 {
				view.Abort()
				fmsg := "retry: expected %q, got %q"
				return fmt.Errorf(fmsg, lmdbval, llrbval)
			}
		}
		return nil
	}
	trylmdbget(lmdbenv, 5000, get)

	if ok == true {
		cur, err := view.OpenCursor(key)
		if err != nil {
			panic(err)
		}
		if ckey, cdel := cur.Key(); cdel != del {
			panic(fmt.Errorf("expected %v, got %v", del, cdel))
		} else if bytes.Compare(ckey, key) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, ckey))
		} else if cvalue := cur.Value(); bytes.Compare(cvalue, llrbval) != 0 {
			panic(fmt.Errorf("expected %q, got %q", llrbval, cvalue))
		}
	}
	view.Abort()

	return llrbval, 0, del, ok
}

var llrblmdbrngs = []func(index *llrb.LLRB, key, val []byte) int64{
	llrbLmdbRange1, llrbLmdbRange2, llrbLmdbRange3, llrbLmdbRange4,
}

func llrbLmdbRanger(n, seedl, seedc int64, fin chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	var nranges int64
	var key []byte
	g := Generateread(int64(options.keylen), n, seedl, seedc)

	rnd := rand.New(rand.NewSource(seedc))
	epoch, value := time.Now(), make([]byte, 16)
loop:
	for {
		index := loadllrbindex()
		key = g(key, atomic.LoadInt64(&ncreates))
		ln := len(llrblmdbrngs)
		n := llrblmdbrngs[rnd.Intn(1000000)%ln](index, key, value)
		nranges += n
		select {
		case <-fin:
			break loop
		default:
		}
		runtime.Gosched()
	}
	took := time.Since(epoch).Round(time.Second)
	<-fin
	fmt.Printf("at exit, llrbLmdbRanger %v items in %v\n", nranges, took)
}

func llrbLmdbRange1(index *llrb.LLRB, key, value []byte) (n int64) {
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
		} else if (int64(x)%updtdel) != delmod && del == true {
			panic("unexpected delete")
		} else if del == false {
			comparekeyvalue(key, value, options.vallen)
		}
		n++
	}
	txn.Abort()
	return
}

func llrbLmdbRange2(index *llrb.LLRB, key, value []byte) (n int64) {
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
		} else if (int64(x)%updtdel) != delmod && del == true {
			panic("unexpected delete")
		} else if del == false {
			comparekeyvalue(key, value, options.vallen)
		}
		n++
	}
	txn.Abort()
	return
}

func llrbLmdbRange3(index *llrb.LLRB, key, value []byte) (n int64) {
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
		} else if (int64(x)%updtdel) != delmod && del == true {
			panic("unexpected delete")
		} else if del == false {
			comparekeyvalue(key, value, options.vallen)
		}
		n++
	}
	view.Abort()
	return
}

func llrbLmdbRange4(index *llrb.LLRB, key, value []byte) (n int64) {
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
		} else if (int64(x)%updtdel) != delmod && del == true {
			panic("unexpected delete")
		} else if del == false {
			comparekeyvalue(key, value, options.vallen)
		}
		n++
	}
	view.Abort()
	return
}

func compareLlrbLmdb(index *llrb.LLRB, lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {
	lmdbcount := getlmdbCount(lmdbenv, lmdbdbi)
	llrbcount := index.Count()
	seqno := atomic.LoadUint64(&seqno)
	fmsg := "compareLlrbLmdb, lmdbcount:%v llrbcount:%v seqno:%v\n"
	fmt.Printf(fmsg, lmdbcount, llrbcount, seqno)

	epoch, cmpcount := time.Now(), 0

	iter := index.Scan()
	err := lmdbenv.View(func(txn *lmdb.Txn) error {
		lmdbcur, err := txn.OpenCursor(lmdbdbi)
		if err != nil {
			panic(err)
		}

		llrbkey, llrbval, _, llrbdel, llrberr := iter(false /*fin*/)
		lmdbkey, lmdbval, lmdberr := lmdbcur.Get(nil, nil, lmdb.Next)

		for llrbkey != nil {
			if llrbdel == false {
				cmpcount++
				if llrberr != nil {
					panic(llrberr)

				} else if lmdberr != nil {
					panic(lmdberr)

				} else if bytes.Compare(llrbkey, lmdbkey) != 0 {
					val, seqno, del, ok := index.Get(lmdbkey, make([]byte, 0))
					fmt.Printf("%q %v %v %v\n", val, seqno, del, ok)
					val = cmplmdbget(lmdbenv, lmdbdbi, llrbkey)
					fmt.Printf("%q\n", val)

					fmsg := "expected %q,%q, got %q,%q"
					panic(fmt.Errorf(fmsg, lmdbkey, lmdbval, llrbkey, llrbval))

				} else if bytes.Compare(llrbval, lmdbval) != 0 {
					fmsg := "for %q expected val %q, got val %q\n"
					x, y := lmdbval[:options.vallen], llrbval[:options.vallen]
					fmt.Printf(fmsg, llrbkey, x, y)
					fmsg = "for %q expected seqno %v, got %v\n"
					x, y = lmdbval[options.vallen:], llrbval[options.vallen:]
					fmt.Printf(fmsg, llrbkey, lmdbval, llrbval)
					panic("error")
				}
				//fmt.Printf("compareLlrbLmdb %q okay ...\n", llrbkey)
			}

			llrbkey, llrbval, _, llrbdel, llrberr = iter(false /*fin*/)
			lmdbkey, lmdbval, lmdberr = lmdbcur.Get(nil, nil, lmdb.Next)
		}
		if lmdbkey != nil {
			return fmt.Errorf("found lmdb key %q\n", lmdbkey)
		}

		return lmdberr
	})
	if err != nil {
		panic(err)
	}
	iter(true /*fin*/)

	took := time.Since(epoch).Round(time.Second)
	fmt.Printf("Took %v to compare (%v) LLRB and LMDB\n\n", took, cmpcount)
}