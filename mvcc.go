package main

import "io"
import "os"
import "fmt"
import "sync"
import "time"
import "bytes"
import "strconv"
import "runtime"
import "math/rand"
import "unsafe"
import "sync/atomic"

import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"
import "github.com/prataprc/gostore/llrb"
import "github.com/prataprc/gostore/api"
import "github.com/bmatsuo/lmdb-go/lmdb"

// manage global mvcc-index and older copy of the mvcc-index.
var mvccold *llrb.MVCC
var mvccindex unsafe.Pointer // *llrb.MVCC
func loadmvccindex() *llrb.MVCC {
	return (*llrb.MVCC)(atomic.LoadPointer(&mvccindex))
}
func storemvccindex(index *llrb.MVCC) {
	atomic.StorePointer(&mvccindex, unsafe.Pointer(index))
}

func testmvcc() error {
	seedl, seedc := int64(options.seed), int64(options.seed)+100
	fmt.Printf("Seed for load: %v, for ops: %v\n\n", seedl, seedc)

	// LMDB instance
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
	mvccname, mvccsetts := "dbtest", llrb.Defaultsettings()
	index := llrb.NewMVCC("dbtest", mvccsetts)
	defer index.Destroy()
	storemvccindex(index)

	// load index and reference with initial data.
	domvccload(index, lmdbenv, lmdbdbi)
	// test index and reference read / write
	domvccrw(mvccname, mvccsetts, index, lmdbenv, lmdbdbi)

	index.Log()
	index.Validate()

	if mvccold != nil {
		mvccold.Close()
		mvccold.Destroy()
	}
	if index := loadmvccindex(); index != nil {
		index.Close()
		index.Destroy()
	}

	fmt.Printf("Number of ROLLBACKS: %v\n", rollbacks)
	fmt.Printf("Number of conflicts: %v\n", conflicts)
	count, n := index.Count(), atomic.LoadInt64(&numentries)
	fmt.Printf("MVCC total indexed %v items, expected %v\n", count, n)

	return nil

	return nil
}

func domvccload(index *llrb.MVCC, lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {
	seedl := int64(options.seed)
	if err := mvccLoad(seedl); err != nil {
		panic(err)
	}
	seqno = 0
	atomic.StoreInt64(&totalwrites, 0)
	if err := lmdbLoad(lmdbenv, lmdbdbi, seedl); err != nil {
		panic(err)
	}
}

func domvccrw(
	mvccname string, mvccsetts s.Settings,
	index *llrb.MVCC, lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {

	seedl, seedc := int64(options.seed), int64(options.seed)+100

	var wwg, rwg sync.WaitGroup
	fin := make(chan struct{})

	go mvccvalidator(
		lmdbenv, lmdbdbi, true, seedl, mvccname, mvccsetts, &rwg, fin,
	)
	rwg.Add(1)

	// writer routines
	n := atomic.LoadInt64(&numentries)
	go mvccCreater(lmdbenv, lmdbdbi, n, seedc, &wwg)
	go mvccUpdater(lmdbenv, lmdbdbi, n, seedl, seedc, &wwg)
	go mvccDeleter(lmdbenv, lmdbdbi, n, seedl, seedc, &wwg)
	wwg.Add(3)
	// reader routines
	for i := 0; i < numcpus; i++ {
		go mvccGetter(lmdbenv, lmdbdbi, n, seedl, seedc, fin, &rwg)
		go mvccRanger(n, seedl, seedc, fin, &rwg)
		rwg.Add(2)
	}
	wwg.Wait()
	close(fin)
	rwg.Wait()

}

var mvccrw sync.RWMutex

func mvccvalidator(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI, log bool, seedl int64,
	mvccname string, mvccsetts s.Settings,
	wg *sync.WaitGroup, fin chan struct{}) {

	defer wg.Done()

	rnd := rand.New(rand.NewSource(seedl))

	loadmvcc := func(index *llrb.MVCC) *llrb.MVCC {
		now := time.Now()
		newindex := llrb.LoadMVCC(mvccname, mvccsetts, index.Scan())
		storemvccindex(newindex)
		thisseqno := index.Getseqno()
		newindex.Setseqno(thisseqno)
		fmsg := "Took %v to LoadMVCC index @ %v \n\n"
		fmt.Printf(fmsg, time.Since(now), thisseqno)

		if mvccold != nil {
			mvccold.Close()
			mvccold.Destroy()
			mvccold = index
		}

		return newindex
	}

	do := func() {
		index := loadmvccindex()

		if log {
			fmt.Println()
			index.Log()
		}

		now := time.Now()
		index.Validate()
		fmt.Printf("Took %v to validate index\n\n", time.Since(now))

		func() {
			mvccrw.Lock()
			defer mvccrw.Unlock()

			syncsleep(mvccsetts)
			compareMvccLmdb(index, lmdbenv, lmdbdbi)
			if (rnd.Intn(100000) % 3) == 0 {
				loadmvcc(index)
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

	tick := time.NewTicker(25 * time.Second)
	for {
		select {
		case <-tick.C:
			do()
		case <-fin:
			return
		}
	}
}

func mvccLoad(seedl int64) error {
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateloadr(klen, vlen, int64(options.load), int64(seedl))

	now, oldvalue := time.Now(), make([]byte, 16)
	opaque := atomic.AddUint64(&seqno, 1)
	key, value := g(make([]byte, 16), make([]byte, 16), opaque)
	for key != nil {
		index := loadmvccindex()
		oldvalue, _ := index.Set(key, value, oldvalue)
		if len(oldvalue) > 0 {
			panic(fmt.Errorf("unexpected %q", oldvalue))
		}
		opaque = atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
	}
	atomic.AddInt64(&numentries, int64(options.load))
	atomic.AddInt64(&totalwrites, int64(options.load))

	index := loadmvccindex()
	fmt.Printf("Loaded MVCC %v items in %v\n\n", index.Count(), time.Since(now))
	return nil
}

var mvccsets = []func(index *llrb.MVCC, k, v, ov []byte) (uint64, []byte){
	mvccSet1, mvccSet2, mvccSet3, mvccSet4,
}

func mvccCreater(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	n, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generatecreate(klen, vlen, n, seedc)

	key, value := make([]byte, 16), make([]byte, 16)
	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)

	do := func() error {
		mvccrw.RLock()
		defer mvccrw.RUnlock()

		index := loadmvccindex()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		setidx := rnd.Intn(1000000) % len(mvccsets)
		refcas, _ := mvccsets[setidx](index, key, value, oldvalue)
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
			x, y := time.Since(now).Round(time.Second), time.Since(epoch)
			fmsg := "mvccCreated {%v items in %v} {%v items in %v} count:%v\n"
			fmt.Printf(fmsg, markercount, x, nc, y.Round(time.Second), count)
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
	fmsg := "at exit, mvccCreated %v items in %v"
	fmt.Printf(fmsg, atomic.LoadInt64(&ncreates), time.Since(epoch))
}

func vmvccupdater(
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

func mvccUpdater(
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
		mvccrw.RLock()
		defer mvccrw.RUnlock()

		index := loadmvccindex()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		setidx := rnd.Intn(1000000) % len(mvccsets)
		for i := 2; i >= 0; i-- {
			refcas, oldvalue := mvccsets[setidx](index, key, value, oldvalue)
			if len(oldvalue) == 0 {
				atomic.AddInt64(&numentries, 1)
			}
			oldvalue, cas, del, ok := index.Get(key, oldvalue)
			if vmvccupdater(key, oldvalue, refcas, cas, i, del, ok) == "ok" {
				break
			}
		}

		atomic.AddInt64(&totalwrites, 1)
		if nupdates = nupdates + 1; nupdates%markercount == 0 {
			count := index.Count()
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "mvccUpdated {%v items in %v} {%v items in %v} count:%v\n"
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
	fmsg := "at exit, mvccUpdated %v items in %v\n"
	fmt.Printf(fmsg, nupdates, time.Since(epoch))
}

func mvccSet1(index *llrb.MVCC, key, value, oldvalue []byte) (uint64, []byte) {
	oldvalue, cas := index.Set(key, value, oldvalue)
	//fmt.Printf("update1 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 {
		comparekeyvalue(key, oldvalue, options.vallen)
	}
	return cas, oldvalue
}

func mvccverifyset2(err error, i int, key, oldvalue []byte) string {
	if err == nil && len(oldvalue) > 0 {
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

func mvccSet2(index *llrb.MVCC, key, value, oldvalue []byte) (uint64, []byte) {
	for i := 3; i >= 0; i-- {
		oldvalue, oldcas, deleted, ok := index.Get(key, oldvalue)
		if deleted || ok == false {
			oldcas = 0
		} else if oldcas == 0 {
			panic(fmt.Errorf("unexpected %v", oldcas))
		}
		comparekeyvalue(key, oldvalue, options.vallen)
		oldvalue, cas, err := index.SetCAS(key, value, oldvalue, oldcas)
		//fmt.Printf("mvccSet2 %q %q %v %v\n", key, value, oldcas, err)
		if mvccverifyset2(err, i, key, oldvalue) == "ok" {
			return cas, oldvalue
		}
	}
	panic("unreachable code")
}

func mvccSet3(index *llrb.MVCC, key, value, oldvalue []byte) (uint64, []byte) {
	for i := numcpus * 2; i >= 0; i-- {
		txn := index.BeginTxn(0xC0FFEE)
		oldvalue = txn.Set(key, value, oldvalue)
		//fmt.Printf("update3 %q %q %q \n", key, value, oldvalue)
		if len(oldvalue) > 0 {
			comparekeyvalue(key, oldvalue, options.vallen)
		}
		err := txn.Commit()
		if err == nil {
			return 0, oldvalue
		} else if i == 0 {
			panic(err)
		} else if err.Error() == api.ErrorRollback.Error() {
			atomic.AddInt64(&rollbacks, 1)
		}
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
	return 0, oldvalue
}

func mvccSet4(index *llrb.MVCC, key, value, oldvalue []byte) (uint64, []byte) {
	for i := numcpus * 2; i >= 0; i-- {
		txn := index.BeginTxn(0xC0FFEE)
		cur, err := txn.OpenCursor(key)
		if err != nil {
			panic(err)
		}
		oldvalue = cur.Set(key, value, oldvalue)
		//fmt.Printf("update4 %q %q %q\n", key, value, oldvalue)
		if len(oldvalue) > 0 {
			comparekeyvalue(key, oldvalue, options.vallen)
		}
		err = txn.Commit()
		if err == nil {
			return 0, oldvalue
		} else if i == 0 {
			panic(err)
		} else if err.Error() == api.ErrorRollback.Error() {
			atomic.AddInt64(&rollbacks, 1)
		}
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
	return 0, oldvalue
}

var mvccdels = []func(*llrb.MVCC, []byte, []byte, bool) (uint64, bool){
	mvccDel1, mvccDel2, mvccDel3, mvccDel4,
}

func vmvccdel(
	index interface{}, key, oldvalue []byte, refcas uint64,
	i int, lsm, ok bool) string {

	var err error
	if lsm {
		var view api.Transactor
		switch idx := index.(type) {
		case *llrb.LLRB:
			view = idx.View(0x1234)
		case *llrb.MVCC:
			view = idx.View(0x1234)
		}

		cur, err := view.OpenCursor(key)
		if err == nil {
			_, oldvalue, cas, del, err := cur.YNext(false)
			if err != nil {
			} else if del == false {
				err = fmt.Errorf("expected delete")
			} else if refcas > 0 && cas != refcas {
				err = fmt.Errorf("expected %v, got %v", refcas, cas)
			}
			if err == nil && len(oldvalue) > 0 {
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

func mvccDeleter(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	var ndeletes, xdeletes int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generatedelete(klen, vlen, n, seedl, seedc, delmod)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	lsm := options.lsm

	do := func() error {
		mvccrw.RLock()
		defer mvccrw.RUnlock()

		index := loadmvccindex()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		//fmt.Printf("delete %q\n", key)
		delidx := rnd.Intn(1000000) % len(mvccdels)
		if lsm {
			delidx = delidx % 2
		}
		for i := 2; i >= 0; i-- {
			refcas, ok1 := mvccdels[delidx](index, key, value, lsm)
			oldvalue, _, _, ok2 := index.Get(key, oldvalue)
			if vmvccdel(index, key, oldvalue, refcas, i, lsm, ok2) == "ok" {
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
			fmsg := "mvccDeleted {%v items in %v} {%v:%v items in %v} cnt:%v\n"
			fmt.Printf(fmsg, markercount, x, ndeletes, xdeletes, y, count)
			now = time.Now()
		}

		// update lmdb
		if _, err := lmdbDodelete(lmdbenv, lmdbdbi, key, value); err != nil {
			return err
		}
		return nil
	}

	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		if err := do(); err != nil {
			return
		}
		runtime.Gosched()
	}
	fmsg := "at exit, mvccDeleter %v:%v items in %v\n"
	fmt.Printf(fmsg, ndeletes, xdeletes, time.Since(epoch))
}

func mvccDel1(index *llrb.MVCC, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	//fmt.Printf("mvccDel1 %q %v\n", key, lsm)
	oldvalue, cas := index.Delete(key, oldvalue, lsm)
	if len(oldvalue) > 0 {
		comparekeyvalue(key, oldvalue, options.vallen)
	} else if len(oldvalue) > 0 {
		ok = true
	}
	return cas, ok
}

func mvccDel2(index *llrb.MVCC, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	for i := numcpus * 2; i >= 0; i-- {
		txn := index.BeginTxn(0xC0FFEE)
		oldvalue = txn.Delete(key, oldvalue, lsm)
		if len(oldvalue) > 0 {
			comparekeyvalue(key, oldvalue, options.vallen)
		} else if len(oldvalue) > 0 {
			ok = true
		}
		err := txn.Commit()
		if err == nil {
			return 0, ok
		} else if i == 0 {
			panic(err)
		} else if err.Error() == api.ErrorRollback.Error() {
			atomic.AddInt64(&rollbacks, 1)
		}
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
	return 0, ok
}

func mvccDel3(index *llrb.MVCC, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	for i := numcpus * 2; i >= 0; i-- {
		txn := index.BeginTxn(0xC0FFEE)
		cur, err := txn.OpenCursor(key)
		if err != nil {
			panic(err)
		}
		oldvalue = cur.Delete(key, oldvalue, lsm)
		if len(oldvalue) > 0 {
			comparekeyvalue(key, oldvalue, options.vallen)
		} else if len(oldvalue) > 0 {
			ok = true
		}
		err = txn.Commit()
		if err == nil {
			return 0, ok
		} else if i == 0 {
			panic(err)
		} else if err.Error() == api.ErrorRollback.Error() {
			atomic.AddInt64(&rollbacks, 1)
		}
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
	return 0, ok
}

func mvccDel4(index *llrb.MVCC, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	for i := numcpus * 2; i >= 0; i-- {
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
		err = txn.Commit()
		if err == nil {
			return 0, ok
		} else if i == 0 {
			panic(err)
		} else if err.Error() == api.ErrorRollback.Error() {
			atomic.AddInt64(&rollbacks, 1)
		}
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
	return 0, ok
}

var mvccgets = []func(*lmdb.Env, lmdb.DBI, *llrb.MVCC, []byte, []byte) ([]byte, uint64, bool, bool){
	mvccGet1, mvccGet2, mvccGet3,
}

func mvccGetter(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	n, seedl, seedc int64,
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
		index := loadmvccindex()
		ngets++
		key = g(key, atomic.LoadInt64(&ncreates))
		get := mvccgets[rnd.Intn(1000000)%len(mvccgets)]
		value, _, del, _ = get(lmdbenv, lmdbdbi, index, key, value)
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

		select {
		case <-fin:
			break loop
		default:
		}
		if ngm := ngets + nmisses; ngm%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "mvccGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
			now = time.Now()
		}

		runtime.Gosched()
	}
	duration := time.Since(epoch)
	<-fin
	fmsg := "at exit, mvccGetter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, duration)
}

func mvccGet1(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *llrb.MVCC, key, value []byte) ([]byte, uint64, bool, bool) {

	mvccval, seqno, del, ok := index.Get(key, value)

	get := func(txn *lmdb.Txn) (err error) {
		lmdbval, err := txn.Get(lmdbdbi, key)
		if del == false && options.vallen > 0 {
			if bytes.Compare(mvccval, lmdbval) != 0 {
				fmsg := "retry: expected %q, got %q"
				return fmt.Errorf(fmsg, lmdbval, mvccval)
			}
		}
		return nil
	}
	trylmdbget(lmdbenv, 5000, get)

	return mvccval, seqno, del, ok
}

func mvccGet2(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *llrb.MVCC, key, value []byte) ([]byte, uint64, bool, bool) {

	var mvccval []byte
	var del, ok bool
	var mvcctxn api.Transactor

	get := func(txn *lmdb.Txn) (err error) {
		mvcctxn = index.BeginTxn(0xC0FFEE)
		mvccval, _, del, ok = mvcctxn.Get(key, value)

		lmdbval, err := txn.Get(lmdbdbi, key)
		if del == false && options.vallen > 0 {
			if bytes.Compare(mvccval, lmdbval) != 0 {
				mvcctxn.Abort()
				fmsg := "retry: expected %q, got %q"
				return fmt.Errorf(fmsg, lmdbval, mvccval)
			}
		}
		return nil
	}
	trylmdbget(lmdbenv, 5000, get)

	if ok == true {
		cur, err := mvcctxn.OpenCursor(key)
		if err != nil {
			panic(err)
		}
		if ckey, cdel := cur.Key(); cdel != del {
			panic(fmt.Errorf("expected %v, got %v", del, cdel))
		} else if bytes.Compare(ckey, key) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, ckey))
		} else if cvalue := cur.Value(); bytes.Compare(cvalue, mvccval) != 0 {
			panic(fmt.Errorf("expected %q, got %q", mvccval, cvalue))
		}
	}
	mvcctxn.Abort()

	return mvccval, 0, del, ok
}

func mvccGet3(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *llrb.MVCC, key, value []byte) ([]byte, uint64, bool, bool) {

	var mvccval []byte
	var del, ok bool
	var view api.Transactor

	get := func(txn *lmdb.Txn) (err error) {
		view = index.View(0x1235)
		mvccval, _, del, ok = view.Get(key, value)

		lmdbval, err := txn.Get(lmdbdbi, key)
		if del == false && options.vallen > 0 {
			if bytes.Compare(mvccval, lmdbval) != 0 {
				view.Abort()
				fmsg := "retry: expected %q, got %q"
				return fmt.Errorf(fmsg, lmdbval, mvccval)
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
		ykey, yvalue, _, ydel, _ := cur.YNext(false /*fin*/)
		if bytes.Compare(key, ykey) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, ykey))
		} else if del == false && ydel == false {
			if bytes.Compare(mvccval, yvalue) != 0 {
				panic(fmt.Errorf("expected %q, got %q", mvccval, yvalue))
			}
		}
		if ckey, _ := cur.Key(); bytes.Compare(ckey, key) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, ckey))
		} else if cvalue := cur.Value(); bytes.Compare(cvalue, mvccval) != 0 {
			panic(fmt.Errorf("expected %q, got %q", mvccval, cvalue))
		}
	}
	view.Abort()

	return mvccval, 0, del, ok
}

var mvccrngs = []func(index *llrb.MVCC, key, val []byte) int64{
	mvccRange1, mvccRange2, mvccRange3, mvccRange4,
}

func mvccRanger(
	n, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	defer wg.Done()

	var nranges int64
	var key []byte
	g := Generateread(int64(options.keylen), n, seedl, seedc)

	rnd := rand.New(rand.NewSource(seedc))
	epoch, value := time.Now(), make([]byte, 16)
loop:
	for {
		index := loadmvccindex()
		key = g(key, atomic.LoadInt64(&ncreates))
		ln := len(mvccrngs)
		n := mvccrngs[rnd.Intn(1000000)%ln](index, key, value)
		nranges += n
		select {
		case <-fin:
			break loop
		default:
		}
		runtime.Gosched()
	}
	duration := time.Since(epoch)
	<-fin
	fmt.Printf("at exit, mvccRanger %v items in %v\n", nranges, duration)
}

func mvccRange1(index *llrb.MVCC, key, value []byte) (n int64) {
	//fmt.Printf("mvccRange1 %q\n", key)
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
		}
		comparekeyvalue(key, value, options.vallen)
		n++
	}
	txn.Abort()
	return
}

func mvccRange2(index *llrb.MVCC, key, value []byte) (n int64) {
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
		} else if del == false {
			comparekeyvalue(key, value, options.vallen)
		}
		n++
	}
	txn.Abort()
	return
}

func mvccRange3(index *llrb.MVCC, key, value []byte) (n int64) {
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

func mvccRange4(index *llrb.MVCC, key, value []byte) (n int64) {
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
