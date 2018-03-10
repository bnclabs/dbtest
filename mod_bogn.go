package main

import "io"
import "os"
import "fmt"
import "sync"
import "time"
import "bytes"
import "runtime"
import "strings"
import "strconv"
import "math/rand"
import "sync/atomic"

import s "github.com/bnclabs/gosettings"
import "github.com/bnclabs/golog"
import "github.com/bnclabs/gostore/api"
import "github.com/bnclabs/gostore/bogn"
import "github.com/bmatsuo/lmdb-go/lmdb"
import "github.com/bmatsuo/lmdb-go/lmdbscan"

func testbogn() error {
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
	// new bogn instance.
	bognname, bognsetts := "dbtest", bognsettings(options.seed)
	logpath := bognsetts.String("logpath")
	diskstore := bognsetts.String("diskstore")
	diskpaths := bognsetts.Strings("bubt.diskpaths")
	bogn.PurgeIndex(bognname, logpath, diskstore, diskpaths)
	fmt.Println()
	index, err := bogn.New(bognname, bognsetts)
	if err != nil {
		panic(err)
	}
	index.Start()

	// load index and reference with initial data.
	dobognload(index, lmdbenv, lmdbdbi)
	// test index and reference read / write
	dobognrw(bognsetts, index, lmdbenv, lmdbdbi)

	index.Close()

	diskBognLmdb(bognname, lmdbpath, seedl, bognsetts)

	return nil
}

func dobognload(index *bogn.Bogn, lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {
	seedl := int64(options.seed)
	if err := bognLoad(index, seedl); err != nil {
		panic(err)
	}
	seqno = 0
	atomic.StoreInt64(&totalwrites, 0)
	if err := lmdbLoad(lmdbenv, lmdbdbi, seedl); err != nil {
		panic(err)
	}
}

func dobognrw(
	bognsetts s.Settings, index *bogn.Bogn,
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {

	seedl, seedc := int64(options.seed), int64(options.seed)+100

	var wwg, rwg sync.WaitGroup
	fin := make(chan struct{})

	go bognvalidator(lmdbenv, lmdbdbi, index, true, &rwg, fin, bognsetts)
	rwg.Add(1)

	// writer routines
	n := atomic.LoadInt64(&numentries)
	go bognCreater(lmdbenv, lmdbdbi, index, n, seedc, &wwg)
	go bognUpdater(lmdbenv, lmdbdbi, index, n, seedl, seedc, &wwg)
	go bognDeleter(lmdbenv, lmdbdbi, index, n, seedl, seedc, &wwg)
	wwg.Add(3)

	// reader routines
	for i := 0; i < options.cpu; i++ {
		go bognGetter(lmdbenv, lmdbdbi, index, n, seedl, seedc, fin, &rwg)
		go bognRanger(index, n, seedl, seedc, fin, &rwg)
		rwg.Add(2)
	}
	wwg.Wait()
	close(fin)
	rwg.Wait()

	fmt.Println()
	index.Log()
	index.Validate()
	fmt.Printf("Number of ROLLBACKS: %v\n", rollbacks)
	fmt.Printf("Number of conflicts: %v\n", conflicts)
}

var bognrw sync.RWMutex

func bognvalidator(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *bogn.Bogn, log bool, wg *sync.WaitGroup, fin chan struct{},
	bognsetts s.Settings) {

	defer wg.Done()

	do := func() {
		fmt.Println()
		if log {
			index.Log()
		}

		now := time.Now()
		index.Validate()
		took := time.Since(now).Round(time.Second)
		fmt.Printf("Took %v to validate index\n\n", took)

		func() {
			bognrw.Lock()
			defer bognrw.Unlock()

			syncsleep(bognsetts)
			compareBognLmdb(index, lmdbenv, lmdbdbi)
		}()
	}

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

func bognLoad(index *bogn.Bogn, seedl int64) error {
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateloadr(klen, vlen, int64(options.load), int64(seedl))

	now, oldvalue := time.Now(), make([]byte, 16)
	opaque := atomic.AddUint64(&seqno, 1)
	key, value := g(make([]byte, 16), make([]byte, 16), opaque)
	for key != nil {
		//fmt.Printf("load %q\n", key)
		oldvalue, _ := index.Set(key, value, oldvalue)
		if len(oldvalue) > 0 {
			panic(fmt.Errorf("unexpected %q", oldvalue))
		}
		opaque = atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
	}
	atomic.AddInt64(&numentries, int64(options.load))
	atomic.AddInt64(&totalwrites, int64(options.load))

	took := time.Since(now).Round(time.Second)
	fmt.Printf("Loaded BOGN %v items in %v\n\n", options.load, took)
	return nil
}

var bognsets = []func(index *bogn.Bogn, key, val, ov []byte) (uint64, []byte){
	bognSet1, bognSet2, bognSet3, bognSet4,
}

func bognCreater(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *bogn.Bogn, n, seedc int64, wg *sync.WaitGroup) {
	defer wg.Done()

	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generatecreate(klen, vlen, n, seedc)

	key, value := make([]byte, 16), make([]byte, 16)
	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)

	do := func() error {
		bognrw.RLock()
		defer bognrw.RUnlock()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		setidx := rnd.Intn(1000000) % len(bognsets)
		refcas, _ := bognsets[setidx](index, key, value, oldvalue)
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
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "bognCreated {%v items in %v} {%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, nc, y)
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
	fmsg := "at exit, bognCreated %v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, atomic.LoadInt64(&ncreates), took)
}

func vbognupdater(
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

func bognUpdater(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *bogn.Bogn, n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	var nupdates int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateupdate(klen, vlen, n, seedl, seedc, -1)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)

	do := func() error {
		bognrw.RLock()
		defer bognrw.RUnlock()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		setidx := rnd.Intn(1000000) % len(bognsets)
		for i := 2; i >= 0; i-- {
			refcas, _ := bognsets[setidx](index, key, value, oldvalue)
			oldvalue, cas, del, ok := index.Get(key, oldvalue)
			if vbognupdater(key, oldvalue, refcas, cas, i, del, ok) == "ok" {
				break
			}
		}

		atomic.AddInt64(&totalwrites, 1)
		if nupdates = nupdates + 1; nupdates%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "bognUpdated {%v items in %v} {%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, nupdates, y)
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
	fmsg := "at exit, bognUpdated %v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, nupdates, took)
}

func bognSet1(index *bogn.Bogn, key, value, oldvalue []byte) (uint64, []byte) {
	oldvalue, cas := index.Set(key, value, oldvalue)
	//fmt.Printf("update1 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 {
		comparekeyvalue(key, oldvalue, options.vallen)
	}
	return cas, oldvalue
}

func bognverifyset2(err error, i int, key, oldvalue []byte) string {
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

func bognSet2(index *bogn.Bogn, key, value, oldvalue []byte) (uint64, []byte) {
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
		//fmt.Printf("Set2 %q %q %q \n", key, value, oldvalue)
		if bognverifyset2(err, i, key, oldvalue) == "ok" {
			return cas, oldvalue
		}
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
	panic("unreachable code")
}

func bognSet3(index *bogn.Bogn, key, value, oldvalue []byte) (uint64, []byte) {
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

func bognSet4(index *bogn.Bogn, key, value, oldvalue []byte) (uint64, []byte) {
	for i := numcpus * 2; i >= 0; i-- {
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

var bogndels = []func(*bogn.Bogn, []byte, []byte, bool) (uint64, bool){
	bognDel1, bognDel2, bognDel3, bognDel4,
}

func vbogndel(
	index interface{}, key, oldvalue []byte, refcas uint64,
	i int, lsm, ok bool) string {

	var err error
	if lsm == false {
		if ok == true {
			err = fmt.Errorf("unexpected true when lsm is false")
		} else if len(oldvalue) > 0 {
			err = fmt.Errorf("unexpected %q when lsm is false", oldvalue)
		}

	} else {
		var view api.Transactor
		switch idx := index.(type) {
		case *bogn.Bogn:
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

func bognDeleter(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *bogn.Bogn, n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()
	time.Sleep(1 * time.Second) // delay start for bognUpdater to catchup.

	var ndeletes, xdeletes int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generatedelete(klen, vlen, n, seedl, seedc, delmod)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	lsm := options.lsm

	do := func() error {
		bognrw.RLock()
		defer bognrw.RUnlock()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		//fmt.Printf("delete %q\n", key)
		delidx := rnd.Intn(1000000) % len(bogndels)
		if lsm {
			delidx = delidx % 2
		}
		for i := 2; i >= 0; i-- {
			refcas, ok1 := bogndels[delidx](index, key, value, lsm)
			oldvalue, refcas, _, ok2 := index.Get(key, oldvalue)
			if vbogndel(index, key, oldvalue, refcas, i, lsm, ok2) == "ok" {
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
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "bognDeleted {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ndeletes, xdeletes, y)
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
	fmsg := "at exit, bognDeleter %v:%v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, ndeletes, xdeletes, took)
}

func bognDel1(index *bogn.Bogn, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	oldvalue, cas := index.Delete(key, oldvalue, lsm)
	if len(oldvalue) > 0 {
		comparekeyvalue(key, oldvalue, options.vallen)
	} else if len(oldvalue) > 0 {
		ok = true
	}
	return cas, ok
}

func bognDel2(index *bogn.Bogn, key, oldvalue []byte, lsm bool) (uint64, bool) {
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

func bognDel3(index *bogn.Bogn, key, oldvalue []byte, lsm bool) (uint64, bool) {
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

func bognDel4(index *bogn.Bogn, key, oldvalue []byte, lsm bool) (uint64, bool) {
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

var bogngets = []func(e *lmdb.Env, dbi lmdb.DBI, x *bogn.Bogn, k, v []byte) ([]byte, uint64, bool, bool){
	bognGet1, bognGet2, bognGet3,
}

func bognGetter(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *bogn.Bogn, n, seedl, seedc int64,
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
		key = g(key, atomic.LoadInt64(&ncreates))
		get := bogngets[rnd.Intn(1000000)%len(bogngets)]
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
			fmsg := "bognGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
		}

		runtime.Gosched()
	}
	took := time.Since(epoch).Round(time.Second)
	<-fin
	fmsg := "at exit, bognGetter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, took)
}

func bognGet1(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *bogn.Bogn, key, value []byte) ([]byte, uint64, bool, bool) {

	bognval, seqno, del, ok := index.Get(key, value)

	get := func(txn *lmdb.Txn) (err error) {
		lmdbval, err := txn.Get(lmdbdbi, key)
		if del == false && options.vallen > 0 {
			if bytes.Compare(bognval, lmdbval) != 0 {
				fmsg := "retry: expected %q, got %q"
				return fmt.Errorf(fmsg, lmdbval, bognval)
			}
		}
		return nil
	}
	trylmdbget(lmdbenv, 5000, get)

	return bognval, seqno, del, ok
}

func bognGet2(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *bogn.Bogn, key, value []byte) ([]byte, uint64, bool, bool) {

	var bognval []byte
	var del, ok bool
	var bogntxn api.Transactor

	get := func(txn *lmdb.Txn) (err error) {
		bogntxn = index.BeginTxn(0xC0FFEE)
		bognval, _, del, ok = bogntxn.Get(key, value)

		lmdbval, err := txn.Get(lmdbdbi, key)
		if del == false && options.vallen > 0 {
			if bytes.Compare(bognval, lmdbval) != 0 {
				bogntxn.Abort()
				fmsg := "retry: expected %q, got %q"
				return fmt.Errorf(fmsg, lmdbval, bognval)
			}
		}
		return nil
	}
	trylmdbget(lmdbenv, 5000, get)

	if ok == true {
		cur, err := bogntxn.OpenCursor(key)
		if err != nil {
			panic(err)
		}
		if ckey, cdel := cur.Key(); cdel != del {
			panic(fmt.Errorf("expected %v, got %v", del, cdel))
		} else if bytes.Compare(ckey, key) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, ckey))
		} else if cvalue := cur.Value(); bytes.Compare(cvalue, bognval) != 0 {
			panic(fmt.Errorf("expected %q, got %q", bognval, cvalue))
		}
	}
	bogntxn.Abort()

	return bognval, 0, del, ok
}

func bognGet3(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *bogn.Bogn, key, value []byte) ([]byte, uint64, bool, bool) {

	var bognval []byte
	var del, ok bool
	var view api.Transactor

	get := func(txn *lmdb.Txn) (err error) {
		view = index.View(0x1235)
		bognval, _, del, ok = view.Get(key, value)

		lmdbval, err := txn.Get(lmdbdbi, key)
		if del == false && options.vallen > 0 {
			if bytes.Compare(bognval, lmdbval) != 0 {
				view.Abort()
				fmsg := "retry: expected %q, got %q"
				return fmt.Errorf(fmsg, lmdbval, bognval)
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
		} else if cvalue := cur.Value(); bytes.Compare(cvalue, bognval) != 0 {
			panic(fmt.Errorf("expected %q, got %q", bognval, cvalue))
		}
	}
	view.Abort()

	return bognval, 0, del, ok
}

var bognrngs = []func(index *bogn.Bogn, key, val []byte) int64{
	bognRange1, bognRange2, bognRange3, bognRange4,
}

func bognRanger(
	index *bogn.Bogn, n, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	defer wg.Done()

	var nranges int64
	var key []byte
	g := Generateread(int64(options.keylen), n, seedl, seedc)

	rnd := rand.New(rand.NewSource(seedc))
	epoch, value := time.Now(), make([]byte, 16)
loop:
	for {
		key = g(key, atomic.LoadInt64(&ncreates))
		ln := len(bognrngs)
		n := bognrngs[rnd.Intn(1000000)%ln](index, key, value)
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
	fmt.Printf("at exit, bognRanger %v items in %v\n", nranges, took)
}

func bognRange1(index *bogn.Bogn, key, value []byte) (n int64) {
	//fmt.Printf("bognRange1 %q\n", key)
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
		}
		comparekeyvalue(key, value, options.vallen)
		n++
	}
	txn.Abort()
	return
}

func bognRange2(index *bogn.Bogn, key, value []byte) (n int64) {
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
		}
		comparekeyvalue(key, value, options.vallen)
		n++
	}
	txn.Abort()
	return
}

func bognRange3(index *bogn.Bogn, key, value []byte) (n int64) {
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
		}
		comparekeyvalue(key, value, options.vallen)
		n++
	}
	view.Abort()
	return
}

func bognRange4(index *bogn.Bogn, key, value []byte) (n int64) {
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
		}
		comparekeyvalue(key, value, options.vallen)
		n++
	}
	view.Abort()
	return
}

func bognsettings(seed int) s.Settings {
	flushratios := []float64{.5, .33, .25, .20, .16, .125, .1}
	rnd := rand.New(rand.NewSource(int64(seed)))

	setts := bogn.Defaultsettings()
	setts["memstore"] = options.memstore
	setts["flushperiod"] = int64(options.period)
	setts["flushratio"] = flushratios[rnd.Intn(10000)%len(flushratios)]
	setts["bubt.mmap"] = []bool{true, false}[rnd.Intn(10000)%2]
	setts["bubt.msize"] = []int64{4096, 8192, 12288}[rnd.Intn(10000)%3]
	setts["bubt.zsize"] = []int64{4096, 8192, 12288}[rnd.Intn(10000)%3]
	setts["llrb.memcapacity"] = options.capacity
	setts["llrb.allocator"] = "flist"
	setts["llrb.snapshottick"] = []int64{4, 8, 16, 32}[rnd.Intn(10000)%4]
	switch options.bogn {
	case "memonly":
		setts["durable"] = false
		setts["dgm"] = false
		setts["workingset"] = false
	case "durable":
		setts["durable"] = true
		setts["dgm"] = false
		setts["workingset"] = false
	case "dgm":
		setts["durable"] = true
		setts["dgm"] = true
		setts["workingset"] = false
	case "workset":
		setts["durable"] = true
		setts["dgm"] = true
		setts["workingset"] = true
	}

	a, b, c := setts["durable"], setts["dgm"], setts["workingset"]
	fmt.Printf("durable:%v dgm:%v workingset:%v lsm:%v\n", a, b, c, options.lsm)
	a, b = setts["flushratio"], setts["flushperiod"]
	fmt.Printf("flushratio:%v flushperiod:%v\n", a, b)
	a, b = setts["compactratio"], setts["compactperiod"]
	fmt.Printf("compactratio:%v compactperiod:%v\n", a, b)
	a = setts["llrb.snapshottick"]
	fmt.Printf("llrb snapshottick:%v\n", a)
	a, b = setts["bubt.diskpaths"], setts["bubt.msize"]
	c, d := setts["bubt.zsize"], setts["bubt.mmap"]
	fmt.Printf("bubt diskpaths:%v msize:%v zsize:%v mmap:%v\n", a, b, c, d)
	fmt.Println()

	return setts
}

func compareBognLmdb(index *bogn.Bogn, lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {
	epoch, cmpcount := time.Now(), 0

	//fmt.Println("bogn seqno", index.Getseqno())
	iter := index.Scan()
	err := lmdbenv.View(func(txn *lmdb.Txn) error {
		lmdbs := lmdbscan.New(txn, lmdbdbi)
		defer lmdbs.Close()

		lmdbs.Scan()

		bognkey, bognval, _, bogndel, bognerr := iter(false /*fin*/)
		lmdbkey, lmdbval, lmdberr := lmdbs.Key(), lmdbs.Val(), lmdbs.Err()

		for bognkey != nil {
			if bogndel == false {
				cmpcount++
				if bognerr != nil {
					panic(bognerr)
				} else if lmdberr != nil {
					panic(lmdberr)
				} else if bytes.Compare(bognkey, lmdbkey) != 0 {
					fmsg := "expected %q,%q, got %q,%q"
					panic(fmt.Errorf(fmsg, lmdbkey, lmdbval, bognkey, bognval))
				} else if bytes.Compare(bognval, lmdbval) != 0 {
					fmsg := "for %q expected %q, got %q"
					panic(fmt.Errorf(fmsg, bognkey, lmdbval, bognval))
				}
				//fmt.Printf("comparebognLmdb %q okay ...\n", llrbkey)
				lmdbs.Scan()
				lmdbkey, lmdbval, lmdberr = lmdbs.Key(), lmdbs.Val(), lmdbs.Err()
			}
			bognkey, bognval, _, bogndel, bognerr = iter(false /*fin*/)
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
	fmt.Printf("Took %v to compare (%v) BOGN and LMDB\n\n", took, cmpcount)
}

func diskBognLmdb(name, lmdbpath string, seedl int64, bognsetts s.Settings) {
	if bognsetts.Bool("durable") == false {
		return
	}

	fmt.Println("\n.......... Final disk check ..............\n")

	rnd := rand.New(rand.NewSource(seedl))
	// update settings
	memstores := []string{"mvcc", "llrb"}
	bognsetts["memstore"] = memstores[rnd.Intn(len(memstores))]
	_, _, freemem := getsysmem()
	capacities := []uint64{freemem, freemem, 10000}
	bognsetts["llrb.memcapacity"] = capacities[rnd.Intn(len(capacities))]
	index, err := bogn.New(name /*dbtest*/, bognsetts)
	if err != nil {
		panic(err)
	}
	index.Start()
	defer index.Close()

	lmdbenv, lmdbdbi, err := initlmdb(lmdbpath, lmdb.NoSync|lmdb.NoMetaSync)
	if err != nil {
		panic(err)
	}
	defer lmdbenv.Close()

	compareBognLmdb(index, lmdbenv, lmdbdbi)
}
