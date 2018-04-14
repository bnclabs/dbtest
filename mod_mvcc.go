package main

import "io"
import "os"
import "fmt"
import "sync"
import "time"
import "bytes"
import "strconv"
import "strings"
import "runtime"
import "unsafe"
import "sync/atomic"
import "math/rand"

import "github.com/bnclabs/golog"
import s "github.com/bnclabs/gosettings"
import "github.com/bnclabs/gostore/llrb"
import "github.com/bnclabs/gostore/api"
import "github.com/bmatsuo/lmdb-go/lmdb"
import "github.com/dgraph-io/badger"

type TestMVCC struct {
	old    *llrb.MVCC
	index  unsafe.Pointer // *llrb.MVCC
	mvccrw sync.RWMutex
}

func (t *TestMVCC) loadindex() *llrb.MVCC {
	return (*llrb.MVCC)(atomic.LoadPointer(&t.index))
}
func (t *TestMVCC) storeindex(index *llrb.MVCC) {
	atomic.StorePointer(&t.index, unsafe.Pointer(index))
}

func (t *TestMVCC) mvccwithlmdb() error {
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

	// new mvcc instance.
	mvccname, mvccsetts := "dbtest", llrb.Defaultsettings()
	index := llrb.NewMVCC("dbtest", mvccsetts)
	t.storeindex(index)

	// load index and reference with initial data.
	t.lmdbload(lmdbenv, lmdbdbi)
	// test index and reference read / write
	t.lmdbrw(mvccname, mvccsetts, index, lmdbenv, lmdbdbi)

	index.Log()
	index.Validate()

	if t.old != nil {
		t.old.Close()
		t.old.Destroy()
	}
	if index := t.loadindex(); index != nil {
		index.Close()
		index.Destroy()
	}

	fmt.Printf("Number of ROLLBACKS: %v\n", rollbacks)
	fmt.Printf("Number of conflicts: %v\n", conflicts)
	count, n := index.Count(), atomic.LoadInt64(&numentries)
	fmt.Printf("MVCC total indexed %v items, expected %v\n", count, n)

	return nil
}

func (t *TestMVCC) mvccwithbadg() error {
	seedl, seedc := int64(options.seed), int64(options.seed)+100
	fmt.Printf("Seed for load: %v, for ops: %v\n\n", seedl, seedc)

	pathdir := badgerpath()
	defer func() {
		if err := os.RemoveAll(pathdir); err != nil {
			log.Errorf("%v", err)
		}
	}()
	fmt.Printf("BADGER path %q\n", pathdir)

	// new badger instance.
	badg, err := initbadger(pathdir)
	if err != nil {
		panic(err)
	}
	defer badg.Close()

	// new mvcc instance.
	mvccname, mvccsetts := "dbtest", llrb.Defaultsettings()
	index := llrb.NewMVCC(mvccname, mvccsetts)
	t.storeindex(index)

	// load index and reference with initial data.
	klen, vlen := int64(options.keylen), int64(options.vallen)
	loadn := int64(options.load)
	t.badgload(badg, klen, vlen, loadn, seedl)
	// test index and reference read / write
	t.badgrw(mvccname, mvccsetts, index, badg)

	if t.old != nil {
		t.old.Close()
		t.old.Destroy()
	}
	if index := t.loadindex(); index != nil {
		index.Close()
		index.Destroy()
	}

	count, n := index.Count(), atomic.LoadInt64(&numentries)
	fmt.Printf("MVCC write conflicts %v\n", conflicts)
	fmt.Printf("MVCC total indexed %v items, expected %v\n", count, n)

	return nil
}

func (t *TestMVCC) lmdbload(lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {
	seedl := int64(options.seed)
	if err := t.mvccload(seedl); err != nil {
		panic(err)
	}
	seqno = 0
	if err := lmdbLoad(lmdbenv, lmdbdbi, seedl); err != nil {
		panic(err)
	}
	atomic.AddInt64(&numentries, -int64(options.load))
	atomic.AddInt64(&totalwrites, -int64(options.load))
}

func (t *TestMVCC) badgload(badg *badger.DB, klen, vlen, loadn, seedl int64) {
	if err := t.mvccload(seedl); err != nil {
		panic(err)
	}
	seqno = 0
	if err := badgerload(badg, klen, vlen, loadn, seedl); err != nil {
		panic(err)
	}
	atomic.AddInt64(&numentries, -int64(options.load))
	atomic.AddInt64(&totalwrites, -int64(options.load))
}

func (t *TestMVCC) mvccload(seedl int64) error {
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateloadr(
		klen, vlen, int64(options.load), int64(seedl), options.randwidth,
	)

	now, oldvalue := time.Now(), make([]byte, 16)
	opaque := atomic.AddUint64(&seqno, 1)
	key, value := g(make([]byte, 16), make([]byte, 16), opaque)
	for key != nil {
		index := t.loadindex()
		oldvalue, _ := index.Set(key, value, oldvalue)
		if len(oldvalue) > 0 {
			panic(fmt.Errorf("unexpected %q", oldvalue))
		}
		opaque = atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
	}
	atomic.AddInt64(&numentries, int64(options.load))
	atomic.AddInt64(&totalwrites, int64(options.load))

	index := t.loadindex()
	took := time.Since(now).Round(time.Second)
	fmt.Printf("Loaded MVCC %v items in %v\n\n", index.Count(), took)
	return nil
}

func (t *TestMVCC) lmdbrw(
	mvccname string, mvccsetts s.Settings,
	index *llrb.MVCC, lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {

	seedl, seedc := int64(options.seed), int64(options.seed)+100

	var wwg, rwg sync.WaitGroup
	fin := make(chan struct{})

	go t.lmdbvalidator(
		lmdbenv, lmdbdbi, true, seedl, mvccname, mvccsetts, &rwg, fin,
	)
	rwg.Add(1)

	// writer routines
	n := atomic.LoadInt64(&numentries)
	go t.lmdbCreater(lmdbenv, lmdbdbi, n, seedc, &wwg)
	go t.lmdbUpdater(lmdbenv, lmdbdbi, n, seedl, seedc, &wwg)
	go t.lmdbDeleter(lmdbenv, lmdbdbi, n, seedl, seedc, &wwg)
	//go t.lmdbSidechan(lmdbenv, lmdbdbi, n, seedl, seedc, &wwg)
	wwg.Add(3)
	// reader routines
	for i := 0; i < options.cpu; i++ {
		go t.lmdbGetter(lmdbenv, lmdbdbi, n, seedl, seedc, fin, &rwg)
		go t.Ranger(n, seedl, seedc, fin, &rwg)
		rwg.Add(2)
	}
	wwg.Wait()
	close(fin)
	rwg.Wait()
}

func (t *TestMVCC) badgrw(
	mvccname string, mvccsetts s.Settings,
	index *llrb.MVCC, badg *badger.DB) {

	seedl, seedc := int64(options.seed), int64(options.seed)+100

	var wwg, rwg sync.WaitGroup
	fin := make(chan struct{})

	// validator
	go t.badgvalidator(
		badg, true, seedl, mvccname, mvccsetts, &rwg, fin,
	)
	rwg.Add(1)
	// writer routines
	n := atomic.LoadInt64(&numentries)
	go t.badgCreater(badg, n, seedc, &wwg)
	go t.badgUpdater(badg, n, seedl, seedc, &wwg)
	go t.badgDeleter(badg, n, seedl, seedc, &wwg)
	wwg.Add(3)
	// reader routines
	for i := 0; i < options.cpu; i++ {
		go t.badgGetter(badg, n, seedl, seedc, fin, &rwg)
		go t.Ranger(n, seedl, seedc, fin, &rwg)
		rwg.Add(2)
	}
	wwg.Wait()
	close(fin)
	rwg.Wait()
}

func (t *TestMVCC) lmdbvalidator(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI, log bool, seedl int64,
	mvccname string, mvccsetts s.Settings,
	wg *sync.WaitGroup, fin chan struct{}) {

	defer wg.Done()

	rnd := rand.New(rand.NewSource(seedl))

	loadmvcc := func(index *llrb.MVCC) *llrb.MVCC {
		now := time.Now()
		iter := index.Scan()
		newindex := llrb.LoadMVCC(mvccname, mvccsetts, iter)
		iter(true /*fin*/)
		t.storeindex(newindex)
		thisseqno := index.Getseqno()
		newindex.Setseqno(thisseqno)
		fmsg := "Took %v to LoadMVCC index @ %v \n\n"
		took := time.Since(now).Round(time.Second)
		fmt.Printf(fmsg, took, thisseqno)

		if t.old != nil {
			t.old.Close()
			t.old.Destroy()
			t.old = index
		}

		return newindex
	}

	do := func() {
		index := t.loadindex()

		if log {
			fmt.Println()
			index.Log()
		}

		now := time.Now()
		index.Validate()
		took := time.Since(now).Round(time.Second)
		fmt.Printf("Took %v to validate index\n", took)

		func() {
			t.mvccrw.Lock()
			defer t.mvccrw.Unlock()

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

	tick := time.NewTicker(60 * time.Second)
	for {
		select {
		case <-tick.C:
			do()
		case <-fin:
			return
		}
	}
}

func (t *TestMVCC) badgvalidator(
	badg *badger.DB, log bool, seedl int64,
	mvccname string, mvccsetts s.Settings,
	wg *sync.WaitGroup, fin chan struct{}) {

	defer wg.Done()

	rnd := rand.New(rand.NewSource(seedl))

	loadmvcc := func(index *llrb.MVCC) *llrb.MVCC {
		now := time.Now()
		iter := index.Scan()
		newindex := llrb.LoadMVCC(mvccname, mvccsetts, iter)
		iter(true /*fin*/)
		t.storeindex(newindex)
		thisseqno := index.Getseqno()
		newindex.Setseqno(thisseqno)
		fmsg := "Took %v to LoadMVCC index @ %v \n\n"
		took := time.Since(now).Round(time.Second)
		fmt.Printf(fmsg, took, thisseqno)

		if t.old != nil {
			t.old.Close()
			t.old.Destroy()
			t.old = index
		}

		return newindex
	}

	do := func() {
		index := t.loadindex()
		if log {
			fmt.Println()
			index.Log()
		}

		now := time.Now()
		index.Validate()
		took := time.Since(now).Round(time.Second)
		fmt.Printf("Took %v to validate index\n\n", took)

		func() {
			t.mvccrw.Lock()
			defer t.mvccrw.Unlock()

			syncsleep(mvccsetts)
			compareMvccBadger(index, badg)
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

	tick := time.NewTicker(60 * time.Second)
	for {
		select {
		case <-tick.C:
			do()
		case <-fin:
			return
		}
	}
}

func (t *TestMVCC) lmdbCreater(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	n, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()
	sets := []func(index *llrb.MVCC, k, v, ov []byte) (uint64, []byte){
		t.set1, t.set2, t.set3, t.set4,
	}

	klen, vlen := int64(options.keylen), int64(options.vallen)
	writes := int64(options.writes)
	g := Generatecreate(klen, vlen, n, writes, seedc, options.randwidth)

	key, value := make([]byte, 16), make([]byte, 16)
	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(100000)

	do := func() error {
		t.mvccrw.RLock()
		defer t.mvccrw.RUnlock()

		index := t.loadindex()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		setidx := rnd.Intn(1000000) % len(sets)
		refcas, _ := sets[setidx](index, key, value, oldvalue)
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
			fmsg := "Creater {%v items in %v} {%v items in %v} count:%v\n"
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
	fmsg := "at exit, Creater %v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, atomic.LoadInt64(&ncreates), took)
}

func (t *TestMVCC) badgCreater(
	badg *badger.DB, n, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	sets := []func(index *llrb.MVCC, k, v, ov []byte) (uint64, []byte){
		t.set1, t.set2, t.set3, t.set4,
	}

	klen, vlen := int64(options.keylen), int64(options.vallen)
	writes := int64(options.writes)
	g := Generatecreate(klen, vlen, n, writes, seedc, options.randwidth)

	key, value := make([]byte, 16), make([]byte, 16)
	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(100000)

	do := func() error {
		t.mvccrw.RLock()
		defer t.mvccrw.RUnlock()

		index := t.loadindex()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		setidx := rnd.Intn(1000000) % 4
		refcas, _ := sets[setidx](index, key, value, oldvalue)
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
			fmsg := "Creater {%v items in %v} {%v items in %v} count:%v\n"
			fmt.Printf(fmsg, markercount, x, nc, y, count)
			now = time.Now()
		}
		// update the badger object
		if err := badgDocreate(badg, key, value); err != nil {
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
	fmsg := "at exit, Creater %v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, atomic.LoadInt64(&ncreates), took)
}

func (t *TestMVCC) lmdbUpdater(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()
	sets := []func(index *llrb.MVCC, k, v, ov []byte) (uint64, []byte){
		t.set1, t.set2, t.set3, t.set4,
	}

	var nupdates int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	writes := int64(options.writes)
	g := Generateupdate(
		klen, vlen, n, writes, seedl, seedc, -1, options.randwidth,
	)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(100000)

	do := func() error {
		t.mvccrw.RLock()
		defer t.mvccrw.RUnlock()

		index := t.loadindex()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		setidx := rnd.Intn(1000000) % len(sets)
		for i := 2; i >= 0; i-- {
			refcas, oldvalue := sets[setidx](index, key, value, oldvalue)
			if len(oldvalue) == 0 {
				atomic.AddInt64(&numentries, 1)
			}
			oldvalue, cas, del, ok := index.Get(key, oldvalue)
			rc := t.verifyupdater(key, oldvalue, refcas, cas, i, del, ok)
			if rc == "ok" {
				break
			}
		}

		atomic.AddInt64(&totalwrites, 1)
		if nupdates = nupdates + 1; nupdates%markercount == 0 {
			count := index.Count()
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "Updater {%v items in %v} {%v items in %v} count:%v\n"
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
	fmsg := "at exit, Updater %v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, nupdates, took)
}

func (t *TestMVCC) badgUpdater(
	badg *badger.DB,
	n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	sets := []func(index *llrb.MVCC, k, v, ov []byte) (uint64, []byte){
		t.set1, t.set2, t.set3, t.set4,
	}

	var nupdates int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	writes := int64(options.writes)
	g := Generateupdate(
		klen, vlen, n, writes, seedl, seedc, -1, options.randwidth,
	)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(100000)

	do := func() error {
		t.mvccrw.RLock()
		defer t.mvccrw.RUnlock()

		index := t.loadindex()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		setidx := rnd.Intn(1000000) % 4
		for i := 2; i >= 0; i-- {
			refcas, oldvalue := sets[setidx](index, key, value, oldvalue)
			if len(oldvalue) == 0 {
				atomic.AddInt64(&numentries, 1)
			}
			oldvalue, cas, del, ok := index.Get(key, oldvalue)
			rc := t.verifyupdater(key, oldvalue, refcas, cas, i, del, ok)
			if rc == "ok" {
				break
			}
		}

		atomic.AddInt64(&totalwrites, 1)
		if nupdates = nupdates + 1; nupdates%markercount == 0 {
			count := index.Count()
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "Updater {%v items in %v} {%v items in %v} count:%v\n"
			fmt.Printf(fmsg, markercount, x, nupdates, y, count)
			now = time.Now()
		}
		// update the badger updater.
		_, err := badgDoupdate(badg, key, value)
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
	fmsg := "at exit, Updater %v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, nupdates, took)
}

func (t *TestMVCC) verifyupdater(
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

func (t *TestMVCC) set1(
	index *llrb.MVCC, key, value, oldvalue []byte) (uint64, []byte) {

	oldvalue, cas := index.Set(key, value, oldvalue)
	//fmt.Printf("update1 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 {
		comparekeyvalue(key, oldvalue, options.vallen)
	}
	return cas, oldvalue
}

func (t *TestMVCC) set2(
	index *llrb.MVCC, key, value, oldvalue []byte) (uint64, []byte) {

	for i := 3; i >= 0; i-- {
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
		//fmt.Printf("mvccSet2 %q %q %v %v\n", key, value, oldcas, err)
		if t.verifyset2(err, i, key, oldvalue) == "ok" {
			return cas, oldvalue
		}
	}
	panic("unreachable code")
}

func (t *TestMVCC) verifyset2(err error, i int, key, oldvalue []byte) string {
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

func (t *TestMVCC) set3(
	index *llrb.MVCC, key, value, oldvalue []byte) (uint64, []byte) {

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

func (t *TestMVCC) set4(
	index *llrb.MVCC, key, value, oldvalue []byte) (uint64, []byte) {

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

func (t *TestMVCC) lmdbDeleter(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()
	dels := []func(*llrb.MVCC, []byte, []byte, bool) (uint64, bool){
		t.del1, t.del2, t.del3, t.del4,
	}

	time.Sleep(1 * time.Second) // delay start for mvccUpdater to catchup.

	var ndeletes, xdeletes int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	writes := int64(options.writes)
	g := Generatedelete(
		klen, vlen, n, writes, seedl, seedc, delmod, options.randwidth,
	)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(100000)
	lsmmap := map[int]bool{0: true, 1: false}

	do := func() error {
		t.mvccrw.RLock()
		defer t.mvccrw.RUnlock()

		index := t.loadindex()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		//fmt.Printf("delete %q\n", key)
		ln := len(dels)
		delidx, lsm := rnd.Intn(1000000)%ln, lsmmap[rnd.Intn(1000000)%2]
		if lsm {
			delidx = delidx % 2
		}
		//fmt.Printf("delete %q %v %v\n", key, lsm, delidx)
		var ok1 bool
		var refcas uint64
		for i := 2; i >= 0; i-- {
			refcas, ok1 = dels[delidx](index, key, value, lsm)
			oldvalue, _, _, ok2 := index.Get(key, oldvalue)
			rc := t.vmvccdel(index, key, oldvalue, refcas, i, lsm, ok2)
			if rc == "ok" {
				if ok1 == false {
					xdeletes++
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
			fmsg := "Deleter {%v items in %v} {%v:%v items in %v} cnt:%v\n"
			fmt.Printf(fmsg, markercount, x, ndeletes, xdeletes, y, count)
			now = time.Now()
		}

		// update lmdb
		if ok1 {
			_, err := lmdbDodelete(lmdbenv, lmdbdbi, key, value)
			if err != nil {
				if strings.Contains(err.Error(), lmdbmissingerr) {
					return nil
				}
				panic(err)
			}
		}
		return nil
	}

	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		if err := do(); err != nil {
			return
		}
		runtime.Gosched()
	}
	fmsg := "at exit, Deleter %v:%v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, ndeletes, xdeletes, took)
}

func (t *TestMVCC) badgDeleter(
	badg *badger.DB,
	n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()
	dels := []func(*llrb.MVCC, []byte, []byte, bool) (uint64, bool){
		t.del1, t.del2, t.del3, t.del4,
	}

	time.Sleep(1 * time.Second) // delay start for mvccUpdater to catchup.

	var ndeletes, xdeletes int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	writes := int64(options.writes)
	g := Generatedelete(
		klen, vlen, n, writes, seedl, seedc, delmod, options.randwidth,
	)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(100000)
	lsmmap := map[int]bool{0: true, 1: false}

	do := func() error {
		t.mvccrw.RLock()
		defer t.mvccrw.RUnlock()

		index := t.loadindex()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		ln := len(dels)
		delidx, lsm := rnd.Intn(1000000)%ln, lsmmap[rnd.Intn(1000000)%2]
		if lsm {
			delidx = delidx % 2
		}
		//fmt.Printf("delete %q %v %v\n", key, lsm, delidx)
		var ok1 bool
		var refcas uint64
		for i := 2; i >= 0; i-- {
			refcas, ok1 = dels[delidx](index, key, value, lsm)
			oldvalue, _, _, ok2 := index.Get(key, oldvalue)
			rc := t.vmvccdel(index, key, oldvalue, refcas, i, lsm, ok2)
			if rc == "ok" {
				if ok1 == false {
					xdeletes++
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
			fmsg := "Deleter {%v items %v} {%v:%v items in %v} count:%v\n"
			fmt.Printf(fmsg, markercount, x, ndeletes, xdeletes, y, count)
			now = time.Now()
		}

		// do delete on badger
		if _, err := badgDodelete(badg, key, value); err != nil {
			if strings.Contains(err.Error(), badgkeymissing) {
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
	fmsg := "at exit, Deleter %v:%v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, ndeletes, xdeletes, took)
}

func (t *TestMVCC) vmvccdel(
	index *llrb.MVCC, key, oldvalue []byte, refcas uint64,
	i int, lsm, ok bool) string {

	var err error
	if lsm {
		view := index.View(0x1234)

		cur, err := view.OpenCursor(key)
		if err == nil {
			key1, value, cas, del, err := cur.YNext(false)
			if err == io.EOF || bytes.Compare(key1, key) != 0 {
				// TODO: handle this case !!
			} else if err != nil {
				panic(err)
			} else if del == false {
				err = fmt.Errorf("expected delete")
			} else if refcas > 0 && cas != refcas {
				err = fmt.Errorf("expected %v, got %v", refcas, cas)
			} else if len(value) > 0 {
				comparekeyvalue(key, value, options.vallen)
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

func (t *TestMVCC) del1(
	index *llrb.MVCC, key, oldvalue []byte, lsm bool) (uint64, bool) {

	//fmt.Printf("mvccDel1 %q %v\n", key, lsm)
	oldvalue, cas := index.Delete(key, oldvalue, lsm)
	if len(oldvalue) > 0 {
		comparekeyvalue(key, oldvalue, options.vallen)
	}
	return cas, true
}

func (t *TestMVCC) del2(
	index *llrb.MVCC, key, oldvalue []byte, lsm bool) (uint64, bool) {

	for i := numcpus * 2; i >= 0; i-- {
		txn := index.BeginTxn(0xC0FFEE)
		oldvalue = txn.Delete(key, oldvalue, lsm)
		if len(oldvalue) > 0 {
			comparekeyvalue(key, oldvalue, options.vallen)
		}
		err := txn.Commit()
		if err == nil {
			return 0, true
		} else if err.Error() == api.ErrorRollback.Error() {
			atomic.AddInt64(&rollbacks, 1)
		} else {
			panic(err)
		}
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
	panic("too many rollbacks")
}

func (t *TestMVCC) del3(
	index *llrb.MVCC, key, oldvalue []byte, lsm bool) (uint64, bool) {

	for i := numcpus * 2; i >= 0; i-- {
		txn := index.BeginTxn(0xC0FFEE)
		cur, err := txn.OpenCursor(key)
		if err != nil {
			panic(err)
		}
		oldvalue = cur.Delete(key, oldvalue, lsm)
		if len(oldvalue) > 0 {
			comparekeyvalue(key, oldvalue, options.vallen)
		}
		err = txn.Commit()
		if err == nil {
			return 0, true
		} else if err.Error() == api.ErrorRollback.Error() {
			atomic.AddInt64(&rollbacks, 1)
		} else {
			panic(err)
		}
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
	panic("too many rollbacks")
}

func (t *TestMVCC) del4(
	index *llrb.MVCC, key, oldvalue []byte, lsm bool) (uint64, bool) {

	for i := numcpus * 2; i >= 0; i-- {
		txn := index.BeginTxn(0xC0FFEE)
		cur, err := txn.OpenCursor(key)
		if err != nil {
			panic(err)
		}
		curkey, _ := cur.Key()
		if bytes.Compare(key, curkey) != 0 {
			txn.Abort()
			return 0, false
		}
		cur.Delcursor(lsm)
		err = txn.Commit()
		if err == nil {
			return 0, true
		} else if err.Error() == api.ErrorRollback.Error() {
			atomic.AddInt64(&rollbacks, 1)
		} else {
			panic(err)
		}
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
	panic("too many rollbacks")
}

var sidekey = []byte("00000011000000000000000002396386")
var sideval = []byte("00000011000000000000000002396386u\x00\x00\x00\x00\x0fBE")

func (t *TestMVCC) lmdbSidechan(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	oldvalue := make([]byte, 16)
	do := func() error {
		t.mvccrw.RLock()
		defer t.mvccrw.RUnlock()

		// update
		index := t.loadindex()
		t.set1(index, sidekey, sideval, oldvalue)
		_, err := lmdbDoupdate(lmdbenv, lmdbdbi, sidekey, sideval)
		if err != nil {
			panic(err)
		}
		// delete
		t.del1(index, sidekey, sideval, options.lsm)
		_, err = lmdbDodelete(lmdbenv, lmdbdbi, sidekey, sideval)
		if err != nil {
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
}

func (t *TestMVCC) lmdbGetter(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	n, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	defer wg.Done()

	gets := []func(*lmdb.Env, lmdb.DBI, *llrb.MVCC,
		[]byte, []byte) ([]byte, uint64, bool, bool){

		t.lmdbGet1, t.lmdbGet2, t.lmdbGet3,
	}

	var ngets, nmisses int64
	var key []byte
	var del bool
	writes := int64(options.writes)
	g := Generateread(
		int64(options.keylen), n, writes, seedl, seedc, options.randwidth,
	)

	rnd := rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(10000000)
	value := make([]byte, 16)
loop:
	for {
		index := t.loadindex()
		ngets++
		key = g(key, atomic.LoadInt64(&ncreates))
		get := gets[rnd.Intn(1000000)%len(gets)]
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
			fmsg := "Getter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
			now = time.Now()
		}

		runtime.Gosched()
	}
	took := time.Since(epoch).Round(time.Second)
	<-fin
	fmsg := "at exit, Getter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, took)
}

func (t *TestMVCC) lmdbGet1(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *llrb.MVCC, key, value []byte) ([]byte, uint64, bool, bool) {

	var mvccval []byte
	var seqno uint64
	var del, ok bool

	get := func(txn *lmdb.Txn) (err error) {
		var lmdbval []byte

		mvccval, seqno, del, ok = index.Get(key, value)
		if lmdbval, err = txn.Get(lmdbdbi, key); err == nil {
			if del == false && options.vallen > 0 {
				if bytes.Compare(mvccval, lmdbval) != 0 {
					fmsg := "retry: expected %q, got %q"
					return fmt.Errorf(fmsg, lmdbval, mvccval)
				}
			}

		} else if del && strings.Contains(err.Error(), lmdbmissingerr) {
			return nil
		} else if strings.Contains(err.Error(), lmdbmissingerr) {
			return fmt.Errorf("retry: %v", err)
		}
		return err
	}
	trylmdbget(lmdbenv, 5000, get)

	return mvccval, seqno, del, ok
}

func (t *TestMVCC) lmdbGet2(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *llrb.MVCC, key, value []byte) ([]byte, uint64, bool, bool) {

	var mvccval []byte
	var del, ok bool
	var mvcctxn api.Transactor

	get := func(txn *lmdb.Txn) (err error) {
		var lmdbval []byte

		mvcctxn = index.BeginTxn(0xC0FFEE)
		mvccval, _, del, ok = mvcctxn.Get(key, value)

		if lmdbval, err = txn.Get(lmdbdbi, key); err == nil {
			if del == false && options.vallen > 0 {
				if bytes.Compare(mvccval, lmdbval) != 0 {
					mvcctxn.Abort()
					fmsg := "retry: expected %q, got %q"
					return fmt.Errorf(fmsg, lmdbval, mvccval)
				}
			}
		} else if del && strings.Contains(err.Error(), lmdbmissingerr) {
			return nil
		} else if strings.Contains(err.Error(), lmdbmissingerr) {
			mvcctxn.Abort()
			return fmt.Errorf("retry: %v", err)
		} else if err != nil {
			mvcctxn.Abort()
			return err
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

func (t *TestMVCC) lmdbGet3(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	index *llrb.MVCC, key, value []byte) ([]byte, uint64, bool, bool) {

	var mvccval []byte
	var del, ok bool
	var view api.Transactor

	get := func(txn *lmdb.Txn) (err error) {
		var lmdbval []byte

		view = index.View(0x1235)
		mvccval, _, del, ok = view.Get(key, value)

		if lmdbval, err = txn.Get(lmdbdbi, key); err == nil {
			if del == false && options.vallen > 0 {
				if bytes.Compare(mvccval, lmdbval) != 0 {
					view.Abort()
					fmsg := "retry: expected %q, got %q"
					return fmt.Errorf(fmsg, lmdbval, mvccval)
				}
			}
		} else if del && strings.Contains(err.Error(), lmdbmissingerr) {
			return nil
		} else if strings.Contains(err.Error(), lmdbmissingerr) {
			view.Abort()
			return fmt.Errorf("retry: %v", err)
		} else if err != nil {
			view.Abort()
			return err
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

func (t *TestMVCC) badgGetter(
	badg *badger.DB,
	n, seedl, seedc int64, fin chan struct{}, wg *sync.WaitGroup) {

	defer wg.Done()

	gets := []func(*badger.DB, *llrb.MVCC,
		[]byte, []byte) ([]byte, uint64, bool, bool){

		t.badgGet1, t.badgGet2, t.badgGet3,
	}

	var ngets, nmisses int64
	var key []byte
	var del bool
	writes := int64(options.writes)
	g := Generateread(
		int64(options.keylen), n, writes, seedl, seedc, options.randwidth,
	)

	rnd := rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(10000000)
	value := make([]byte, 16)
loop:
	for {
		index := t.loadindex()
		ngets++
		key = g(key, atomic.LoadInt64(&ncreates))
		get := gets[(rnd.Intn(1000000) % len(gets))]
		value, _, del, _ = get(badg, index, key, value)
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
			fmsg := "Getter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
		}

		runtime.Gosched()
	}
	took := time.Since(epoch).Round(time.Second)
	<-fin
	fmsg := "at exit, Getter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, took)
}

func (t *TestMVCC) badgGet1(
	badg *badger.DB,
	index *llrb.MVCC, key, value []byte) ([]byte, uint64, bool, bool) {

	var mvccval []byte
	var seqno uint64
	var del, ok bool

	get := func(txn *badger.Txn) error {
		mvccval, seqno, del, ok = index.Get(key, value)
		item, _ := txn.Get(key)
		if del == false && options.vallen > 0 && item != nil {
			badgval, _ := item.Value()
			if bytes.Compare(mvccval, badgval) != 0 {
				fmsg := "retry: expected %q, got %q"
				return fmt.Errorf(fmsg, badgval, mvccval)
			}
		}
		return nil
	}
	trybadgget(badg, 5000, get)

	return mvccval, seqno, del, ok
}

func (t *TestMVCC) badgGet2(
	badg *badger.DB,
	index *llrb.MVCC, key, value []byte) ([]byte, uint64, bool, bool) {

	var mvccval []byte
	var del, ok bool
	var mvcctxn api.Transactor

	get := func(txn *badger.Txn) (err error) {
		mvcctxn = index.BeginTxn(0xC0FFEE)
		mvccval, _, del, ok = mvcctxn.Get(key, value)

		item, err := txn.Get(key)
		if del == false && options.vallen > 0 && item != nil {
			badgval, _ := item.Value()
			if bytes.Compare(mvccval, badgval) != 0 {
				mvcctxn.Abort()
				fmsg := "retry: expected %q, got %q"
				return fmt.Errorf(fmsg, badgval, mvccval)
			}
		}
		return nil
	}
	trybadgget(badg, 5000, get)

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

func (t *TestMVCC) badgGet3(
	badg *badger.DB,
	index *llrb.MVCC, key, value []byte) ([]byte, uint64, bool, bool) {

	var mvccval []byte
	var del, ok bool
	var view api.Transactor

	get := func(txn *badger.Txn) (err error) {
		view = index.View(0x1235)
		mvccval, _, del, ok = view.Get(key, value)

		item, err := txn.Get(key)
		if del == false && options.vallen > 0 && item != nil {
			badgval, _ := item.Value()
			if bytes.Compare(mvccval, badgval) != 0 {
				view.Abort()
				fmsg := "retry: expected %q, got %q"
				return fmt.Errorf(fmsg, badgval, mvccval)
			}
		}
		return nil
	}
	trybadgget(badg, 5000, get)

	if ok == true {
		cur, err := view.OpenCursor(key)
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
	view.Abort()

	return mvccval, 0, del, ok
}

func (t *TestMVCC) Ranger(
	n, seedl, seedc int64, fin chan struct{}, wg *sync.WaitGroup) {

	defer wg.Done()

	rngs := []func(index *llrb.MVCC, key, val []byte) int64{
		t.range1, t.range2, t.range3, t.range4,
	}

	var nranges int64
	var key []byte
	writes := int64(options.writes)
	g := Generateread(
		int64(options.keylen), n, writes, seedl, seedc, options.randwidth,
	)

	rnd := rand.New(rand.NewSource(seedc))
	epoch, value := time.Now(), make([]byte, 16)
loop:
	for {
		index := t.loadindex()
		key = g(key, atomic.LoadInt64(&ncreates))
		dorange := rngs[rnd.Intn(1000000)%len(rngs)]
		n := dorange(index, key, value)
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
	fmt.Printf("at exit, Ranger %v items in %v\n", nranges, took)
}

func (t *TestMVCC) range1(index *llrb.MVCC, key, value []byte) (n int64) {
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
		} else if (int64(x)%updtdel) != delmod && del == true {
			panic("unexpected delete")
		}
		comparekeyvalue(key, value, options.vallen)
		n++
	}
	txn.Abort()
	return
}

func (t *TestMVCC) range2(index *llrb.MVCC, key, value []byte) (n int64) {
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

func (t *TestMVCC) range3(index *llrb.MVCC, key, value []byte) (n int64) {
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

func (t *TestMVCC) range4(index *llrb.MVCC, key, value []byte) (n int64) {
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

func compareMvccLmdb(index *llrb.MVCC, lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {
	lmdbcount := getlmdbCount(lmdbenv, lmdbdbi)
	mvcccount := index.Count()
	fmsg := "compareMvccLmdb, lmdbcount:%v mvcccount:%v\n"
	fmt.Printf(fmsg, lmdbcount, mvcccount)

	epoch, cmpcount := time.Now(), 0

	//fmt.Println("mvcc seqno", index.Getseqno())
	iter := index.Scan()
	err := lmdbenv.View(func(txn *lmdb.Txn) error {
		lmdbcur, err := txn.OpenCursor(lmdbdbi)
		if err != nil {
			panic(err)
		}

		mvcckey, mvccval, _, mvccdel, mvccerr := iter(false /*fin*/)
		lmdbkey, lmdbval, lmdberr := lmdbcur.Get(nil, nil, lmdb.Next)

		for mvcckey != nil {
			if mvccdel == false {
				cmpcount++
				if mvccerr != nil {
					panic(mvccerr)
				} else if lmdberr != nil {
					panic(lmdberr)
				} else if bytes.Compare(mvcckey, lmdbkey) != 0 {
					val, seqno, del, ok := index.Get(lmdbkey, make([]byte, 0))
					fmt.Printf("%q %v %v %v\n", val, seqno, del, ok)
					val, seqno, del, ok = index.Get(mvcckey, make([]byte, 0))
					fmt.Printf("%q %v %v %v\n", val, seqno, del, ok)

					fmsg := "expected %q,%q, got %q,%q,%v"
					x, y, z := mvcckey, mvccval, mvccdel
					panic(fmt.Errorf(fmsg, lmdbkey, lmdbval, x, y, z))
				} else if bytes.Compare(mvccval, lmdbval) != 0 {
					fmsg := "for %q expected %q, got %q"
					panic(fmt.Errorf(fmsg, mvcckey, lmdbval, mvccval))
				}
				//fmt.Printf("compareMvccLmdb %q okay ...\n", llrbkey)
				lmdbkey, lmdbval, lmdberr = lmdbcur.Get(nil, nil, lmdb.Next)
			}
			mvcckey, mvccval, _, mvccdel, mvccerr = iter(false /*fin*/)
		}
		if lmdbkey != nil {
			return fmt.Errorf("found lmdb key %q\n", lmdbkey)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	iter(true /*fin*/)

	took := time.Since(epoch).Round(time.Second)
	fmt.Printf("Took %v to compare (%v) MVCC and LMDB\n\n", took, cmpcount)
}

func compareMvccBadger(index *llrb.MVCC, badg *badger.DB) {
	epoch, cmpcount := time.Now(), 0

	iter := index.Scan()
	err := badg.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		it.Rewind()

		for it.Valid() {
			mvcckey, mvccval, _, mvccdel, mvccerr := iter(false /*fin*/)
			item := it.Item()
			badgkey := item.Key()
			badgval, badgerr := item.Value()

			if mvccdel == false {
				cmpcount++
				if mvccerr != nil {
					panic(mvccerr)

				} else if badgerr != nil {
					panic(badgerr)

				} else if bytes.Compare(mvcckey, badgkey) != 0 {
					fmsg := "expected %q,%q, got %q,%q"
					panic(fmt.Errorf(fmsg, badgkey, badgval, mvcckey, mvccval))

				} else if bytes.Compare(mvccval, badgval) != 0 {
					fmsg := "for %q expected val %q, got val %q\n"
					x, y := badgval[:options.vallen], mvccval[:options.vallen]
					fmt.Printf(fmsg, mvcckey, x, y)
					fmsg = "for %q expected seqno %v, got %v\n"
					x, y = badgval[options.vallen:], mvccval[options.vallen:]
					fmt.Printf(fmsg, mvcckey, badgval, mvccval)
					panic("error")
				}
				it.Next()
			}
		}
		mvcckey, _, _, mvccdel, mvccerr := iter(false /*fin*/)
		for mvccerr != io.EOF && mvccdel == true {
			mvcckey, _, _, mvccdel, mvccerr = iter(false /*fin*/)
		}
		if mvccerr != io.EOF {
			panic(fmt.Errorf("mvcc items remaining %q", mvcckey))
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	iter(true /*fin*/)

	took := time.Since(epoch).Round(time.Second)
	fmt.Printf("Took %v to compare (%v) MVCC and BADGER\n\n", took, cmpcount)
}
