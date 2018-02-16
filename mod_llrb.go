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
import "github.com/dgraph-io/badger"

type TestLLRB struct {
	old    *llrb.LLRB
	index  unsafe.Pointer // *llrb.LLRB
	llrbrw sync.RWMutex
}

func (t *TestLLRB) loadindex() *llrb.LLRB {
	return (*llrb.LLRB)(atomic.LoadPointer(&t.index))
}
func (t *TestLLRB) storeindex(index *llrb.LLRB) {
	atomic.StorePointer(&t.index, unsafe.Pointer(index))
}

func (t *TestLLRB) llrbwithlmdb() error {
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
	t.storeindex(index)

	// load index and reference with initial data.
	t.lmdbload(lmdbenv, lmdbdbi)
	// test index and reference read / write
	t.lmdbrw(llrbname, llrbsetts, index, lmdbenv, lmdbdbi)

	if t.old != nil {
		t.old.Close()
		t.old.Destroy()
	}
	if index := t.loadindex(); index != nil {
		index.Close()
		index.Destroy()
	}

	count, n := index.Count(), atomic.LoadInt64(&numentries)
	fmt.Printf("LLRB write conflicts %v\n", conflicts)
	fmt.Printf("LLRB total indexed %v items, expected %v\n", count, n)

	return nil
}

func (t *TestLLRB) llrbwithbadg() error {
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

	// new llrb instance.
	llrbname, llrbsetts := "dbtest", llrb.Defaultsettings()
	index := llrb.NewLLRB(llrbname, llrbsetts)
	t.storeindex(index)

	// load index and reference with initial data.
	klen, vlen := int64(options.keylen), int64(options.vallen)
	loadn := int64(options.load)
	t.badgload(badg, klen, vlen, loadn, seedl)
	// test index and reference read / write
	t.badgrw(llrbname, llrbsetts, index, badg)

	if t.old != nil {
		t.old.Close()
		t.old.Destroy()
	}
	if index := t.loadindex(); index != nil {
		index.Close()
		index.Destroy()
	}

	count, n := index.Count(), atomic.LoadInt64(&numentries)
	fmt.Printf("LLRB write conflicts %v\n", conflicts)
	fmt.Printf("LLRB total indexed %v items, expected %v\n", count, n)

	return nil
}

func (t *TestLLRB) lmdbload(lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {
	seedl := int64(options.seed)
	if err := t.llrbload(seedl); err != nil {
		panic(err)
	}
	seqno = 0
	if err := lmdbLoad(lmdbenv, lmdbdbi, seedl); err != nil {
		panic(err)
	}
	atomic.AddInt64(&numentries, -int64(options.load))
	atomic.AddInt64(&totalwrites, -int64(options.load))
}

func (t *TestLLRB) badgload(badg *badger.DB, klen, vlen, loadn, seedl int64) {
	if err := t.llrbload(seedl); err != nil {
		panic(err)
	}
	seqno = 0
	if err := badgerload(badg, klen, vlen, loadn, seedl); err != nil {
		panic(err)
	}
	atomic.AddInt64(&numentries, -int64(options.load))
	atomic.AddInt64(&totalwrites, -int64(options.load))
}

func (t *TestLLRB) llrbload(seedl int64) error {
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateloadr(klen, vlen, int64(options.load), int64(seedl))

	now, oldvalue := time.Now(), make([]byte, 16)
	opaque := atomic.AddUint64(&seqno, 1)
	key, value := g(make([]byte, 16), make([]byte, 16), opaque)
	for ; key != nil; key, value = g(key, value, opaque) {
		index := t.loadindex()
		oldvalue, _ := index.Set(key, value, oldvalue)
		if len(oldvalue) > 0 {
			panic(fmt.Errorf("unexpected %q", oldvalue))
		}
		opaque = atomic.AddUint64(&seqno, 1)
	}
	atomic.AddInt64(&numentries, int64(options.load))
	atomic.AddInt64(&totalwrites, int64(options.load))

	index := t.loadindex()
	took := time.Since(now).Round(time.Second)
	fmt.Printf("Loaded LLRB %v items in %v\n\n", index.Count(), took)
	return nil
}

func (t *TestLLRB) lmdbrw(
	llrbname string, llrbsetts s.Settings,
	index *llrb.LLRB, lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {

	seedl, seedc := int64(options.seed), int64(options.seed)+100

	var wwg, rwg sync.WaitGroup
	fin := make(chan struct{})

	// validator
	go t.lmdbvalidator(
		lmdbenv, lmdbdbi, true, seedl, llrbname, llrbsetts, &rwg, fin,
	)
	rwg.Add(1)
	// writer routines
	n := atomic.LoadInt64(&numentries)
	go t.lmdbCreater(lmdbenv, lmdbdbi, n, seedc, &wwg)
	go t.lmdbUpdater(lmdbenv, lmdbdbi, n, seedl, seedc, &wwg)
	go t.lmdbDeleter(lmdbenv, lmdbdbi, n, seedl, seedc, &wwg)
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

func (t *TestLLRB) badgrw(
	llrbname string, llrbsetts s.Settings,
	index *llrb.LLRB, badg *badger.DB) {

	seedl, seedc := int64(options.seed), int64(options.seed)+100

	var wwg, rwg sync.WaitGroup
	fin := make(chan struct{})

	// validator
	go t.badgvalidator(
		badg, true, seedl, llrbname, llrbsetts, &rwg, fin,
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

func (t *TestLLRB) lmdbvalidator(
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
		t.storeindex(newindex)
		thisseqno := index.Getseqno()
		newindex.Setseqno(thisseqno)
		fmsg := "Took %v to LoadLLRB index @ %v \n\n"
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
			t.llrbrw.Lock()
			defer t.llrbrw.Unlock()

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

	tick := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-tick.C:
			do()
		case <-fin:
			return
		}
	}
}

func (t *TestLLRB) badgvalidator(
	badg *badger.DB, log bool, seedl int64,
	llrbname string, llrbsetts s.Settings,
	wg *sync.WaitGroup, fin chan struct{}) {

	defer wg.Done()

	rnd := rand.New(rand.NewSource(seedl))

	loadllrb := func(index *llrb.LLRB) *llrb.LLRB {
		now := time.Now()
		iter := index.Scan()
		newindex := llrb.LoadLLRB(llrbname, llrbsetts, iter)
		iter(true /*fin*/)
		t.storeindex(newindex)
		thisseqno := index.Getseqno()
		newindex.Setseqno(thisseqno)
		fmsg := "Took %v to LoadLLRB index @ %v \n\n"
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
			t.llrbrw.Lock()
			defer t.llrbrw.Unlock()

			compareLlrbBadger(index, badg)
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

	tick := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-tick.C:
			do()
		case <-fin:
			return
		}
	}
}

func (t *TestLLRB) lmdbCreater(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	n, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	sets := []func(index *llrb.LLRB, k, v, ov []byte) (uint64, []byte){
		t.set1, t.set2, t.set3, t.set4,
	}

	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generatecreate(klen, vlen, n, seedc)

	key, value := make([]byte, 16), make([]byte, 16)
	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)

	do := func() error {
		t.llrbrw.RLock()
		defer t.llrbrw.RUnlock()

		index := t.loadindex()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		set := sets[rnd.Intn(1000000)%4]
		refcas, _ := set(index, key, value, oldvalue)
		oldvalue, cas, del, ok := index.Get(key, oldvalue[:0])
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
			fmsg := "Creater {%v items in %v} {%v items in %v} " +
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
	fmsg := "at exit, Creater %v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, atomic.LoadInt64(&ncreates), took)
}

func (t *TestLLRB) badgCreater(
	badg *badger.DB, n, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	sets := []func(index *llrb.LLRB, k, v, ov []byte) (uint64, []byte){
		t.set1, t.set2, t.set3, t.set4,
	}

	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generatecreate(klen, vlen, n, seedc)

	key, value := make([]byte, 16), make([]byte, 16)
	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)

	do := func() error {
		t.llrbrw.RLock()
		defer t.llrbrw.RUnlock()

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

func (t *TestLLRB) lmdbUpdater(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	sets := []func(index *llrb.LLRB, k, v, ov []byte) (uint64, []byte){
		t.set1, t.set2, t.set3, t.set4,
	}

	var nupdates int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateupdate(klen, vlen, n, seedl, seedc, -1)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)

	do := func() error {
		t.llrbrw.RLock()
		defer t.llrbrw.RUnlock()

		index := t.loadindex()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		setidx := rnd.Intn(1000000) % 4
		for i := 2; i >= 0; i-- {
			set := sets[setidx]
			refcas, oldvalue := set(index, key, value, oldvalue)
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
			fmsg := "Updater {%v items in %v} {%v items in %v} " +
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
	fmsg := "at exit, Updater %v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, nupdates, took)
}

func (t *TestLLRB) badgUpdater(
	badg *badger.DB,
	n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	sets := []func(index *llrb.LLRB, k, v, ov []byte) (uint64, []byte){
		t.set1, t.set2, t.set3, t.set4,
	}

	var nupdates int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateupdate(klen, vlen, n, seedl, seedc, -1)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)

	do := func() error {
		t.llrbrw.RLock()
		defer t.llrbrw.RUnlock()

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

func (t *TestLLRB) verifyupdater(
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

func (t *TestLLRB) set1(
	index *llrb.LLRB, key, value, oldvalue []byte) (uint64, []byte) {

	oldvalue, cas := index.Set(key, value, oldvalue)
	//fmt.Printf("update1 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 {
		comparekeyvalue(key, oldvalue, options.vallen)
	}
	return cas, oldvalue
}

func (t *TestLLRB) set2(
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
		if t.verifyset2(err, i, key, oldvalue) == "ok" {
			return cas, oldvalue
		}
	}
	panic("unreachable code")
}

func (t *TestLLRB) verifyset2(err error, i int, key, oldvalue []byte) string {
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

func (t *TestLLRB) set3(
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

func (t *TestLLRB) set4(
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

func (t *TestLLRB) lmdbDeleter(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()
	time.Sleep(1 * time.Second) // delay start for llrbUpdater to catchup.

	dels := []func(*llrb.LLRB, []byte, []byte, bool) (uint64, bool){
		t.del1, t.del2, t.del3, t.del4,
	}

	var ndeletes, xdeletes int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generatedelete(klen, vlen, n, seedl, seedc, delmod)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	lsmmap := map[int]bool{0: true, 1: false}

	do := func() error {
		t.llrbrw.RLock()
		defer t.llrbrw.RUnlock()

		index := t.loadindex()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		//fmt.Printf("delete %q\n", key)
		delidx, lsm := rnd.Intn(1000000)%len(dels), lsmmap[rnd.Intn(1000000)%2]
		if lsm {
			delidx = delidx % 2
		}
		for i := 2; i >= 0; i-- {
			dodel := dels[delidx]
			refcas, ok1 := dodel(index, key, value, lsm)
			oldvalue, _, _, ok2 := index.Get(key, oldvalue)
			rc := t.vllrbdel(index, key, oldvalue, refcas, i, lsm, ok2)
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
			fmsg := "Deleter {%v items %v} {%v:%v items in %v} " +
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
	fmsg := "at exit, Deleter %v:%v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, ndeletes, xdeletes, took)
}

func (t *TestLLRB) badgDeleter(
	badg *badger.DB,
	n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()
	dels := []func(*llrb.LLRB, []byte, []byte, bool) (uint64, bool){
		t.del1, t.del2, t.del3, t.del4,
	}

	time.Sleep(1 * time.Second) // delay start for llrbUpdater to catchup.

	var ndeletes, xdeletes int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generatedelete(klen, vlen, n, seedl, seedc, delmod)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	lsmmap := map[int]bool{0: true, 1: false}

	do := func() error {
		t.llrbrw.RLock()
		defer t.llrbrw.RUnlock()

		index := t.loadindex()

		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		//fmt.Printf("delete %q\n", key)
		ln := len(dels)
		delidx, lsm := rnd.Intn(1000000)%ln, lsmmap[rnd.Intn(1000000)%2]
		if lsm {
			delidx = delidx % 2
		}
		for i := 2; i >= 0; i-- {
			refcas, ok1 := dels[delidx](index, key, value, lsm)
			oldvalue, _, _, ok2 := index.Get(key, oldvalue)
			rc := t.vllrbdel(index, key, oldvalue, refcas, i, lsm, ok2)
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

func (t *TestLLRB) vllrbdel(
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

func (t *TestLLRB) del1(
	index *llrb.LLRB, key, oldval []byte, lsm bool) (uint64, bool) {

	var ok bool

	oldval, cas := index.Delete(key, oldval, lsm)
	if len(oldval) > 0 {
		comparekeyvalue(key, oldval, options.vallen)
		ok = true
	}
	return cas, ok
}

func (t *TestLLRB) del2(
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

func (t *TestLLRB) del3(
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

func (t *TestLLRB) del4(
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

func (t *TestLLRB) lmdbGetter(
	lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI,
	n, seedl, seedc int64, fin chan struct{}, wg *sync.WaitGroup) {

	defer wg.Done()

	gets := []func(*lmdb.Env, lmdb.DBI, *llrb.LLRB,
		[]byte, []byte) ([]byte, uint64, bool, bool){

		t.lmdbGet1, t.lmdbGet2, t.lmdbGet3,
	}

	var ngets, nmisses int64
	var key []byte
	var del bool
	g := Generateread(int64(options.keylen), n, seedl, seedc)

	rnd := rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(10000000)
	value := make([]byte, 16)

loop:
	for {
		index := t.loadindex()
		ngets++
		key = g(key, atomic.LoadInt64(&ncreates))
		get := gets[(rnd.Intn(1000000) % len(gets))]
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

func (t *TestLLRB) lmdbGet1(
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

func (t *TestLLRB) lmdbGet2(
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

func (t *TestLLRB) lmdbGet3(
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

func (t *TestLLRB) badgGetter(
	badg *badger.DB,
	n, seedl, seedc int64, fin chan struct{}, wg *sync.WaitGroup) {

	defer wg.Done()

	gets := []func(*badger.DB, *llrb.LLRB,
		[]byte, []byte) ([]byte, uint64, bool, bool){

		t.badgGet1, t.badgGet2, t.badgGet3,
	}

	var ngets, nmisses int64
	var key []byte
	var del bool
	g := Generateread(int64(options.keylen), n, seedl, seedc)

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

func (t *TestLLRB) badgGet1(
	badg *badger.DB,
	index *llrb.LLRB, key, value []byte) ([]byte, uint64, bool, bool) {

	llrbval, seqno, del, ok := index.Get(key, value)

	get := func(txn *badger.Txn) error {
		item, _ := txn.Get(key)
		if del == false && options.vallen > 0 && item != nil {
			badgval, _ := item.Value()
			if bytes.Compare(llrbval, badgval) != 0 {
				fmsg := "retry: expected %q, got %q"
				return fmt.Errorf(fmsg, badgval, llrbval)
			}
		}
		return nil
	}
	trybadgget(badg, 5000, get)

	return llrbval, seqno, del, ok
}

func (t *TestLLRB) badgGet2(
	badg *badger.DB,
	index *llrb.LLRB, key, value []byte) ([]byte, uint64, bool, bool) {

	var llrbval []byte
	var del, ok bool
	var llrbtxn api.Transactor

	get := func(txn *badger.Txn) (err error) {
		llrbtxn = index.BeginTxn(0xC0FFEE)
		llrbval, _, del, ok = llrbtxn.Get(key, value)

		item, err := txn.Get(key)
		if del == false && options.vallen > 0 && item != nil {
			badgval, _ := item.Value()
			if bytes.Compare(llrbval, badgval) != 0 {
				llrbtxn.Abort()
				fmsg := "retry: expected %q, got %q"
				return fmt.Errorf(fmsg, badgval, llrbval)
			}
		}
		return nil
	}
	trybadgget(badg, 5000, get)

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

func (t *TestLLRB) badgGet3(
	badg *badger.DB,
	index *llrb.LLRB, key, value []byte) ([]byte, uint64, bool, bool) {

	var llrbval []byte
	var del, ok bool
	var view api.Transactor

	get := func(txn *badger.Txn) (err error) {
		view = index.View(0x1235)
		llrbval, _, del, ok = view.Get(key, value)

		item, err := txn.Get(key)
		if del == false && options.vallen > 0 && item != nil {
			badgval, _ := item.Value()
			if bytes.Compare(llrbval, badgval) != 0 {
				view.Abort()
				fmsg := "retry: expected %q, got %q"
				return fmt.Errorf(fmsg, badgval, llrbval)
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
		} else if cvalue := cur.Value(); bytes.Compare(cvalue, llrbval) != 0 {
			panic(fmt.Errorf("expected %q, got %q", llrbval, cvalue))
		}
	}
	view.Abort()

	return llrbval, 0, del, ok
}

func (t *TestLLRB) Ranger(
	n, seedl, seedc int64, fin chan struct{}, wg *sync.WaitGroup) {

	defer wg.Done()

	rngs := []func(index *llrb.LLRB, key, val []byte) int64{
		t.range1, t.range2, t.range3, t.range4,
	}

	var nranges int64
	var key []byte
	g := Generateread(int64(options.keylen), n, seedl, seedc)

	rnd := rand.New(rand.NewSource(seedc))
	epoch, value := time.Now(), make([]byte, 16)

loop:
	for {
		index := t.loadindex()
		key = g(key, atomic.LoadInt64(&ncreates))
		rangefn := rngs[rnd.Intn(1000000)%len(rngs)]
		n := rangefn(index, key, value)
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

func (t *TestLLRB) range1(index *llrb.LLRB, key, value []byte) (n int64) {
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

func (t *TestLLRB) range2(index *llrb.LLRB, key, value []byte) (n int64) {
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

func (t *TestLLRB) range3(index *llrb.LLRB, key, value []byte) (n int64) {
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

func (t *TestLLRB) range4(index *llrb.LLRB, key, value []byte) (n int64) {
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
				lmdbkey, lmdbval, lmdberr = lmdbcur.Get(nil, nil, lmdb.Next)
			}
			llrbkey, llrbval, _, llrbdel, llrberr = iter(false /*fin*/)
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
	fmt.Printf("Took %v to compare (%v) LLRB and LMDB\n\n", took, cmpcount)
}

func compareLlrbBadger(index *llrb.LLRB, badg *badger.DB) {
	epoch, cmpcount := time.Now(), 0

	iter := index.Scan()
	err := badg.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		it.Rewind()

		for it.Valid() {
			llrbkey, llrbval, _, llrbdel, llrberr := iter(false /*fin*/)
			item := it.Item()
			badgkey := item.Key()
			badgval, badgerr := item.Value()

			if llrbdel == false {
				cmpcount++
				if llrberr != nil {
					panic(llrberr)

				} else if badgerr != nil {
					panic(badgerr)

				} else if bytes.Compare(llrbkey, badgkey) != 0 {
					fmsg := "expected %q,%q, got %q,%q"
					panic(fmt.Errorf(fmsg, badgkey, badgval, llrbkey, llrbval))

				} else if bytes.Compare(llrbval, badgval) != 0 {
					fmsg := "for %q expected val %q, got val %q\n"
					x, y := badgval[:options.vallen], llrbval[:options.vallen]
					fmt.Printf(fmsg, llrbkey, x, y)
					fmsg = "for %q expected seqno %v, got %v\n"
					x, y = badgval[options.vallen:], llrbval[options.vallen:]
					fmt.Printf(fmsg, llrbkey, badgval, llrbval)
					panic("error")
				}
				it.Next()
			}
		}
		llrbkey, _, _, llrbdel, llrberr := iter(false /*fin*/)
		for llrberr != io.EOF && llrbdel == true {
			llrbkey, _, _, llrbdel, llrberr = iter(false /*fin*/)
		}
		if llrberr != io.EOF {
			fmsg := "llrb items remaining {key:%q, del:%v err:%v"
			panic(fmt.Errorf(fmsg, llrbkey, llrbdel, llrberr))
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	iter(true /*fin*/)

	took := time.Since(epoch).Round(time.Second)
	fmt.Printf("Took %v to compare (%v) LLRB and BADGER\n\n", took, cmpcount)
}
