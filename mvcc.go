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

import "github.com/prataprc/gostore/llrb"
import "github.com/prataprc/gostore/api"

// TODO: add test cases for transactions.

func testmvcc() error {
	setts := llrb.Defaultsettings()
	index := llrb.NewMVCC("dbtest", setts)
	defer index.Destroy()

	seedl, seedc := int64(options.seed), int64(options.seed)+100
	fmt.Printf("Seed for load: %v, for ops: %v\n", seedl, seedc)
	if err := mvccLoad(index, seedl); err != nil {
		return err
	}

	go logger(index)

	var wwg, rwg sync.WaitGroup
	//// writer routines
	n := atomic.LoadInt64(&numentries)
	go mvccCreater(index, n, seedc, &wwg)
	go mvccUpdater(index, n, seedl, seedc, &wwg)
	go mvccDeleter(index, n, seedl, seedc, &wwg)
	wwg.Add(3)
	// reader routines
	fin := make(chan struct{})
	for i := 0; i < runtime.GOMAXPROCS(-1); i++ {
		go mvccGetter(index, n, seedl, seedc, fin, &rwg)
		go mvccRanger(index, n, seedl, seedc, fin, &rwg)
		rwg.Add(2)
	}
	wwg.Wait()
	close(fin)
	rwg.Wait()

	fmt.Printf("LMDB total indexed %v items\n", index.Count())

	return nil
}

func logger(index *llrb.MVCC) {
	tick := time.NewTicker(10 * time.Second)
	for {
		<-tick.C
		index.Log()
		m := index.Stats()
		fmt.Printf("count: %10d\n", m["n_count"])
		a, b, c := m["n_inserts"], m["n_updates"], m["n_deletes"]
		fmt.Printf("write: %10d %10d %10d\n", a, b, c)
		a, b, c = m["n_nodes"], m["n_frees"], m["n_clones"]
		fmt.Printf("nodes: %10d %10d %10d\n", a, b, c)
		a, b, c = m["n_txns"], m["n_commits"], m["n_aborts"]
		fmt.Printf("txns : %10d %10d %10d\n", a, b, c)
		a, b = m["keymemory"], m["valmemory"]
		fmt.Printf("reqm : %10d %10d\n", a, b)
		a, b, c, d := m["n_reclaims"], m["n_snapshots"], m["n_purgedss"], m["n_activess"]
		fmt.Printf("mvcc : %10d %10d %10d %10d\n", a, b, c, d)
		//m["h_upsertdepth"]
		//m["h_bulkfree"]
		//m["h_reclaims"]
		//m["h_versions"]
	}
}

func mvccLoad(index *llrb.MVCC, seedl int64) error {
	klen, vlen := int64(options.keylen), int64(options.keylen)
	n := int64(options.entries / 2)
	if n > 1000000 {
		n = 1000000
	}
	g := Generateloadr(klen, vlen, n, int64(seedl))

	key, value := make([]byte, 16), make([]byte, 16)
	now, oldvalue := time.Now(), make([]byte, 16)
	for key, value = g(key, value); key != nil; key, value = g(key, value) {
		//fmt.Printf("load %q\n", key)
		oldvalue, _ := index.Set(key, value, oldvalue)
		if len(oldvalue) > 0 {
			panic(fmt.Errorf("unexpected %q", oldvalue))
		}
	}
	atomic.AddInt64(&numentries, n)
	atomic.AddInt64(&totalwrites, n)

	fmt.Printf("Loaded %v items in %v\n", n, time.Since(now))
	return nil
}

var mvccsets = map[int]func(index *llrb.MVCC, key, val, oldval []byte) uint64{
	0: mvccSet1, 1: mvccSet2, 2: mvccSet3, 3: mvccSet4,
}

func mvccCreater(index *llrb.MVCC, n, seedc int64, wg *sync.WaitGroup) {
	defer wg.Done()

	klen, vlen := int64(options.keylen), int64(options.keylen)
	g := Generatecreate(klen, vlen, n, seedc)

	key, value := make([]byte, 16), make([]byte, 16)
	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		key, value = g(key, value)
		setidx := rnd.Intn(1000000) % 4
		refcas := mvccsets[setidx](index, key, value, oldvalue)
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
			fmsg := "mvccCreated {%v items in %v} {%v items in %v} count:%v\n"
			fmt.Printf(fmsg, markercount, x, nc, y.Round(time.Second), count)
			now = time.Now()
		}
	}
	rlbks := atomic.LoadInt64(&rollbacks)
	fmsg := "at exit, mvccCreated %v items in %v, rollback %v\n"
	fmt.Printf(fmsg, atomic.LoadInt64(&ncreates), time.Since(epoch), rlbks)
}

func mvccUpdater(index *llrb.MVCC, n, seedl, seedc int64, wg *sync.WaitGroup) {
	defer wg.Done()

	var nupdates int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.keylen)
	g := Generateupdate(klen, vlen, n, seedl, seedc, -1)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		key, value = g(key, value)
		setidx := rnd.Intn(1000000) % 4
		for i := 2; i >= 0; i-- {
			refcas := mvccsets[setidx](index, key, value, oldvalue)
			oldvalue, cas, del, ok := index.Get(key, oldvalue)
			if verifyupdater(key, oldvalue, refcas, cas, i, del, ok) == "ok" {
				break
			}
		}

		atomic.AddInt64(&totalwrites, 1)
		if nupdates = nupdates + 1; nupdates%markercount == 0 {
			count := index.Count()
			x, y := time.Since(now).Round(time.Second), time.Since(epoch)
			fmsg := "mvccUpdated {%v items in %v} {%v items in %v} count:%v\n"
			fmt.Printf(fmsg, markercount, x, nupdates, y.Round(time.Second), count)
			now = time.Now()
		}
	}
	fmsg := "at exit, mvccUpdated %v items in %v\n"
	fmt.Printf(fmsg, nupdates, time.Since(epoch))
}

func mvccSet1(index *llrb.MVCC, key, value, oldvalue []byte) uint64 {
	oldvalue, cas := index.Set(key, value, oldvalue)
	//fmt.Printf("update1 %q %q %q \n", key, value, oldvalue)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	}
	return cas
}

func mvccverifyset2(err error, i int, key, oldvalue []byte) string {
	if err == nil && len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
	}
	if err != nil && i == 0 {
		panic(err)
	} else if err != nil {
		atomic.AddInt64(&conflicts, 1)
		return "repeat"
	}
	return "ok"
}

func mvccSet2(index *llrb.MVCC, key, value, oldvalue []byte) uint64 {
	for i := 3; i >= 0; i-- {
		oldvalue, oldcas, deleted, ok := index.Get(key, oldvalue)
		if deleted || ok == false {
			oldcas = 0
		} else if oldcas == 0 {
			panic(fmt.Errorf("unexpected %v", oldcas))
		} else if bytes.Compare(key, oldvalue) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
		}
		oldvalue, cas, err := index.SetCAS(key, value, oldvalue, oldcas)
		//fmt.Printf("mvccSet2 %q %q %v %v\n", key, value, oldcas, err)
		if mvccverifyset2(err, i, key, oldvalue) == "ok" {
			return cas
		}
	}
	panic("unreachable code")
}

func mvccSet3(index *llrb.MVCC, key, value, oldvalue []byte) uint64 {
	for i := 2; i >= 0; i-- {
		txn := index.BeginTxn(0xC0FFEE)
		oldvalue = txn.Set(key, value, oldvalue)
		//fmt.Printf("update3 %q %q %q \n", key, value, oldvalue)
		if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
		}
		err := txn.Commit()
		if err == nil {
			return 0
		} else if i == 0 {
			panic(err)
		} else if err.Error() == api.ErrorRollback.Error() {
			atomic.AddInt64(&rollbacks, 1)
		}
	}
	return 0
}

func mvccSet4(index *llrb.MVCC, key, value, oldvalue []byte) uint64 {
	for i := 2; i >= 0; i-- {
		txn := index.BeginTxn(0xC0FFEE)
		cur := txn.OpenCursor(key)
		oldvalue = cur.Set(key, value, oldvalue)
		//fmt.Printf("update4 %q %q %q \n", key, value, oldvalue)
		if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
		}
		err := txn.Commit()
		if err == nil {
			return 0
		} else if i == 0 {
			panic(err)
		} else if err.Error() == api.ErrorRollback.Error() {
			atomic.AddInt64(&rollbacks, 1)
		}
	}
	return 0
}

var mvccdels = map[int]func(*llrb.MVCC, []byte, []byte, bool) (uint64, bool){
	0: mvccDel1, 1: mvccDel2, 2: mvccDel3, 3: mvccDel4,
}

func mvccDeleter(index *llrb.MVCC, n, seedl, seedc int64, wg *sync.WaitGroup) {
	defer wg.Done()

	var ndeletes, xdeletes int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.keylen)
	g := Generatedelete(klen, vlen, n, seedl, seedc, delmod)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	lsmmap := map[int]bool{0: true, 1: false}
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		key, value = g(key, value)
		//fmt.Printf("delete %q\n", key)
		ln := len(mvccdels)
		delidx, lsm := rnd.Intn(1000000)%ln, lsmmap[rnd.Intn(1000000)%2]
		if lsm {
			delidx = delidx % 2
		}
		for i := 2; i >= 0; i-- {
			refcas, ok1 := mvccdels[delidx](index, key, value, lsm)
			oldvalue, _, _, ok2 := index.Get(key, oldvalue)
			if verifydel(index, key, oldvalue, refcas, i, lsm, ok2) == "ok" {
				if ok1 || lsm == true {
					ndeletes++
					atomic.AddInt64(&numentries, -1)
					atomic.AddInt64(&totalwrites, 1)
				} else {
					xdeletes++
				}
				break
			}
		}

		if x := ndeletes + xdeletes; x > 0 && (x%markercount) == 0 {
			count := index.Count()
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "mvccDeleted {%v items in %v} {%v:%v items in %v} count:%v\n"
			fmt.Printf(fmsg, markercount, x, ndeletes, xdeletes, y, count)
			now = time.Now()
		}
	}
	fmsg := "at exit, mvccDeleter %v:%v items in %v\n"
	fmt.Printf(fmsg, ndeletes, xdeletes, time.Since(epoch))
}

func mvccDel1(index *llrb.MVCC, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	//fmt.Printf("mvccDel1 %q %v\n", key, lsm)
	oldvalue, cas := index.Delete(key, oldvalue, lsm)
	if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
		panic(fmt.Errorf("expected %q, got %s", key, oldvalue))
	} else if len(oldvalue) > 0 {
		ok = true
	}
	return cas, ok
}

func mvccDel2(index *llrb.MVCC, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	for i := 2; i >= 0; i-- {
		txn := index.BeginTxn(0xC0FFEE)
		oldvalue = txn.Delete(key, oldvalue, lsm)
		if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
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
	}
	return 0, ok
}

func mvccDel3(index *llrb.MVCC, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	for i := 2; i >= 0; i-- {
		txn := index.BeginTxn(0xC0FFEE)
		cur := txn.OpenCursor(key)
		oldvalue = cur.Delete(key, oldvalue, lsm)
		if len(oldvalue) > 0 && bytes.Compare(key, oldvalue) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, oldvalue))
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
	}
	return 0, ok
}

func mvccDel4(index *llrb.MVCC, key, oldvalue []byte, lsm bool) (uint64, bool) {
	var ok bool

	for i := 2; i >= 0; i-- {
		txn := index.BeginTxn(0xC0FFEE)
		cur := txn.OpenCursor(key)
		curkey, _ := cur.Key()
		if bytes.Compare(key, curkey) == 0 {
			cur.Delcursor(lsm)
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
	}
	return 0, ok
}

var mvccgets = map[int]func(index *llrb.MVCC, key, val []byte) ([]byte, uint64, bool, bool){
	0: mvccGet1, 1: mvccGet2, 2: mvccGet3,
}

func mvccGetter(
	index *llrb.MVCC, n, seedl, seedc int64,
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
		ln := len(mvccgets)
		value, _, del, _ = mvccgets[rnd.Intn(1000000)%ln](index, key, value)
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
		if ngets%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "mvccGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
		}
	}
	duration := time.Since(epoch)
	<-fin
	fmsg := "at exit, mvccGetter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, duration)
}

func mvccGet1(
	index *llrb.MVCC, key, value []byte) ([]byte, uint64, bool, bool) {

	//fmt.Printf("mvccGet1 %q\n", key)
	//defer fmt.Printf("mvccGet1-abort %q\n", key)
	return index.Get(key, value)
}

func mvccGet2(
	index *llrb.MVCC, key, value []byte) ([]byte, uint64, bool, bool) {

	//fmt.Printf("mvccGet2\n")
	txn := index.BeginTxn(0xC0FFEE)
	value, del, ok := txn.Get(key, value)
	if ok == true {
		cur := txn.OpenCursor(key)
		if ckey, cdel := cur.Key(); cdel != del {
			panic(fmt.Errorf("expected %v, got %v", del, cdel))
		} else if bytes.Compare(ckey, key) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, ckey))
		} else if cvalue := cur.Value(); bytes.Compare(cvalue, value) != 0 {
			panic(fmt.Errorf("expected %q, got %q", value, cvalue))
		}
	}
	//fmt.Printf("mvccGet2-abort\n")
	txn.Abort()
	return value, 0, del, ok
}

func mvccGet3(
	index *llrb.MVCC, key, value []byte) ([]byte, uint64, bool, bool) {

	view := index.View(0x1235)
	value, del, ok := view.Get(key, value)
	if ok == true {
		cur := view.OpenCursor(key)
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

var mvccrngs = map[int]func(index *llrb.MVCC, key, val []byte) int64{
	0: mvccRange1, 1: mvccRange2, 2: mvccRange3, 3: mvccRange4,
}

func mvccRanger(
	index *llrb.MVCC, n, seedl, seedc int64,
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
		ln := len(mvccrngs)
		n := mvccrngs[rnd.Intn(1000000)%ln](index, key, value)
		nranges += n
		select {
		case <-fin:
			break loop
		default:
		}
	}
	duration := time.Since(epoch)
	<-fin
	fmt.Printf("at exit, mvccRanger %v items in %v\n", nranges, duration)
}

func mvccRange1(index *llrb.MVCC, key, value []byte) (n int64) {
	//fmt.Printf("mvccRange1 %q\n", key)
	txn := index.BeginTxn(0xC0FFEE)
	cur := txn.OpenCursor(key)
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

func mvccRange2(index *llrb.MVCC, key, value []byte) (n int64) {
	txn := index.BeginTxn(0xC0FFEE)
	cur := txn.OpenCursor(key)
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
	}
	txn.Abort()
	return
}

func mvccRange3(index *llrb.MVCC, key, value []byte) (n int64) {
	view := index.View(0x1236)
	cur := view.OpenCursor(key)
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
	}
	view.Abort()
	return
}

func mvccRange4(index *llrb.MVCC, key, value []byte) (n int64) {
	view := index.View(0x1237)
	cur := view.OpenCursor(key)
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
	}
	view.Abort()
	return
}