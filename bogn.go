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
import "github.com/prataprc/gostore/bogn"
import s "github.com/prataprc/gosettings"

func testbogn() error {
	setts := bognsettings(options.seed)
	bogn.PurgeIndex("dbtest", setts)

	index, err := bogn.New("dbtest", setts)
	if err != nil {
		panic(err)
	}
	defer index.Destroy()
	defer index.Close()
	index.Start()

	seedl, seedc := int64(options.seed), int64(options.seed)+100
	fmt.Printf("Seed for load: %v, for ops: %v\n", seedl, seedc)
	if err := bognLoad(index, seedl); err != nil {
		return err
	}

	var wwg, rwg sync.WaitGroup
	fin := make(chan struct{})

	go bognvalidator(index, true /*log*/, &rwg, fin)
	rwg.Add(1)

	// writer routines
	n := atomic.LoadInt64(&numentries)
	go bognCreater(index, n, seedc, &wwg)
	go bognUpdater(index, n, seedl, seedc, &wwg)
	go bognDeleter(index, n, seedl, seedc, &wwg)
	wwg.Add(3)

	// reader routines
	for i := 0; i < 4; i++ { // options.cpu; i++ {
		go bognGetter(index, n, seedl, seedc, fin, &rwg)
		go bognRanger(index, n, seedl, seedc, fin, &rwg)
		rwg.Add(2)
	}
	wwg.Wait()
	close(fin)
	rwg.Wait()

	index.Log()
	index.Validate()

	fmt.Printf("Number of ROLLBACKS: %v\n", rollbacks)
	fmt.Printf("Number of conflicts: %v\n", conflicts)
	//count, n := index.Count(), atomic.LoadInt64(&numentries)
	//fmt.Printf("BOGN total indexed %v items, expected %v\n", count, n)

	return nil
}

func bognvalidator(
	index *bogn.Bogn, log bool, wg *sync.WaitGroup, fin chan struct{}) {

	defer wg.Done()

	tick := time.NewTicker(10 * time.Second)
loop:
	for {
		<-tick.C
		select {
		case <-fin:
		default:
			break loop
		}

		if log {
			index.Log()
		}

		now := time.Now()
		index.Validate()
		fmt.Printf("Took %v to validate index\n", time.Since(now))
	}
}

func bognLoad(index *bogn.Bogn, seedl int64) error {
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateloadr(klen, vlen, int64(options.load), int64(seedl))

	now, oldvalue := time.Now(), make([]byte, 16)
	opaque := atomic.AddUint64(&seqno, 1)
	key, value := g(make([]byte, 16), make([]byte, 16), opaque)
	for ; key != nil; key, value = g(key, value, opaque) {
		//fmt.Printf("load %q\n", key)
		oldvalue, _ := index.Set(key, value, oldvalue)
		if len(oldvalue) > 0 {
			panic(fmt.Errorf("unexpected %q", oldvalue))
		}
		opaque = atomic.AddUint64(&seqno, 1)
	}
	atomic.AddInt64(&numentries, int64(options.load))
	atomic.AddInt64(&totalwrites, int64(options.load))

	fmt.Printf("Loaded %v items in %v\n", options.load, time.Since(now))
	return nil
}

var bognsets = []func(index *bogn.Bogn, key, val, ov []byte) (uint64, []byte){
	bognSet1, bognSet2, bognSet3, bognSet4,
}

func bognCreater(index *bogn.Bogn, n, seedc int64, wg *sync.WaitGroup) {
	defer wg.Done()

	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generatecreate(klen, vlen, n, seedc)

	key, value := make([]byte, 16), make([]byte, 16)
	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
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
			x, y := time.Since(now).Round(time.Second), time.Since(epoch)
			fmsg := "bognCreated {%v items in %v} {%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, nc, y.Round(time.Second))
			now = time.Now()
		}
		runtime.Gosched()
	}
	fmsg := "at exit, bognCreated %v items in %v\n"
	fmt.Printf(fmsg, atomic.LoadInt64(&ncreates), time.Since(epoch))
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

func bognUpdater(index *bogn.Bogn, n, seedl, seedc int64, wg *sync.WaitGroup) {
	defer wg.Done()

	var nupdates int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateupdate(klen, vlen, n, seedl, seedc, -1)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
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
			x, y := time.Since(now).Round(time.Second), time.Since(epoch)
			fmsg := "bognUpdated {%v items in %v} {%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, nupdates, y.Round(time.Second))
			now = time.Now()
		}
		runtime.Gosched()
	}
	fmsg := "at exit, bognUpdated %v items in %v\n"
	fmt.Printf(fmsg, nupdates, time.Since(epoch))
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
		//fmt.Printf("update2 %q %q %q \n", key, value, oldvalue)
		if bognverifyset2(err, i, key, oldvalue) == "ok" {
			return cas, oldvalue
		}
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

func bognDeleter(index *bogn.Bogn, n, seedl, seedc int64, wg *sync.WaitGroup) {
	defer wg.Done()

	var ndeletes, xdeletes int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generatedelete(klen, vlen, n, seedl, seedc, delmod)

	oldvalue, rnd := make([]byte, 16), rand.New(rand.NewSource(seedc))
	epoch, now, markercount := time.Now(), time.Now(), int64(1000000)
	lsm := options.lsm
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
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
		runtime.Gosched()
	}
	fmsg := "at exit, bognDeleter %v:%v items in %v\n"
	fmt.Printf(fmsg, ndeletes, xdeletes, time.Since(epoch))
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

var bogngets = []func(x *bogn.Bogn, k, v []byte) ([]byte, uint64, bool, bool){
	bognGet1, bognGet2, bognGet3,
}

func bognGetter(
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
		ln := len(bogngets)
		value, _, del, _ = bogngets[rnd.Intn(1000000)%ln](index, key, value)
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
			fmsg := "bognGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
		}
		runtime.Gosched()
	}
	duration := time.Since(epoch)
	<-fin
	fmsg := "at exit, bognGetter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, duration)
}

func bognGet1(
	index *bogn.Bogn, key, value []byte) ([]byte, uint64, bool, bool) {

	//fmt.Printf("bognGet1 %q\n", key)
	//defer fmt.Printf("bognGet1-abort %q\n", key)
	return index.Get(key, value)
}

func bognGet2(
	index *bogn.Bogn, key, value []byte) ([]byte, uint64, bool, bool) {

	//fmt.Printf("bognGet2\n")
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
	//fmt.Printf("bognGet2-abort\n")
	txn.Abort()
	return value, 0, del, ok
}

func bognGet3(
	index *bogn.Bogn, key, value []byte) ([]byte, uint64, bool, bool) {

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
	duration := time.Since(epoch)
	<-fin
	fmt.Printf("at exit, bognRanger %v items in %v\n", nranges, duration)
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
		} else if (int64(x)%2) != delmod && del == true {
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
		} else if (int64(x)%2) != delmod && del == true {
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
		} else if (int64(x)%2) != delmod && del == true {
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
		} else if (int64(x)%2) != delmod && del == true {
			panic("unexpected delete")
		}
		comparekeyvalue(key, value, options.vallen)
		n++
	}
	view.Abort()
	return
}

func bognsettings(seed int) s.Settings {
	rnd := rand.New(rand.NewSource(int64(seed)))
	setts := bogn.Defaultsettings()
	setts["memstore"] = options.memstore
	setts["period"] = int64(options.period)
	ratio := []float64{.5, .33, .25, .20, .16, .125, .1}[rnd.Intn(10000)%7]
	setts["ratio"] = ratio
	setts["bubt.mmap"] = []bool{true, false}[rnd.Intn(10000)%2]
	setts["bubt.msize"] = []int64{4096, 8192, 12288}[rnd.Intn(10000)%3]
	setts["bubt.zsize"] = []int64{4096, 8192, 12288}[rnd.Intn(10000)%3]
	//setts["llrb.memcapacity"] = 10 * 1024 * 1024 * 1024
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
	fmt.Printf("durable:%v dgm:%v workingset:%v\n", a, b, c)
	a, b = setts["ratio"], setts["period"]
	fmt.Printf("ratio:%v period:%v lsm:%v\n", a, b, options.lsm)
	a = setts["llrb.snapshottick"]
	fmt.Printf("llrb snapshottick:%v\n", a)
	a, b = setts["bubt.diskpaths"], setts["bubt.msize"]
	c, d := setts["bubt.zsize"], setts["bubt.mmap"]
	fmt.Printf("bubt diskpaths:%v msize:%v zsize:%v mmap:%v\n", a, b, c, d)

	return setts
}
