package main

import "os"
import "fmt"
import "sync"
import "time"
import "strings"
import "runtime"
import "strconv"
import "path/filepath"
import "sync/atomic"
import "math/rand"

import "github.com/bnclabs/golog"
import "github.com/dgraph-io/badger"

func testbadger() error {
	pathdir := badgerpath()
	defer func() {
		if err := os.RemoveAll(pathdir); err != nil {
			log.Errorf("%v", err)
		}
	}()
	fmt.Printf("BADGER path %q\n", pathdir)

	badg, err := initbadger(pathdir)
	if err != nil {
		panic(err)
	}
	defer badg.Close()

	klen, vlen := int64(options.keylen), int64(options.vallen)
	loadn := int64(options.load)
	seedl, seedc := int64(options.seed), int64(options.seed)+100
	fmt.Printf("Seed for load: %v, for ops: %v\n", seedl, seedc)
	if err = badgerload(badg, klen, vlen, loadn, seedl); err != nil {
		return err
	}

	var wwg, rwg sync.WaitGroup
	// writer routines
	go badgerCreater(badg, loadn, klen, vlen, seedc, &wwg)
	go badgerUpdater(badg, loadn, klen, vlen, seedl, seedc, &wwg)
	go badgerDeleter(badg, loadn, klen, vlen, seedl, seedc, &wwg)
	wwg.Add(3)
	// reader routines
	fin := make(chan struct{})
	for i := 0; i < options.cpu; i++ {
		go badgerGetter(badg, loadn, klen, vlen, seedl, seedc, fin, &rwg)
		go badgerRanger(badg, loadn, klen, vlen, seedl, seedc, fin, &rwg)
		rwg.Add(2)
	}
	wwg.Wait()
	close(fin)
	rwg.Wait()

	numentries := atomic.LoadInt64(&numentries)
	fmt.Printf("BADGER total expected entries %v\n", numentries)
	fmt.Printf("BADGER transaction conflicts %v", conflicts)

	return nil
}

func initbadger(pathdir string) (badg *badger.DB, err error) {
	opts := badger.DefaultOptions
	opts.Dir, opts.ValueDir = pathdir, pathdir
	badg, err = badger.Open(opts)
	if err != nil {
		fmt.Printf("badger.Open(): %v\n", err)
		return nil, err
	}
	return badg, nil
}

type badgerop struct {
	op    byte
	key   []byte
	value []byte
}

func badgerload(badg *badger.DB, klen, vlen, loadn, seedl int64) error {
	g, count := Generateloadr(klen, vlen, loadn, seedl), int64(0)

	cmds := make([]*badgerop, 10000)
	for off := range cmds {
		cmds[off] = &badgerop{}
	}
	populate := func(txn *badger.Txn) (err error) {
		for _, cmd := range cmds {
			if err = txn.Set(cmd.key, cmd.value); err != nil {
				log.Errorf("key %q err : %v", cmd.key, err)
				return err
			}
		}
		return nil
	}

	epoch, off := time.Now(), 0
	cmd := cmds[off]
	opaque := atomic.AddUint64(&seqno, 1)
	cmd.key, cmd.value = g(cmd.key, cmd.value, opaque)
	for off = 1; cmd.key != nil; off++ {
		if off == len(cmds) {
			if err := badg.Update(populate); err != nil {
				panic(err)
			}
			off = 0
		}
		cmd = cmds[off]
		opaque := atomic.AddUint64(&seqno, 1)
		cmd.key, cmd.value = g(cmd.key, cmd.value, opaque)
		count++
	}

	atomic.AddInt64(&numentries, int64(loadn))
	atomic.AddInt64(&totalwrites, int64(loadn))

	took := time.Since(epoch).Round(time.Second)
	fmt.Printf("Loaded %v items in %v\n", loadn, took)
	return nil
}

var badgconflict = "Transaction Conflict"
var badgkeymissing = "Key not found"

func badgerCreater(
	badg *badger.DB, loadn, klen, vlen, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	var key, value []byte

	g := Generatecreate(klen, vlen, loadn, seedc)

	epoch, now, markercount := time.Now(), time.Now(), int64(100000)
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		if err := badgDocreate(badg, key, value); err != nil {
			return
		}
		atomic.AddInt64(&numentries, 1)
		atomic.AddInt64(&totalwrites, 1)
		if nc := atomic.AddInt64(&ncreates, 1); nc%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "badgerCreated {%v items in %v} {%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, nc, y)
			now = time.Now()
		}
	}
	fmsg := "at exit, badgerCreated %v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, atomic.LoadInt64(&ncreates), took)
}

func badgDocreate(badg *badger.DB, key, value []byte) (err error) {
	put := func(txn *badger.Txn) (err error) {
		if err := txn.Set(key, value); err != nil {
			return err
		}
		return nil
	}
	for i := 0; i < 10; i++ {
		if err = badg.Update(put); err == nil {
			return
		} else if strings.Contains(err.Error(), badgconflict) {
			atomic.AddInt64(&conflicts, 1)
			continue
		} else {
			log.Errorf("key %q err : %v", key, err)
		}
	}
	return err
}

func badgerUpdater(
	badg *badger.DB,
	loadn, klen, vlen, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	var nupdates int64
	var key, value []byte

	g := Generateupdate(klen, vlen, loadn, seedl, seedc, -1)

	epoch, now, markercount := time.Now(), time.Now(), int64(100000)
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		if new, err := badgDoupdate(badg, key, value); err != nil {
			return
		} else if new {
			atomic.AddInt64(&numentries, 1)
		}
		atomic.AddInt64(&totalwrites, 1)
		if nupdates = nupdates + 1; nupdates%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "badgerUpdated {%v items in %v} {%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, nupdates, y)
			now = time.Now()
		}
	}
	fmsg := "at exit, badgerUpdated %v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, nupdates, took)
}

func badgDoupdate(badg *badger.DB, key, value []byte) (new bool, err error) {
	update := func(txn *badger.Txn) (err error) {
		_, err = txn.Get(key)
		new = err != nil && strings.Contains(err.Error(), badgkeymissing)
		if err := txn.Set(key, value); err != nil {
			return err
		}
		return nil
	}
	for i := 0; i < 20; i++ {
		if err = badg.Update(update); err == nil {
			return
		} else if strings.Contains(err.Error(), badgconflict) {
			atomic.AddInt64(&conflicts, 1)
			continue
		} else {
			log.Errorf("key %q err : %v", key, err)
			return new, err
		}
	}
	return new, err
}

func badgerDeleter(
	badg *badger.DB,
	loadn, klen, vlen, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	var xdeletes, ndeletes int64
	var key, value []byte

	g := Generatedelete(klen, vlen, loadn, seedl, seedc, delmod)

	epoch, now, markercount := time.Now(), time.Now(), int64(100000)
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		if x, err := badgDodelete(badg, key, value); err != nil {
			return
		} else if x {
			xdeletes++
		} else {
			ndeletes++
			atomic.AddInt64(&numentries, -1)
			atomic.AddInt64(&totalwrites, 1)
		}
		if (ndeletes+xdeletes)%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "badgerDeleted {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ndeletes, xdeletes, y)
			now = time.Now()
		}
	}
	fmsg := "at exit, badgerDeleter %v:%v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, ndeletes, xdeletes, took)
}

func badgDodelete(badg *badger.DB, key, value []byte) (x bool, err error) {
	delete := func(txn *badger.Txn) (err error) {
		if err := txn.Delete(key); err != nil {
			return err
		}
		return nil
	}

	for i := 0; i < 10; i++ {
		if err = badg.Update(delete); err == nil {
			return false, nil
		} else if strings.Contains(err.Error(), badgconflict) {
			atomic.AddInt64(&conflicts, 1)
			continue
		} else if strings.Contains(err.Error(), badgconflict) {
			return true, nil
		} else {
			log.Errorf("key %q err : %v", key, err)
			return true, err
		}
	}
	return true, err
}

func badgerGetter(
	badg *badger.DB, loadn, klen, vlen, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	defer wg.Done()

	time.Sleep(time.Duration(rand.Intn(1000)+3000) * time.Millisecond)

	var ngets, nmisses int64
	var key []byte

	g := Generateread(klen, loadn, seedl, seedc)

	get := func(txn *badger.Txn) (err error) {
		ngets++
		item, err := txn.Get(key)
		if x, xerr := strconv.Atoi(Bytes2str(key)); xerr != nil {
			panic(xerr)
		} else if (int64(x) % updtdel) != delmod {
			if err != nil {
				return err
			}
			if value, err := item.Value(); err != nil {
				return err
			} else {
				comparekeyvalue(key, value, int(vlen))
			}
		} else {
			nmisses++
		}
		return nil
	}

	epoch, now, markercount := time.Now(), time.Now(), int64(10000000)
loop:
	for {
		key = g(key, atomic.LoadInt64(&ncreates))
		if err := badg.View(get); err != nil {
			log.Errorf("%q err: %v", key, err)
			break loop
		}
		select {
		case <-fin:
			break loop
		default:
		}
		if ngets%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "badgerGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
			now = time.Now()
		}
	}
	took := time.Since(epoch).Round(time.Second)
	<-fin
	fmsg := "at exit, badgerGetter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, took)
}

func badgerRanger(
	badg *badger.DB, loadn, klen, vlen, seedl, seedc int64,
	fin chan struct{}, wg *sync.WaitGroup) {

	defer wg.Done()

	time.Sleep(time.Duration(rand.Intn(1000)+3000) * time.Millisecond)

	var nranges, nmisses int64
	var key []byte
	g := Generateread(klen, loadn, seedl, seedc)

	ranger := func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100

		it := txn.NewIterator(opts)
		it.Seek(key)
		for i := 0; it.Valid() && i < 100; i++ {
			item := it.Item()
			key := item.Key()
			value, err := item.Value()
			if err != nil {
				return err
			} else if x, xerr := strconv.Atoi(Bytes2str(key)); xerr != nil {
				panic(xerr)
			} else if (int64(x) % updtdel) != delmod {
				if err != nil {
					return err
				}
				comparekeyvalue(key, value, int(klen))
			}

			it.Next()
			nranges++
		}
		return nil
	}

	epoch := time.Now()
loop:
	for {
		key = g(key, atomic.LoadInt64(&ncreates))
		if err := badg.View(ranger); err != nil {
			log.Errorf("%q err: %v", key, err)
			break loop
		}
		select {
		case <-fin:
			break loop
		default:
		}
	}
	took := time.Since(epoch).Round(time.Second)
	<-fin
	fmsg := "at exit, badgerRanger %v:%v items in %v\n"
	fmt.Printf(fmsg, nranges, nmisses, took)
}

func badgerpath() string {
	path := filepath.Join(os.TempDir(), "badger.data")
	if err := os.RemoveAll(path); err != nil {
		panic(err)
	}
	return path
}

func trybadgget(badg *badger.DB, repeat int, get func(*badger.Txn) error) {
	for i := 0; i < repeat; i++ {
		if err := badg.View(get); err == nil {
			break

		} else if strings.Contains(err.Error(), "retry") {
			if i == (repeat - 1) {
				panic(err)
			}

		} else {
			panic(err)
		}
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		runtime.Gosched()
	}
}
