package main

import "os"
import "fmt"
import "sync"
import "time"
import "strings"
import "strconv"
import "runtime"
import "path/filepath"
import "sync/atomic"
import "math/rand"

import "github.com/bnclabs/golog"
import "github.com/bnclabs/gostore/lib"
import "github.com/bmatsuo/lmdb-go/lmdb"

func testlmdb() error {
	lmdbpath := makelmdbpath()
	defer func() {
		if err := os.RemoveAll(lmdbpath); err != nil {
			log.Errorf("%v", err)
		}
	}()

	env, dbi, err := initlmdb(lmdbpath, lmdb.NoSync|lmdb.NoMetaSync)
	if err != nil {
		return err
	}
	defer env.Close()

	seedl, seedc := int64(options.seed), int64(options.seed)+100
	fmt.Printf("Seed for load: %v, for ops: %v\n", seedl, seedc)
	if err = lmdbLoad(env, dbi, seedl); err != nil {
		return err
	}

	var wwg, rwg sync.WaitGroup
	// writer routines
	n := atomic.LoadInt64(&numentries)
	go lmdbCreater(env, dbi, n, seedc, &wwg)
	go lmdbUpdater(env, dbi, n, seedl, seedc, &wwg)
	go lmdbDeleter(env, dbi, n, seedl, seedc, &wwg)
	wwg.Add(3)
	// reader routines
	fin := make(chan struct{})
	for i := 0; i < options.cpu; i++ {
		go lmdbGetter(lmdbpath, n, seedl, seedc, fin, &rwg)
		go lmdbRanger(lmdbpath, n, seedl, seedc, fin, &rwg)
		rwg.Add(2)
	}
	wwg.Wait()
	close(fin)
	rwg.Wait()

	count, numentries := getlmdbCount(env, dbi), atomic.LoadInt64(&numentries)
	fmt.Printf("LMDB total indexed %v items, expected %v\n", count, numentries)

	return nil
}

func initlmdb(
	lmdbpath string, envflags uint) (env *lmdb.Env, dbi lmdb.DBI, err error) {

	env, err = lmdb.NewEnv()
	if err != nil {
		log.Errorf("%v", err)
		return
	}
	defer func() {
		if err != nil {
			env.Close()
		}
	}()

	env.SetMaxDBs(100)
	env.SetMapSize(14 * 1024 * 1024 * 1024) // 14GB

	// FixedMap   Danger zone. Map memory at a fixed address.
	// Readonly   Used in several functions to denote an object as readonly.
	// WriteMap   Use a writable memory map.
	// NoMetaSync Don't fsync metapage after commit.
	// NoSync     Don't fsync after commit.
	// MapAsync   Flush asynchronously when using the WriteMap flag.
	err = env.Open(lmdbpath, envflags, 0755)
	if err != nil {
		log.Errorf("%v", err)
		return
	}

	// Clear stale readers
	stalereads, err := env.ReaderCheck()
	if err != nil {
		log.Errorf("%v", err)
		return
	} else if stalereads > 0 {
		log.Infof("cleared %d reader slots from dead processes", stalereads)
	}

	// load lmdb
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.CreateDBI("perf")
		return err
	})
	if err != nil {
		log.Errorf("%v", err)
		return
	}
	return
}

func readlmdb(lmdbpath string, envflags uint) (*lmdb.Env, lmdb.DBI, error) {
	time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

	var dbi lmdb.DBI

	env, err := lmdb.NewEnv()
	if err != nil {
		log.Errorf("%v", err)
		return nil, dbi, err
	}

	env.SetMaxDBs(100)

	// FixedMap   Danger zone. Map memory at a fixed address.
	// Readonly   Used in several functions to denote an object as readonly.
	// WriteMap   Use a writable memory map.
	// NoMetaSync Don't fsync metapage after commit.
	// NoSync     Don't fsync after commit.
	// MapAsync   Flush asynchronously when using the WriteMap flag.
	err = env.Open(lmdbpath, envflags, 0755)
	if err != nil {
		env.Close()
		log.Errorf("%v", err)
		return env, dbi, err
	}

	// open dbi
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.OpenDBI("perf", 0)
		return err
	})
	if err != nil {
		log.Errorf("lmdb.OpenDBI():%v", err)
		return env, dbi, err
	}
	return env, dbi, nil
}

func lmdbLoad(env *lmdb.Env, dbi lmdb.DBI, seedl int64) error {
	var key, value []byte

	now := time.Now()

	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generateloadr(
		klen, vlen, int64(options.load), int64(seedl), options.randwidth,
	)

	populate := func(txn *lmdb.Txn) (err error) {
		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		//if "00000000000000000000000000699067" == string(key) {
		//	fmt.Println("load", string(key))
		//}
		for key != nil {
			//fmt.Printf("lmdb load %q\n", key)
			if err := txn.Put(dbi, key, value, 0); err != nil {
				return err
			}
			opaque = atomic.AddUint64(&seqno, 1)
			key, value = g(key, value, opaque)
		}
		return nil
	}
	if err := env.Update(populate); err != nil {
		log.Errorf("key %q err : %v", key, err)
		return err
	}
	atomic.AddInt64(&numentries, int64(options.load))
	atomic.AddInt64(&totalwrites, int64(options.load))

	took := time.Since(now).Round(time.Second)
	fmt.Printf("Loaded LMDB %v items in %v\n\n", options.load, took)
	return nil
}

func lmdbCreater(
	env *lmdb.Env, dbi lmdb.DBI, n, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	writes := int64(options.writes)
	g := Generatecreate(klen, vlen, n, writes, seedc, options.randwidth)

	epoch, now, markercount := time.Now(), time.Now(), int64(100000)
	//entries := int64(float64(options.entries) * 1.1)
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		if err := lmdbDocreate(env, dbi, key, value); err != nil {
			return
		}
		atomic.AddInt64(&numentries, 1)
		atomic.AddInt64(&totalwrites, 1)
		if nc := atomic.AddInt64(&ncreates, 1); nc%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "lmdbCreated {%v items in %v} {%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, nc, y)
			now = time.Now()
		}
	}
	fmsg := "at exit, lmdbCreated %v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, atomic.LoadInt64(&ncreates), took)
}

func lmdbDocreate(env *lmdb.Env, dbi lmdb.DBI, key, value []byte) error {
	//fmt.Printf("create %q %q\n", key, value)
	put := func(txn *lmdb.Txn) (err error) {
		//if "00000000000000000000000000699067" == string(key) {
		//	fmt.Println("create", string(key))
		//}
		if err := txn.Put(dbi, key, value, 0); err != nil {
			return err
		}
		return nil
	}
	if err := env.Update(put); err != nil {
		log.Errorf("key %q err : %v", key, err)
		return err
	}
	return nil
}

func lmdbUpdater(
	env *lmdb.Env, dbi lmdb.DBI, n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	var nupdates int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	writes := int64(options.writes)
	g := Generateupdate(
		klen, vlen, n, writes, seedl, seedc, -1, options.randwidth,
	)

	epoch, now, markercount := time.Now(), time.Now(), int64(100000)
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		if new, err := lmdbDoupdate(env, dbi, key, value); err != nil {
			return
		} else if new {
			atomic.AddInt64(&numentries, 1)
		}
		atomic.AddInt64(&totalwrites, 1)
		if nupdates = nupdates + 1; nupdates%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "lmdbUpdated {%v items in %v} {%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, nupdates, y)
			now = time.Now()
		}
	}
	fmsg := "at exit, lmdbUpdated %v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, nupdates, took)
}

func lmdbDoupdate(
	env *lmdb.Env, dbi lmdb.DBI, key, value []byte) (new bool, err error) {

	//fmt.Printf("update %q %q\n", key, value)
	update := func(txn *lmdb.Txn) (err error) {
		_, err = txn.Get(dbi, key)
		new = err != nil && strings.Contains(err.Error(), lmdbmissingerr)
		if err := txn.Put(dbi, key, value, 0); err != nil {
			return err
		}
		return nil
	}
	if err = env.Update(update); err != nil {
		log.Errorf("key %q err : %v", key, err)
	}
	return
}

func lmdbDeleter(
	env *lmdb.Env, dbi lmdb.DBI, n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	var xdeletes, ndeletes int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	writes := int64(options.writes)
	g := Generatedelete(
		klen, vlen, n, writes, seedl, seedc, delmod, options.randwidth,
	)

	epoch, now, markercount := time.Now(), time.Now(), int64(100000)
	//entries := int64(float64(options.entries) * 1.1)
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		if _, err := lmdbDodelete(env, dbi, key, value); err != nil {
			xdeletes++
		} else {
			ndeletes++
			atomic.AddInt64(&numentries, -1)
			atomic.AddInt64(&totalwrites, 1)
		}
		if (ndeletes+xdeletes)%markercount == 0 {
			x := time.Since(now).Round(time.Second)
			y := time.Since(epoch).Round(time.Second)
			fmsg := "lmdbDeleted {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ndeletes, xdeletes, y)
			now = time.Now()
		}
	}
	fmsg := "at exit, lmdbDeleter %v:%v items in %v\n"
	took := time.Since(epoch).Round(time.Second)
	fmt.Printf(fmsg, ndeletes, xdeletes, took)
}

func lmdbDodelete(
	env *lmdb.Env, dbi lmdb.DBI, key, value []byte) (x bool, err error) {

	//fmt.Printf("delete %q %q\n", key, value)
	delete := func(txn *lmdb.Txn) (err error) {
		if err := txn.Del(dbi, key, nil); err != nil {
			return err
		}
		return nil
	}
	if err = env.Update(delete); err != nil {
		//log.Infof("key %q err : %v", key, err)
		return true, err
	}
	return false, err
}

func lmdbGetter(
	lmdbpath string,
	n, seedl, seedc int64, fin chan struct{}, wg *sync.WaitGroup) {

	defer wg.Done()
	env, dbi, err := readlmdb(lmdbpath, 0)
	if err != nil {
		return
	}
	defer env.Close()

	time.Sleep(time.Duration(rand.Intn(1000)+3000) * time.Millisecond)

	var ngets, nmisses int64
	var key []byte
	writes := int64(options.writes)
	g := Generateread(
		int64(options.keylen), n, writes, seedl, seedc, options.randwidth,
	)

	get := func(txn *lmdb.Txn) (err error) {
		ngets++
		value, err := txn.Get(dbi, key)
		if x, xerr := strconv.Atoi(Bytes2str(key)); xerr != nil {
			panic(xerr)
		} else if (int64(x) % updtdel) != delmod {
			if err != nil {
				return err
			}
			comparekeyvalue(key, value, options.vallen)
		} else {
			nmisses++
		}
		return nil
	}

	epoch, now, markercount := time.Now(), time.Now(), int64(10000000)
loop:
	for {
		key = g(key, atomic.LoadInt64(&ncreates))
		if err := env.View(get); err != nil {
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
			fmsg := "lmdbGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
			now = time.Now()
		}
	}
	took := time.Since(epoch).Round(time.Second)
	<-fin
	fmsg := "at exit, lmdbGetter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, took)
}

func lmdbRanger(
	lmdbpath string,
	n, seedl, seedc int64, fin chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	env, dbi, err := readlmdb(lmdbpath, 0)
	if err != nil {
		return
	}
	defer env.Close()

	time.Sleep(time.Duration(rand.Intn(1000)+3000) * time.Millisecond)

	var nranges, nmisses int64
	var key []byte
	writes := int64(options.writes)
	g := Generateread(
		int64(options.keylen), n, writes, seedl, seedc, options.randwidth,
	)

	ranger := func(txn *lmdb.Txn) error {
		cur, err := txn.OpenCursor(dbi)
		if err != nil {
			log.Errorf("lmdb.OpenCursor(): %v", err)
			return err
		}
		defer cur.Close()

		_, value, err := cur.Get(key, nil, 0)
		for i := 0; i < 100; i++ {
			nranges++
			key, value, err = cur.Get(nil, nil, lmdb.Next)
			if lmdb.IsNotFound(err) {
				return nil
			} else if x, xerr := strconv.Atoi(Bytes2str(key)); xerr != nil {
				panic(xerr)
			} else if (int64(x) % updtdel) != delmod {
				if err != nil {
					return err
				}
				comparekeyvalue(key, value, options.vallen)
			} else {
				nmisses++
			}
		}
		return nil
	}

	epoch := time.Now()
loop:
	for {
		key = g(key, atomic.LoadInt64(&ncreates))
		if err := env.View(ranger); err != nil {
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
	fmsg := "at exit, lmdbRanger %v:%v items in %v\n"
	fmt.Printf(fmsg, nranges, nmisses, took)
}

func makelmdbpath() string {
	path := filepath.Join(os.TempDir(), "lmdb.data")
	os.RemoveAll(path)
	if err := os.MkdirAll(path, 0775); err != nil {
		panic(err)
	}
	return path
}

func getlmdbCount(env *lmdb.Env, dbi lmdb.DBI) (count uint64) {
	err := env.Update(func(txn *lmdb.Txn) (err error) {
		stat, err := txn.Stat(dbi)
		if err != nil {
			return err
		}
		count = stat.Entries
		return nil
	})
	if err != nil {
		panic(err)
	}
	return
}

func cmplmdbget(lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI, key []byte) []byte {
	value := make([]byte, 10)
	get := func(txn *lmdb.Txn) (err error) {
		val, err := txn.Get(lmdbdbi, key)
		if err != nil {
			fmsg := "retry: for key %q, err %q"
			return fmt.Errorf(fmsg, key, err)
		}
		value = lib.Fixbuffer(value, int64(len(val)))
		copy(value, val)
		return nil
	}
	trylmdbget(lmdbenv, 5000, get)
	return value
}

func trylmdbget(lmdbenv *lmdb.Env, repeat int, get func(*lmdb.Txn) error) {
	for i := 0; i < repeat; i++ {
		if err := lmdbenv.View(get); err == nil {
			break

		} else if strings.Contains(err.Error(), "retry") {
			if i == (repeat - 1) {
				panic(err)
			}

		} else {
			panic(err)
		}
		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
		runtime.Gosched()
	}
}
