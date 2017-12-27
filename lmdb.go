package main

import "os"
import "fmt"
import "sync"
import "time"
import "strings"
import "strconv"
import "path/filepath"
import "sync/atomic"
import "math/rand"

import "github.com/prataprc/golog"
import "github.com/bmatsuo/lmdb-go/lmdb"

var lmdbpath string

func testlmdb() error {
	lmdbpath = makelmdbpath()
	defer func() {
		if err := os.RemoveAll(lmdbpath); err != nil {
			log.Errorf("%v", err)
		}
	}()

	env, dbi, err := initlmdb(lmdb.NoSync | lmdb.NoMetaSync)
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
		go lmdbGetter(n, seedl, seedc, fin, &rwg)
		go lmdbRanger(n, seedl, seedc, fin, &rwg)
		rwg.Add(2)
	}
	wwg.Wait()
	close(fin)
	rwg.Wait()

	count, numentries := getlmdbCount(env, dbi), atomic.LoadInt64(&numentries)
	fmt.Printf("LMDB total indexed %v items, expected %v\n", count, numentries)

	return nil
}

func initlmdb(envflags uint) (env *lmdb.Env, dbi lmdb.DBI, err error) {
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

func readlmdb(envflags uint) (*lmdb.Env, lmdb.DBI, error) {
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
	opaque := atomic.AddUint64(&seqno, 1)
	g := Generateloadr(klen, vlen, int64(options.load), int64(seedl))

	populate := func(txn *lmdb.Txn) (err error) {
		key, value = g(key, value, opaque)
		//if "00000000000000000000000000699067" == string(key) {
		//	fmt.Println("load", string(key))
		//}
		for ; key != nil; key, value = g(key, value, opaque) {
			opaque = atomic.AddUint64(&seqno, 1)
			if err := txn.Put(dbi, key, value, 0); err != nil {
				return err
			}
		}
		return nil
	}
	if err := env.Update(populate); err != nil {
		log.Errorf("key %q err : %v", key, err)
		return err
	}
	atomic.AddInt64(&numentries, int64(options.load))
	atomic.AddInt64(&totalwrites, int64(options.load))

	fmt.Printf("Loaded %v items in %v\n", options.load, time.Since(now))
	return nil
}

func lmdbCreater(
	env *lmdb.Env, dbi lmdb.DBI, n, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.vallen)
	g := Generatecreate(klen, vlen, n, seedc)

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
			x, y := time.Since(now), time.Since(epoch)
			fmsg := "lmdbCreated {%v items in %v} {%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, nc, y)
			now = time.Now()
		}
	}
	fmsg := "at exit, lmdbCreated %v items in %v\n"
	fmt.Printf(fmsg, atomic.LoadInt64(&ncreates), time.Since(epoch))
}

func lmdbDocreate(env *lmdb.Env, dbi lmdb.DBI, key, value []byte) error {
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
	g := Generateupdate(klen, vlen, n, seedl, seedc, -1)

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
			x, y := time.Since(now), time.Since(epoch)
			fmsg := "lmdbUpdated {%v items in %v} {%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, nupdates, y)
			now = time.Now()
		}
	}
	fmsg := "at exit, lmdbUpdated %v items in %v\n"
	fmt.Printf(fmsg, nupdates, time.Since(epoch))
}

func lmdbDoupdate(
	env *lmdb.Env, dbi lmdb.DBI, key, value []byte) (new bool, err error) {

	missingerr := "No matching key/data pair found"

	update := func(txn *lmdb.Txn) (err error) {
		_, err = txn.Get(dbi, key)
		new = err != nil && strings.Contains(err.Error(), missingerr)
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
	g := Generatedelete(klen, vlen, n, seedl, seedc, delmod)

	epoch, now, markercount := time.Now(), time.Now(), int64(100000)
	//entries := int64(float64(options.entries) * 1.1)
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		opaque := atomic.AddUint64(&seqno, 1)
		key, value = g(key, value, opaque)
		if x, err := lmdbDodelete(env, dbi, key, value); err != nil {
			return
		} else if x {
			xdeletes++
		} else {
			ndeletes++
			atomic.AddInt64(&numentries, -1)
			atomic.AddInt64(&totalwrites, 1)
		}
		if (ndeletes+xdeletes)%markercount == 0 {
			x, y := time.Since(now), time.Since(epoch)
			fmsg := "lmdbDeleted {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ndeletes, xdeletes, y)
			now = time.Now()
		}
	}
	fmsg := "at exit, lmdbDeleter %v:%v items in %v\n"
	fmt.Printf(fmsg, ndeletes, xdeletes, time.Since(epoch))
}

func lmdbDodelete(
	env *lmdb.Env, dbi lmdb.DBI, key, value []byte) (x bool, err error) {

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

func lmdbGetter(n, seedl, seedc int64, fin chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	env, dbi, err := readlmdb(0)
	if err != nil {
		return
	}
	defer env.Close()

	time.Sleep(time.Duration(rand.Intn(1000)+3000) * time.Millisecond)

	var ngets, nmisses int64
	var key []byte
	g := Generateread(int64(options.keylen), n, seedl, seedc)

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
			x, y := time.Since(now), time.Since(epoch)
			fmsg := "lmdbGetter {%v items in %v} {%v:%v items in %v}\n"
			fmt.Printf(fmsg, markercount, x, ngets, nmisses, y)
			now = time.Now()
		}
	}
	duration := time.Since(epoch)
	<-fin
	fmsg := "at exit, lmdbGetter %v:%v items in %v\n"
	fmt.Printf(fmsg, ngets, nmisses, duration)
}

func lmdbRanger(n, seedl, seedc int64, fin chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	env, dbi, err := readlmdb(0)
	if err != nil {
		return
	}
	defer env.Close()

	time.Sleep(time.Duration(rand.Intn(1000)+3000) * time.Millisecond)

	var nranges, nmisses int64
	var key []byte
	g := Generateread(int64(options.keylen), n, seedl, seedc)

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
	duration := time.Since(epoch)
	<-fin
	fmsg := "at exit, lmdbRanger %v:%v items in %v\n"
	fmt.Printf(fmsg, nranges, nmisses, duration)
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
