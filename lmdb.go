package main

import "os"
import "fmt"
import "sync"
import "time"
import "bytes"
import "sync/atomic"

import "github.com/prataprc/golog"
import "github.com/bmatsuo/lmdb-go/lmdb"

var numentries int64
var totalwrites int64

func testlmdb() error {
	options.path = lmdbpath()
	defer func() {
		if err := os.RemoveAll(options.path); err != nil {
			log.Errorf("%v", err)
		}
	}()

	env, dbi, err := initlmdb(lmdb.NoSync | lmdb.NoMetaSync)
	if err != nil {
		return err
	}
	defer env.Close()

	seedl, seedc := int64(options.seed), int64(options.seed)+100
	fmt.Printf("Seeds load: %v, ops: %v\n", seedl, seedc)
	if err = lmdbLoad(env, dbi, seedl); err != nil {
		return err
	}

	var wg sync.WaitGroup
	// writer routines
	n := atomic.LoadInt64(&numentries)
	go lmdbCreater(env, dbi, n, seedc, &wg)
	go lmdbUpdater(env, dbi, n, seedl, seedc, &wg)
	go lmdbDeleter(env, dbi, n, seedl, seedc, &wg)
	wg.Add(3)
	// reader routines
	//fin := make(chan struct{})
	//for i := 0; i < runtime.GOMAXPROCS(-1); i++ {
	//	go lmdbGetter(n, seedl, seedc, fin, &wg)
	//	go lmdbRanger(n, seedl, seedc, fin, &wg)
	//	wg.Add(2)
	//}
	wg.Wait()

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
	err = env.Open(options.path, envflags, 0755)
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
	err = env.Open(options.path, envflags, 0755)
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

	klen, vlen := int64(options.keylen), int64(options.keylen)
	n := int64(options.entries / 2)
	g := Generateloadr(klen, vlen, n, int64(seedl))

	populate := func(txn *lmdb.Txn) (err error) {
		key, value = g(key, value)
		for ; key != nil; key, value = g(key, value) {
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
	atomic.AddInt64(&numentries, n)
	atomic.AddInt64(&totalwrites, n)

	fmt.Printf("Loaded %v items in %v\n", n, time.Since(now))
	return nil
}

func lmdbCreater(
	env *lmdb.Env, dbi lmdb.DBI, n, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	var ncreates int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.keylen)
	g := Generatecreate(klen, vlen, n, seedc)

	put := func(txn *lmdb.Txn) (err error) {
		if err := txn.Put(dbi, key, value, 0); err != nil {
			return err
		}
		return nil
	}

	now := time.Now()
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		key, value = g(key, value)
		if err := env.Update(put); err != nil {
			log.Errorf("key %q err : %v", key, err)
			return
		}
		atomic.AddInt64(&numentries, 1)
		atomic.AddInt64(&totalwrites, 1)
		if ncreates = ncreates + 1; ncreates%100000 == 0 {
			fmsg := "lmdbCreated %v entries in %v\n"
			fmt.Printf(fmsg, ncreates, time.Since(now))
			now = time.Now()
		}
	}
	fmt.Printf("at exit, lmdbCreated %v entries\n", ncreates)
}

func lmdbUpdater(
	env *lmdb.Env, dbi lmdb.DBI, n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	var nupdates int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.keylen)
	g := Generateupdate(klen, vlen, n, seedl, seedc)

	update := func(txn *lmdb.Txn) (err error) {
		if err := txn.Put(dbi, key, value, 0); err != nil {
			return err
		}
		return nil
	}

	now := time.Now()
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		key, value = g(key, value)
		if err := env.Update(update); err != nil {
			log.Errorf("key %q err : %v", key, err)
			return
		}
		atomic.AddInt64(&totalwrites, 1)
		if nupdates = nupdates + 1; nupdates%100000 == 0 {
			fmsg := "lmdbUpdated %v entries in %v\n"
			fmt.Printf(fmsg, nupdates, time.Since(now))
			now = time.Now()
		}
	}
	fmt.Printf("at exit, lmdbUpdated %v entries\n", nupdates)
}

func lmdbDeleter(
	env *lmdb.Env, dbi lmdb.DBI, n, seedl, seedc int64, wg *sync.WaitGroup) {

	defer wg.Done()

	var xdeletes, ndeletes int64
	var key, value []byte
	klen, vlen := int64(options.keylen), int64(options.keylen)
	g := Generatedelete(klen, vlen, n, seedl, seedc)

	delete := func(txn *lmdb.Txn) (err error) {
		if err := txn.Del(dbi, key, nil); err != nil {
			return err
		}
		return nil
	}

	now := time.Now()
	for atomic.LoadInt64(&totalwrites) < int64(options.writes) {
		key, value = g(key, value)
		if err := env.Update(delete); err != nil {
			//log.Infof("key %q err : %v", key, err)
			xdeletes++
		} else {
			ndeletes++
			atomic.AddInt64(&numentries, -1)
			atomic.AddInt64(&totalwrites, 1)
		}
		if ndeletes%100000 == 0 {
			fmsg := "lmdbDeleted %v entries (x:%v) in %v\n"
			fmt.Printf(fmsg, ndeletes, xdeletes, time.Since(now))
			now = time.Now()
		}
	}
	fmt.Printf("at exit, lmdbDeleter %v entries\n", ndeletes)
}

func lmdbGetter(n, seedl, seedc int64, fin chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	env, dbi, err := readlmdb(0)
	if err != nil {
		return
	}
	defer env.Close()
	var key []byte
	g := Generateread(int64(options.keylen), n, seedl, seedc)

	get := func(txn *lmdb.Txn) (err error) {
		value, err := txn.Get(dbi, key)
		if err != nil {
			return err
		} else if bytes.Compare(key, value) != 0 {
			return fmt.Errorf("expected %q, got %q", key, value)
		}
		return nil
	}

	for {
		key = g(key)
		if err := env.View(get); err != nil {
			log.Errorf("%q err: %v", key, err)
			return
		}
		select {
		case <-fin:
			return
		default:
		}
	}
}

func lmdbRanger(n, seedl, seedc int64, fin chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	env, dbi, err := readlmdb(0)
	if err != nil {
		return
	}
	defer env.Close()
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
		if err != nil {
			return err
		} else if bytes.Compare(key, value) != 0 {
			return fmt.Errorf("expected %q, got %q", key, value)
		}
		for i := 0; i < 100; i++ {
			if key, value, err = cur.Get(nil, nil, lmdb.Next); err != nil {
				return err
			} else if bytes.Compare(key, value) != 0 {
				return fmt.Errorf("expected %q, got %q", key, value)
			}
		}
		return nil
	}

	for {
		key = g(key)
		if err := env.View(ranger); err != nil {
			log.Errorf("%q err: %v", key, err)
			return
		}
		select {
		case <-fin:
			return
		default:
		}
	}
}

func lmdbpath() string {
	if options.path == "" {
		options.path = "testdata/lmdb.data"
	}
	if err := os.MkdirAll(options.path, 0775); err != nil {
		panic(err)
	}
	return options.path
}
