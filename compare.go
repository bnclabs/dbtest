package main

import "fmt"
import "time"
import "bytes"
import "strings"
import "runtime"
import "math/rand"
import "sync/atomic"

import s "github.com/prataprc/gosettings"
import "github.com/prataprc/gostore/llrb"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/bogn"
import "github.com/bmatsuo/lmdb-go/lmdb"
import "github.com/bmatsuo/lmdb-go/lmdbscan"

func compareLlrbLmdb(
	index *llrb.LLRB, lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {

	lmdbcount := getlmdbCount(lmdbenv, lmdbdbi)
	llrbcount := index.Count()
	seqno := atomic.LoadUint64(&seqno)
	fmsg := "compareLlrbLmdb, lmdbcount:%v llrbcount:%v seqno:%v\n"
	fmt.Printf(fmsg, lmdbcount, llrbcount, seqno)

	epoch, cmpcount := time.Now(), 0

	iter := index.Scan()
	err := lmdbenv.View(func(txn *lmdb.Txn) error {
		lmdbs := lmdbscan.New(txn, lmdbdbi)
		defer lmdbs.Close()

		lmdbs.Scan()

		llrbkey, llrbval, _, llrbdel, llrberr := iter(false /*fin*/)
		lmdbkey, lmdbval, lmdberr := lmdbs.Key(), lmdbs.Val(), lmdbs.Err()

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
			}

			lmdbs.Scan()
			llrbkey, llrbval, _, llrbdel, llrberr = iter(false /*fin*/)
			lmdbkey, lmdbval, lmdberr = lmdbs.Key(), lmdbs.Val(), lmdbs.Err()
		}
		if lmdbkey != nil {
			return fmt.Errorf("found lmdb key %q\n", lmdbkey)
		}

		return lmdberr
	})
	if err != nil {
		panic(err)
	}

	took := time.Since(epoch).Round(time.Second)
	fmt.Printf("Took %v to compare (%v) LLRB and LMDB\n\n", took, cmpcount)
}

func compareMvccLmdb(
	index *llrb.MVCC, lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {

	lmdbcount := getlmdbCount(lmdbenv, lmdbdbi)
	mvcccount := index.Count()
	fmsg := "compareMvccLmdb, lmdbcount:%v mvcccount:%v\n"
	fmt.Printf(fmsg, lmdbcount, mvcccount)

	epoch, cmpcount := time.Now(), 0

	//fmt.Println("mvcc seqno", index.Getseqno())
	iter := index.Scan()
	err := lmdbenv.View(func(txn *lmdb.Txn) error {
		lmdbs := lmdbscan.New(txn, lmdbdbi)
		defer lmdbs.Close()

		lmdbs.Scan()

		mvcckey, mvccval, _, mvccdel, mvccerr := iter(false /*fin*/)
		lmdbkey, lmdbval, lmdberr := lmdbs.Key(), lmdbs.Val(), lmdbs.Err()

		for mvcckey != nil {
			if mvccdel == false {
				cmpcount++
				if mvccerr != nil {
					panic(mvccerr)
				} else if lmdberr != nil {
					panic(lmdberr)
				} else if bytes.Compare(mvcckey, lmdbkey) != 0 {
					fmsg := "expected %q,%q, got %q,%q"
					panic(fmt.Errorf(fmsg, lmdbkey, lmdbval, mvcckey, mvccval))
				} else if bytes.Compare(mvccval, lmdbval) != 0 {
					fmsg := "for %q expected %q, got %q"
					panic(fmt.Errorf(fmsg, mvcckey, lmdbval, mvccval))
				}
				//fmt.Printf("compareMvccLmdb %q okay ...\n", llrbkey)
			}

			lmdbs.Scan()
			mvcckey, mvccval, _, mvccdel, mvccerr = iter(false /*fin*/)
			lmdbkey, lmdbval, lmdberr = lmdbs.Key(), lmdbs.Val(), lmdbs.Err()
		}
		if lmdbkey != nil {
			return fmt.Errorf("found lmdb key %q\n", lmdbkey)
		}

		return lmdberr
	})
	if err != nil {
		panic(err)
	}

	took := time.Since(epoch).Round(time.Second)
	fmt.Printf("Took %v to compare (%v) MVCC and LMDB\n\n", took, cmpcount)
}

func compareBognLmdb(
	index *bogn.Bogn, lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {

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
			}

			lmdbs.Scan()
			bognkey, bognval, _, bogndel, bognerr = iter(false /*fin*/)
			lmdbkey, lmdbval, lmdberr = lmdbs.Key(), lmdbs.Val(), lmdbs.Err()
		}
		if lmdbkey != nil {
			return fmt.Errorf("found lmdb key %q\n", lmdbkey)
		}

		return lmdberr
	})
	if err != nil {
		panic(err)
	}

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

func comparekeyvalue(key, value []byte, vlen int) bool {
	if vlen > 0 && len(value) > 0 {
		value := value[:len(value)-8]
		if len(key) >= vlen {
			if k := key[len(key)-len(value):]; bytes.Compare(k, value) != 0 {
				panic(fmt.Errorf("expected %q, got %q", k, value))
			}

		} else {
			m := len(value) - len(key)
			for _, ch := range value[:m] {
				if ch != '0' {
					panic(fmt.Errorf("expected %v, got %v", '0', ch))
				}
			}
			if bytes.Compare(value[m:], key) != 0 {
				panic(fmt.Errorf("expected %q, got %q", key, value[m:]))
			}
		}

	}
	return true
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
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		runtime.Gosched()
	}
}
