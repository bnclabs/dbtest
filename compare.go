package main

import "fmt"
import "time"
import "bytes"

import s "github.com/prataprc/gosettings"
import "github.com/prataprc/gostore/llrb"
import "github.com/prataprc/gostore/bogn"
import "github.com/bmatsuo/lmdb-go/lmdb"
import "github.com/bmatsuo/lmdb-go/lmdbscan"

func compareLlrbLmdb(
	index *llrb.LLRB, lmdbenv *lmdb.Env, lmdbdbi lmdb.DBI) {

	lmdbcount := getlmdbCount(lmdbenv, lmdbdbi)
	llrbcount := index.Count()
	fmsg := "compareLlrbLmdb, lmdbcount:%v llrbcount:%v\n"
	fmt.Printf(fmsg, lmdbcount, llrbcount)

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
					fmsg := "expected %q, got %q"
					panic(fmt.Errorf(fmsg, lmdbkey, llrbkey))
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

	took := time.Since(epoch)
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
					fmsg := "expected %q, got %q"
					panic(fmt.Errorf(fmsg, lmdbkey, mvcckey))
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

	took := time.Since(epoch)
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
					fmsg := "expected %q, got %q"
					panic(fmt.Errorf(fmsg, lmdbkey, bognkey))
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

	took := time.Since(epoch)
	fmt.Printf("Took %v to compare (%v) BOGN and LMDB\n\n", took, cmpcount)
}

func diskBognLmdb(name, lmdbpath string, setts s.Settings) {
	if setts.Bool("durable") == false {
		return
	}

	fmt.Println("\n.......... Final disk check ..............\n")

	index, err := bogn.New(name /*dbtest*/, setts)
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
		if bytes.Compare(key, value[:len(key)]) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, value))
		}
	}
	return true
}
