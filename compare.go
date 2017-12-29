package main

import "fmt"
import "time"
import "bytes"

import "github.com/prataprc/gostore/llrb"
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
			if llrbdel {
				continue
			}
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

			lmdbs.Scan()
			llrbkey, llrbval, _, llrbdel, llrberr = iter(false /*fin*/)
			lmdbkey, lmdbval, lmdberr = lmdbs.Key(), lmdbs.Val(), lmdbs.Err()
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
			if mvccdel {
				continue
			}
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

			lmdbs.Scan()
			mvcckey, mvccval, _, mvccdel, mvccerr = iter(false /*fin*/)
			lmdbkey, lmdbval, lmdberr = lmdbs.Key(), lmdbs.Val(), lmdbs.Err()
		}

		return lmdberr
	})
	if err != nil {
		panic(err)
	}

	took := time.Since(epoch)
	fmt.Printf("Took %v to compare (%v) MVCC and LMDB\n\n", took, cmpcount)
}
