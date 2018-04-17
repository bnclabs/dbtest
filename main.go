package main

import "os"
import "fmt"
import "flag"
import "time"
import "strings"
import "runtime"
import "net/http"
import _ "net/http/pprof"

import "github.com/bnclabs/golog"
import "github.com/bnclabs/gostore/llrb"
import "github.com/bnclabs/gostore/bubt"
import "github.com/bnclabs/gostore/bogn"
import "github.com/cloudfoundry/gosigar"

// TODO: add Validate for llrb and mvcc.

var options struct {
	db         string
	ref        string
	cpu        int
	load       int
	writes     int
	reads      int
	keylen     int
	vallen     int
	bogn       string
	capacity   int
	memstore   string
	autocommit int
	lsm        bool
	seed       int
	randwidth  bool
	npaths     int
	msize      int
	log        string
}

func optparse(args []string) {
	f := flag.NewFlagSet("dbperf", flag.ExitOnError)
	cpu := (runtime.GOMAXPROCS(-1) / 2) - 1
	if cpu <= 0 {
		cpu = 2
	}
	_, _, freeram := getsysmem()

	f.StringVar(&options.db, "db", "llrb",
		"lmdb|badger|llrb|mvcc|bubt|bogn store type")
	f.StringVar(&options.ref, "ref", "lmdb", "lmdb|badger store type")
	f.IntVar(&options.cpu, "cpu", cpu, "lmdb|llrb|mvcc|bubt|bogn store type")
	f.IntVar(&options.load, "load", 1000000, "number of entries to load initially")
	f.IntVar(&options.writes, "writes", 10000000, "total number of writes")
	f.IntVar(&options.reads, "reads", 10000000, "total number of read operations")
	f.IntVar(&options.keylen, "key", 32, "key size")
	f.IntVar(&options.vallen, "value", 32, "value size")
	f.IntVar(&options.seed, "seed", 0, "seed value to generate randomness")
	f.StringVar(&options.bogn, "bogn", "memonly", "memonly|durable|dgm|workset")
	f.IntVar(&options.capacity, "capacity", int(freeram), "in dgm, memory capacity")
	f.StringVar(&options.memstore, "memstore", "mvcc", "llrb|mvcc for bogn")
	f.IntVar(&options.autocommit, "autocommit", 10, "bogn auto-commit, in seconds")
	f.BoolVar(&options.lsm, "lsm", false, "use LSM deletes")
	f.IntVar(&options.npaths, "npaths", 0, "number of directory paths for bubt")
	f.BoolVar(&options.randwidth, "randwidth", false, "generate keys and values of random width")
	f.IntVar(&options.msize, "msize", 4096, "bubt option, m-block size")
	f.StringVar(&options.log, "log", "", "llrb,mvcc,bubt,bogn")
	f.Parse(args)

	if options.seed == 0 {
		options.seed = int(time.Now().UnixNano())
	}
	if options.vallen > 0 && options.vallen < 16 {
		fmt.Println("value length should be atleast 16 bytes")
		os.Exit(1)
	}

	for _, comp := range strings.Split(options.log, ",") {
		switch comp {
		case "bubt":
			bubt.LogComponents("self")
		case "bogn":
			bogn.LogComponents("self")
		case "llrb", "mvcc":
			llrb.LogComponents("self")
		case "all":
			bubt.LogComponents("all")
			bogn.LogComponents("all")
			llrb.LogComponents("all")
		}
	}
}

func main() {
	optparse(os.Args[1:])

	go func() {
		log.Infof("%v", http.ListenAndServe("localhost:6060", nil))
	}()

	if options.db == "lmdb" {
		testlmdb()
	} else if options.db == "badger" {
		testbadger()
	} else if options.db == "llrb" && options.ref == "lmdb" {
		(&TestLLRB{}).llrbwithlmdb()
	} else if options.db == "llrb" && options.ref == "badger" {
		(&TestLLRB{}).llrbwithbadg()
	} else if options.db == "mvcc" && options.ref == "lmdb" {
		(&TestMVCC{}).mvccwithlmdb()
	} else if options.db == "mvcc" && options.ref == "badg" {
		(&TestMVCC{}).mvccwithbadg()
	} else if options.db == "bubt" {
		testbubt()
	} else if options.db == "bogn" {
		testbogn()
	}
}

func getsysmem() (total, used, free uint64) {
	mem := sigar.Mem{}
	mem.Get()
	return mem.Total, mem.Used, mem.Free
}
