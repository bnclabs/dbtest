package main

import "os"
import "flag"
import "time"
import "runtime"
import "net/http"
import _ "net/http/pprof"

import "github.com/prataprc/golog"
import "github.com/cloudfoundry/gosigar"

// TODO: add Validate for llrb and mvcc.

var options struct {
	db       string
	cpu      int
	load     int
	writes   int
	reads    int
	keylen   int
	vallen   int
	bogn     string
	capacity int
	memstore string
	period   int
	lsm      bool
	seed     int
}

func optparse(args []string) {
	f := flag.NewFlagSet("dbperf", flag.ExitOnError)
	cpu := (runtime.GOMAXPROCS(-1) / 2) - 1
	if cpu <= 0 {
		cpu = 2
	}
	_, _, freeram := getsysmem()

	f.StringVar(&options.db, "db", "llrb", "lmdb|llrb|mvcc|bubt|bogn store type")
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
	f.IntVar(&options.period, "period", 10, "bogn flush period, in seconds")
	f.BoolVar(&options.lsm, "lsm", false, "use LSM deletes")
	f.Parse(args)

	if options.seed == 0 {
		options.seed = int(time.Now().UnixNano())
	}
}

func main() {
	optparse(os.Args[1:])

	go func() {
		log.Infof("%v", http.ListenAndServe("localhost:6060", nil))
	}()

	switch options.db {
	case "lmdb":
		testlmdb()
	case "llrb":
		testllrb()
	case "mvcc":
		testmvcc()
	case "bubt":
		testbubt()
	case "bogn":
		testbogn()
	}
}

func getsysmem() (total, used, free uint64) {
	mem := sigar.Mem{}
	mem.Get()
	return mem.Total, mem.Used, mem.Free
}
