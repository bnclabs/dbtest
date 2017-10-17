package main

import "os"
import "flag"
import "net/http"
import _ "net/http/pprof"

import "github.com/prataprc/golog"

// TODO: add Validate for llrb and mvcc.

var options struct {
	db      string
	path    string
	entries int
	writes  int
	keylen  int
	seed    int
}

func optparse(args []string) {
	f := flag.NewFlagSet("dbperf", flag.ExitOnError)

	f.StringVar(&options.db, "db", "llrb", "pick db storage to torture test.")
	f.StringVar(&options.path, "path", "", "db path to open")
	f.IntVar(&options.entries, "n", 1000000, "db path to open")
	f.IntVar(&options.writes, "writes", 10000000, "total number of writes")
	f.IntVar(&options.keylen, "key", 32, "db path to open")
	f.IntVar(&options.seed, "seed", 10, "seed value to generate randomness")
	f.Parse(args)
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
	}
}
