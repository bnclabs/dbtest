package main

import "runtime"

var seqno uint64
var ncreates int64
var numentries int64
var totalwrites int64
var totalreads int64
var updtdel = int64(2)
var delmod = int64(0)
var updmod = int64(1)
var conflicts = int64(0)
var rollbacks = int64(0)
var numcpus = runtime.GOMAXPROCS(-1)
var lmdbmissingerr = "No matching key/data pair found"
var badgconflict = "Transaction Conflict"
var badgkeymissing = "Key not found"
