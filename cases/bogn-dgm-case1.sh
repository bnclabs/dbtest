rm dbtest
go build

BUGARGS="-seed 1514722699679526000"
TESTARGS="-load 1000000 -writes 40000000 -lsm"
cmdargs="-db bogn -bogn dgm $BUGARGS $TESTARGS"

echo "./dbtest $cmdargs"
./dbtest $cmdargs

# failure o/p
#
# panic: expected "00000000000000000000000000001997", got "00000000000000000000000000001994"
#
# goroutine 6 [running]:
# main.compareBognLmdb.func1(0xc420cd8000, 0x0, 0x0)
#         /Users/prataprc/myworld/devgo/src/github.com/bnclabs/dbtest/compare.go:154 +0x89f
# github.com/bmatsuo/lmdb-go/lmdb.(*Txn).runOpTerm(0xc420cd8000, 0xc420e42060, 0x0, 0x0)
#         /Users/prataprc/myworld/devgo/src/github.com/bmatsuo/lmdb-go/lmdb/txn.go:158 +0x7c
# github.com/bmatsuo/lmdb-go/lmdb.(*Env).run(0xc420128360, 0x4013e00, 0x20000, 0xc420e42060, 0x0, 0x0)
#         /Users/prataprc/myworld/devgo/src/github.com/bmatsuo/lmdb-go/lmdb/env.go:511 +0x129
# github.com/bmatsuo/lmdb-go/lmdb.(*Env).View(0xc420128360, 0xc420e42060, 0x46eb3e0, 0x4498556)
#         /Users/prataprc/myworld/devgo/src/github.com/bmatsuo/lmdb-go/lmdb/env.go:451 +0x43
# main.compareBognLmdb(0xc42017c0e0, 0xc420128360, 0x2)
#         /Users/prataprc/myworld/devgo/src/github.com/bnclabs/dbtest/compare.go:136 +0x102
# main.bognvalidator.func1.1(0xc4201282a0, 0xc42017c0e0, 0xc420128360, 0x2)
#         /Users/prataprc/myworld/devgo/src/github.com/bnclabs/dbtest/bogn.go:125 +0x87
# main.bognvalidator.func1()
#         /Users/prataprc/myworld/devgo/src/github.com/bnclabs/dbtest/bogn.go:126 +0x167
# main.bognvalidator(0xc420128360, 0x2, 0xc42017c0e0, 0x1, 0xc42026e2b0, 0xc42013c180, 0xc4201282a0)
#         /Users/prataprc/myworld/devgo/src/github.com/bnclabs/dbtest/bogn.go:137 +0xd6
# created by main.dobogntest
#         /Users/prataprc/myworld/devgo/src/github.com/bnclabs/dbtest/bogn.go:71 +0x3ad
# ➜
