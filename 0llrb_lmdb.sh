rm dbtest
go build

cmdargs="-db llrb -ref lmdb -load 1000000 -writes 1100000 -reads 0 -lsm"

echo "./dbtest $cmdargs"
./dbtest $cmdargs