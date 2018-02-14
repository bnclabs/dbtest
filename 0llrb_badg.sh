rm dbtest
go build

cmdargs="-db llrb -ref badger -load 1000000 -writes 1100000 -lsm"

echo "./dbtest $cmdargs"
./dbtest $cmdargs
