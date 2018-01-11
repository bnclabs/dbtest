rm dbtest
go build

KVOPTIONS="-key 32 -value 16"
cmdargs="-db mvcc -load 1000000 -writes 40000000 -lsm -cpu 3 $KVOPTIONS"

echo "./dbtest $cmdargs"
./dbtest $cmdargs
