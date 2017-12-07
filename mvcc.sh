rm dbtest
go build

cmdargs="-db mvcc -load 1000000 -writes 40000000 -lsm"

echo "./dbtest $cmdargs"
./dbtest $cmdargs
