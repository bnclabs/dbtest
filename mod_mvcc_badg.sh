rm dbtest
go build

cmdargs="-db mvcc -ref badg -load 1000000 -writes 4000000 -lsm"

echo "./dbtest $cmdargs"
./dbtest $cmdargs
