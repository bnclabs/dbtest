rm dbtest
go build

cmdargs="-db mvcc -ref badg -load 1000000 -writes 4000000 -lsm"
echo "./dbtest $cmdargs"
./dbtest $cmdargs

cmdargs="-db mvcc -ref badg -value 256 -load 1000000 -writes 4000000 -lsm -randwidth"
echo "./dbtest $cmdargs"
./dbtest $cmdargs
