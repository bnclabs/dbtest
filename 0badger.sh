rm dbtest
go build

cmdargs="-db badger -load 1000000 -writes 4000000"

echo "./dbtest $cmdargs"
./dbtest $cmdargs