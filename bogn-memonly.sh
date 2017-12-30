rm dbtest
go build

cmdargs="-db bogn -bogn memonly -load 1000000 -writes 2000000 -lsm"

echo "./dbtest $cmdargs"
./dbtest $cmdargs
