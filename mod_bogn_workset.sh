rm dbtest
go build

cmdargs="-db bogn -bogn workset -load 1000000 -writes 40000000 -lsm"

echo "./dbtest $cmdargs"
./dbtest $cmdargs
