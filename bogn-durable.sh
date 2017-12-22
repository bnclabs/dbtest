rm dbtest
go build

cmdargs="-db bogn -bogn durable -load 1000000 -writes 40000000 -lsm"

echo "./dbtest $cmdargs"
./dbtest $cmdargs
