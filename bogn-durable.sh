rm dbtest
go build

TESTARGS="-load 1000000 -writes 4000000"
cmdargs="-db bogn -bogn durable -lsm -period 25 $TESTARGS"

echo "./dbtest $cmdargs"
./dbtest $cmdargs
