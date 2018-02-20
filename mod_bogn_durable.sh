rm dbtest
go build

TESTARGS="-load 1000000 -writes 4000000 -period 25"
KVOPTIONS="-key 32 -value 64 "
cmdargs="-db bogn -bogn durable -lsm $TESTARGS $KVOPTIONS"

echo "./dbtest $cmdargs"
./dbtest $cmdargs
