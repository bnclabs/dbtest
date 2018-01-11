rm dbtest
go build

TESTARGS="-load 1000000 -writes 40000000 -period 25"
KVOPTIONS="-key 32 -value 512 -capacity 838860800"
cmdargs="-db bogn -bogn dgm -lsm $KVOPTIONS $TESTARGS"

echo "./dbtest $cmdargs"
./dbtest $cmdargs
