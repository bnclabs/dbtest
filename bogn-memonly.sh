rm dbtest
go build

TESTARGS="-load 1000000 -writes 40000000"
cmdargs="-db bogn -bogn memonly -lsm $TESTARGS"

echo "./dbtest $cmdargs"
./dbtest $cmdargs
