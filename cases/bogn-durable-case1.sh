rm dbtest
go build

CASEARGS="-seed 1514785126379031000"
TESTARGS="-load 1000000 -writes 4000000"
cmdargs="-db bogn -bogn durable -lsm $TESTARGS $CASEARGS"

echo "./dbtest $cmdargs"
./dbtest $cmdargs
