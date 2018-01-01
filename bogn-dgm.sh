rm dbtest
go build

BUG1="-seed 1514722699679526000"

TESTARGS="-load 1000000 -writes 40000000"
KVOPTIONS="-lsm -key 32 -value 512 -memcapacity 838860800"
cmdargs="-db bogn -bogn dgm $BUG1 $KVOPTIONS $TESTARGS"

echo "./dbtest $cmdargs"
./dbtest $cmdargs
