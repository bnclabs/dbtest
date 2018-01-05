rm dbtest
go build

cmdargs="-db llrb -load 1000000 -writes 40000000 -lsm -seed 1515071954080890000"

echo "./dbtest $cmdargs"
./dbtest $cmdargs
