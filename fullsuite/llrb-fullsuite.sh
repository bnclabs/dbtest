rm dbtest
go build

echo "########## TESTSUITE-1 (load 10M) ##########\n"
cmdargs="-db llrb -load 10000000"
echo "./dbtest $cmdargs"
./dbtest $cmdargs
echo "\n\n"

echo "########## TESTSUITE-2 (load 1K val) ##########\n"
cmdargs="-db llrb -load 10000000 -cpu 10 -keylen 256 -vallen 1024"
echo "./dbtest $cmdargs"
./dbtest $cmdargs
echo "\n\n"

echo "########## TESTSUITE-3 (writes) ##########\n"
cmdargs="-db llrb -load 1000000 -writes 1000000"
echo "./dbtest $cmdargs"
./dbtest $cmdargs
echo "\n\n"

echo "########## TESTSUITE-4 (writes lsm) ##########\n"
cmdargs="-db llrb -load 1000000 -writes 1000000 -lsm"
echo "./dbtest $cmdargs"
./dbtest $cmdargs
echo "\n\n"

echo "########## TESTSUITE-5 (fullset single core) ##########\n"
cmdargs="-db llrb -load 1000000 -writes 4000000 -lsm -cpu 1 -keylen 64 -vallen 256"
echo "./dbtest $cmdargs"
./dbtest $cmdargs
echo "\n\n"

echo "########## TESTSUITE-6 (fullset many cores) ##########\n"
cmdargs="-db llrb -load 1000000 -writes 4000000 -lsm -cpu 8 -keylen 64 -vallen 256"
echo "./dbtest $cmdargs"
./dbtest $cmdargs
echo "\n\n"
