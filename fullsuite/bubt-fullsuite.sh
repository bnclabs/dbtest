rm dbtest
go build

echo "########## TESTSUITE-1 (load 1M) ##########\n"
cmdargs="-db bubt -load 1000000"
echo "./dbtest $cmdargs"
./dbtest $cmdargs
echo "\n\n"

echo "########## TESTSUITE-2 (read 60M) ##########\n"
cmdargs="-db bubt -load 1000000 -reads 60000000"
echo "./dbtest $cmdargs"
./dbtest $cmdargs
echo "\n\n"

echo "########## TESTSUITE-3 (load 10M read 10M) ##########\n"
cmdargs="-db bubt -load 10000000 -reads 1000000"
echo "./dbtest $cmdargs"
./dbtest $cmdargs
echo "\n\n"

echo "########## TESTSUITE-3 (1K values) ##########\n"
cmdargs="-db bubt -keylen 64 -vallen 1024 -load 10000000 -reads 1000000"
echo "./dbtest $cmdargs"
./dbtest $cmdargs
echo "\n\n"

echo "########## TESTSUITE-3 (16-byte values) ##########\n"
cmdargs="-db bubt -keylen 64 -vallen 16 -load 10000000 -reads 1000000"
echo "./dbtest $cmdargs"
./dbtest $cmdargs
echo "\n\n"

echo "########## TESTSUITE-3 (full site) ##########\n"
cmdargs="-db bubt -keylen 64 -vallen 64 -cpu 16 -load 100000000 -reads 60000000"
echo "./dbtest $cmdargs"
./dbtest $cmdargs
echo "\n\n"
