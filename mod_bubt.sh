rm dbtest
go build

cmdargs="-db bubt -load 10000000 -reads 60000000"
echo "./dbtest $cmdargs"
./dbtest $cmdargs
