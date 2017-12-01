rm dbtest
go build

cmdargs="-db bubt -load 1000000 -reads 60000000"

echo "./dbtest $cmdargs"
./dbtest $cmdargs
