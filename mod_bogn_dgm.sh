#! /usr/bin/env bash

rm dbtest; go build

echo -e "#########################################\n"
ARGS1="-db bogn -bogn dgm -lsm -key 32 -value 1024"
ARGS2="-capacity 838860800 -autocommit 25"
OPS="-load 100000 -writes 40000000"
echo "./dbtest $ARGS1 $ARGS2 $OPS"
./dbtest $ARGS1 $ARGS2 $OPS
echo

echo -e "#########################################\n"
ARGS1="-db bogn -bogn dgm -lsm -key 32 -value 1024 -randwidth"
ARGS2="-capacity 838860800 -autocommit 25"
OPS="-load 1000000 -writes 40000000"
echo "./dbtest $ARGS1 $ARGS2 $OPS"
./dbtest $ARGS1 $ARGS2 $OPS
echo
