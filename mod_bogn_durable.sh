#! /usr/bin/env bash

rm dbtest; go build

echo -e "#########################################\n"
ARGS="-db bogn -bogn durable -lsm -autocommit 25 -key 32 -value 1024"
OPS="-load 1000000 -writes 2000000"
echo "./dbtest $ARGS $OPS"
./dbtest $ARGS $OPS
echo

echo -e "#########################################\n"
ARGS="-db bogn -bogn durable -lsm -autocommit 25 -key 32 -value 1024 -randwidth"
OPS="-load 1000000 -writes 2000000"
echo "./dbtest $ARGS $OPS"
./dbtest $ARGS $OPS
echo
