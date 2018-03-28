#! /usr/bin/env bash

rm dbtest; go build

echo -e "#########################################\n"
ARGS="-db bogn -bogn workset -lsm -key 32 -value 1024"
OPS="-load 1000000 -writes 40000000"
echo "./dbtest $ARGS $OPS"
./dbtest $ARGS $OPS
echo

echo -e "#########################################\n"
ARGS="-db bogn -bogn workset -lsm -randwidth -key 32 -value 1024"
OPS="-load 1000000 -writes 40000000"
echo "./dbtest $ARGS $OPS"
./dbtest $ARGS $OPS
echo
