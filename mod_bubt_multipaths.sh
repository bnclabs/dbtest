#! /usr/bin/env bash

rm dbtest; go build

echo -e "#########################################\n"
ARGS="-db bubt -key 32 -value 1024 -npaths 3"
LOADS="-load 10000000"
READS="-reads 60000000"
echo "./dbtest $ARGS $LOADS $READS"
./dbtest $ARGS $LOADS $READS
echo

echo -e "#########################################\n"
ARGS="-db bubt -key 32 -value 1024 -npaths 3 -randwidth"
LOADS="-load 10000000"
READS="-reads 60000000"
echo "./dbtest $ARGS $LOADS $READS"
./dbtest $ARGS $LOADS $READS
echo
