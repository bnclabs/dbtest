#! /usr/bin/env bash

rm dbtest; go build

echo -e "#########################################\n"
ARGS="-db bubt -key 32 -value 1024 -log bubt"
LOADS="-load 10000000"
READS="-reads 60000000"
echo "./dbtest $ARGS $LOADS $READS"
./dbtest $ARGS $LOADS $READS
echo

echo -e "#### with random ########################\n"
ARGS="-db bubt -key 32 -value 1024 -randwidth -log bubt"
LOADS="-load 10000000"
READS="-reads 60000000"
echo "./dbtest $ARGS $LOADS $READS"
./dbtest $ARGS $LOADS $READS
echo
