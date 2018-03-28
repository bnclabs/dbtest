#! /usr/bin/env bash

rm dbtest; go build

CASEARGS="-seed 1514729382401081000"
TESTARGS="-load 1000000 -writes 40000000"
cmdargs="-db bogn -bogn memonly -lsm $TESTARGS $CASEARGS"

echo "./dbtest $cmdargs"
./dbtest $cmdargs

