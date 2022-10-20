#!/bin/bash

task=(
'airports'
'hospital'
'ncvoter'
'inspection'
'aminer'
)

tid=$1

for((cid=0;cid<10;cid++));do
    ./interestingness_ours.sh ${tid} ${cid}
    ./interestingness_ours.sh ${tid} ${cid}
    ./interestingness_ours.sh ${tid} ${cid}
done

