#!/bin/bash

task=(
"adults"
"airports"
"flight"
"hospital"
"inspection"
"ncvoter"
"aminer"
"tax100w"
"tax200w"
"tax400w"
"tax600w"
"tax800w"
"tax1000w"
)

dataID=1  # airports
expOption="vary_supp"

suppDefault=0.00001
confDefault=0.85
topKDefault=10
tupleNumDefault=2
numOfProcessorDefault=20
confFilterThrDefault=0.001

./run_unit.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${topKDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${confFilterThrDefault}

