#!/bin/bash

echo -e "varying sample ratio or sample round for airports dataset"

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

dataID=1
expOption="vary_sr"

suppDefault=0.0001
confDefault=0.9
topKDefault=10000
tupleNumDefault=2
numOfProcessorDefault=20
confFilterThrDefault=0.001
srDefault=0.1
roundDefault=1


if [ ${expOption} = "vary_sr" ]
then
  echo -e "---------------- Varying sample ratio --------------------"

for sr in 0.1 0.2 0.3 0.4
do
   echo -e "sample ratio = "${sr}" with data "${task[${dataID}]}
   ./run_unit_sampling.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${confFilterThrDefault} ${topKDefault} ${roundDefault} ${sr}
done
fi



if [ ${expOption} = "vary_round" ]
then
  echo -e "---------------- Varying sample round --------------------"

for round in 1 2 3 4 5 6 7 8
do
   echo -e "sample round = "${round}" with data "${task[${dataID}]}
   ./run_unit_sampling.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${confFilterThrDefault} ${topKDefault} ${round} ${srDefault}
done
fi
