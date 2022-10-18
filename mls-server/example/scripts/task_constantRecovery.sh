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

suppDefault=0.0001
confDefault=0.9
topKDefault=10000
tupleNumDefault=2
numOfProcessorDefault=20
confFilterThrDefault=0.001
srDefault=0.1
roundDefault=1

dataID=$1
expOption=$2



if [ ${expOption} = "vary_sr" ]
then
  echo -e "---------------- Constant Recovery - varying sample ratio --------------------"

for sr in 0.1 0.2 0.3 0.4
do
   echo -e "sample ratio = "${sr}" with data "${task[${dataID}]}
   ./run_unit_constantRecovery.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${confFilterThrDefault} ${topKDefault} ${roundDefault} ${sr}
done
fi



if [ ${expOption} = "vary_round" ]
then
  echo -e "---------------- Constant Recovery - varying sample round --------------------"

for round in 1 2 3 4 5 6 7 8
do
   echo -e "sample round = "${round}" with data "${task[${dataID}]}
   ./run_unit_constantRecovery.sh ${dataID} ${expOption} ${suppDefault} ${confDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${confFilterThrDefault} ${topKDefault} ${round} ${srDefault}
done
fi



if [ ${expOption} = "vary_supp" ]
then
  echo -e "---------------- Varying support --------------------"

for supp in 0.1 0.01 0.001 0.0001 0.00001
do

     echo -e "supp = "${supp}" with data "${task[${dataID}]}
     ./run_unit_constantRecovery.sh ${dataID} ${expOption} ${supp} ${confDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${confFilterThrDefault} ${topKDefault} ${roundDefault} ${srDefault}

done
fi



if [ ${expOption} = "vary_conf" ]
then
  echo -e "---------------- Varying confidence --------------------"

for conf in 0.95 0.9 0.85 0.8; do

     echo -e "confidence = "${conf}" with data "${task[${dataID}]}
     ./run_unit_constantRecovery.sh ${dataID} ${expOption} ${suppDefault} ${conf} ${tupleNumDefault} ${numOfProcessorDefault} ${confFilterThrDefault} ${topKDefault} ${roundDefault} ${srDefault}

done
fi
