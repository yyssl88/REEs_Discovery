#!/bin/bash

echo -e "varying support, confidence, and n for airports dataset"

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
expOption="vary_supp"

suppDefault=0.0001
confDefault=0.9
topKDefault=10000
tupleNumDefault=2
numOfProcessorDefault=20
confFilterThrDefault=0.001



if [ ${expOption} = "vary_supp" ]
then
  echo -e "---------------- Varying support --------------------"

for supp in 0.1 0.01 0.001 0.0001 0.00001
do

     echo -e "supp = "${supp}" with data "${task[${dataID}]}
     ./run_unit_full.sh ${dataID} ${expOption} ${supp} ${confDefault} ${tupleNumDefault} ${numOfProcessorDefault} ${confFilterThrDefault} ${topKDefault}

done
fi



if [ ${expOption} = "vary_conf" ]
then
  echo -e "---------------- Varying confidence --------------------"

for conf in 0.95 0.9 0.85 0.8
do
     echo -e "confidence = "${conf}" with data "${task[${dataID}]}
     ./run_unit_full.sh ${dataID} ${expOption} ${suppDefault} ${conf} ${tupleNumDefault} ${numOfProcessorDefault} ${confFilterThrDefault} ${topKDefault}

done

fi



if [ ${expOption} = "vary_n" ]
then
  echo -e "---------------- Varying number of processors --------------------"

for processor in 20 16 12 8 4
do
     echo -e "processor = "${processor}" with data "${task[${dataID}]}
     ./run_unit_full.sh ${dataID} ${expOption}'_Figi_' ${suppDefault} ${confDefault} ${tupleNumDefault} ${processor} ${confFilterThrDefault} ${topKDefault}

done
fi
