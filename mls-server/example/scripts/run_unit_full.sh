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
"property"
)

filterEnumNumber=10
DQNThreshold=0.95

dataID=$1
expOption=$2
supp=$3
conf=$4
tnum=$5
processor=$5
confFilterThr=$7
topK=$8


cd ..


resRootFile="./discoveryResults/"

mkdir ${resRootFile}

resFile=${resRootFile}${task[${dataID}]}"/"
mkdir ${resFile}

echo -e "result output file "${resFile}

tailFile="_supp"${supp}"_conf"${conf}"_tnum"${tnum}"_topK"${topK}"_processor"${processor}".txt"

echo -e "information : "${tailFile}

outputFile_prminer='outputResult_'${task[${dataID}]}"_prminer_"${expOption}${tailFile}
outputFile_prminernoRL='outputResult_'${task[${dataID}]}"_prminer_noRL_"${expOption}${tailFile}



echo -e "---------- PRMiner algorithm ----------"

./run.sh  support=${supp} confidence=${conf} taskID=${task[${dataID}]} highSelectivityRatio=0 interestingness=1.5 skipEnum=false dataset=${task[${dataID}]} topK=${topK} ifPrune=0 round=1 maxTuplePerREE=${tnum} outputResultFile=${outputFile_prminer} algOption="discoverySampling" numOfProcessors=${processor} MLOption=1 confFilterThr=${confFilterThr} ifConfFilter=1 ifClusterWorkunits=0 filterEnumNumber=${filterEnumNumber=} ifDQN=true DQNModelFile=${task[${dataID}]}"_model.txt" DQNThreshold=${DQNThreshold}

rm ${resFile}${outputFile_prminer}
hdfs dfs -get "/tmp/rulefind/"${task[${dataID}]}"/rule_all/"${outputFile_prminer} ${resFile}




echo -e "---------- PRMiner-noRL algorithm ----------"

./run.sh  support=${supp} confidence=${conf} taskID=${task[${dataID}]} highSelectivityRatio=0 interestingness=1.5 skipEnum=false dataset=${task[${dataID}]} topK=${topK} ifPrune=0 round=1 maxTuplePerREE=${tnum} outputResultFile=${outputFile_prminernoRL} algOption="discoverySampling" numOfProcessors=${processor} MLOption=1 confFilterThr=${confFilterThr} ifConfFilter=0 ifClusterWorkunits=0 filterEnumNumber=${filterEnumNumber} ifDQN=false

rm ${resFile}${outputFile_prminernoRL}
hdfs dfs -get "/tmp/rulefind/"${task[${dataID}]}"/rule_all/"${outputFile_prminernoRL} ${resFile}



