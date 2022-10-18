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
processor=$6
confFilterThr=$7
topK=$8
round=$9
sampleRatio=${10}

cd ..

resRootFile="./discoveryResults/"

mkdir ${resRootFile}

resFile=${resRootFile}${task[${dataID}]}"/"

mkdir ${resFile}

echo -e "result output file "${resFile}

tailFile="_supp"${supp}"_conf"${conf}"_tnum"${tnum}"_topK"${topK}"_processor"${processor}".txt"


echo -e "information : "${tailFile}

# use random walk sampling
outputFile_rw_norm='outputResult_'${task[${dataID}]}'__RW__SRatio'${sampleRatio}'__ROUND'${round}'_PREEMiner_NORM_'${expOption}${tailFile}
# use random walk sampling + RL
outputFile_rw_rl='outputResult_'${task[${dataID}]}'__RW__SRatio'${sampleRatio}'__ROUND'${round}'_PREEMiner_RL_'${expOption}${tailFile}



echo -e "---------- Random Walk Sampling + NORM algorithm ----------"

./run.sh  support=${supp} confidence=${conf} taskID=${task[${dataID}]}'__RW__SRatio'${sampleRatio}'__ROUND'${round} highSelectivityRatio=0 interestingness=1.5 skipEnum=false dataset=${task[${dataID}]}'Sampling' topK=${topK} ifPrune=0 round=1 maxTuplePerREE=${tnum} outputResultFile=${outputFile_rw_norm} algOption="discoverySampling" numOfProcessors=${processor} MLOption=1 confFilterThr=${confFilterThr} ifConfFilter=0 ifClusterWorkunits=0 filterEnumNumber=${filterEnumNumber} ifDQN=false

rm ${resFile}${outputFile_rw_norm}
hdfs dfs -get "/tmp/rulefind/"${task[${dataID}]}"__RW__SRatio"${sampleRatio}"__ROUND"${round}"/rule_all/"${outputFile_rw_norm} ${resFile}

# backup result for constant recovery
crPath="/tmp/rulefind/constantRecovery/"${task[${dataID}]}"__RW__SRatio"${sampleRatio}"__ROUND"${round}"/rule_all/"
hdfs dfs -mkdir -p ${crPath}
hdfs dfs -rm ${crPath}${outputFile_rw_norm}
hdfs dfs -mv "/tmp/rulefind/"${task[${dataID}]}"__RW__SRatio"${sampleRatio}"__ROUND"${round}"/rule_all/"${outputFile_rw_norm} ${crPath}




echo -e "---------- Random Walk Sampling + RL algorithm ----------"

./run.sh  support=${supp} confidence=${conf} taskID=${task[${dataID}]}'__RW__SRatio'${sampleRatio}'__ROUND'${round} highSelectivityRatio=0 interestingness=1.5 skipEnum=false dataset=${task[${dataID}]}'Sampling' topK=${topK} ifPrune=0 round=1 maxTuplePerREE=${tnum} outputResultFile=${outputFile_rw_rl} algOption="discoverySampling" numOfProcessors=${processor} MLOption=1 confFilterThr=${confFilterThr} ifConfFilter=1 ifClusterWorkunits=0 filterEnumNumber=${filterEnumNumber} ifDQN=true DQNModelFile=${task[${dataID}]}"_model.txt" DQNThreshold=${DQNThreshold}

rm ${resFile}${outputFile_rw_rl}
hdfs dfs -get "/tmp/rulefind/"${task[${dataID}]}'__RW__SRatio'${sampleRatio}'__ROUND'${round}"/rule_all/"${outputFile_rw_rl} ${resFile}

# backup result for constant recovery
crPath="/tmp/rulefind/constantRecovery/"${task[${dataID}]}"__RW__SRatio"${sampleRatio}"__ROUND"${round}"/rule_all/"
hdfs dfs -mkdir -p ${crPath}
hdfs dfs -rm ${crPath}${outputFile_rw_rl}
hdfs dfs -mv "/tmp/rulefind/"${task[${dataID}]}"__RW__SRatio"${sampleRatio}"__ROUND"${round}"/rule_all/"${outputFile_rw_rl} ${crPath}

