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

filterEnumNumber=5

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

# use random walk sampling + constant recovery
outputFile_rw_norm_cr='outputResult_'${task[${dataID}]}'__RW__SRatio'${sampleRatio}'__ROUND'${round}'_PREEMiner_NORM_CR_'${expOption}${tailFile}
# use random walk sampling + RL + constant recovery
outputFile_rw_rl_cr='outputResult_'${task[${dataID}]}'__RW__SRatio'${sampleRatio}'__ROUND'${round}'_PREEMiner_RL_CR_'${expOption}${tailFile}



echo -e "---------- Random Walk Sampling + Constant Recovery algorithm"

./run.sh  support=${supp} confidence=${conf} taskID=${task[${dataID}]}'__RW__SRatio'${sampleRatio}'__ROUND'${round} highSelectivityRatio=0 interestingness=1.5 skipEnum=false dataset=${task[${dataID}]}'Sampling' topK=${topK} ifPrune=0 round=1 maxTuplePerREE=${tnum} outputResultFile=${outputFile_rw_norm} algOption="constantRecovery" numOfProcessors=${processor} MLOption=1 confFilterThr=${confFilterThr} ifConfFilter=0 filterEnumNumber=${filterEnumNumber} ifClusterWorkunits=0

rm ${resFile}${outputFile_rw_norm_cr}
hdfs dfs -get "/tmp/rulefind/"${task[${dataID}]}'__RW__SRatio'${sampleRatio}'__ROUND'${round}"/rule_all/"${outputFile_rw_norm} ${resFile}${outputFile_rw_norm_cr}




echo -e "---------- Random Walk Sampling + RL + Constant Recovery algorithm"

./run.sh  support=${supp} confidence=${conf} taskID=${task[${dataID}]}'__RW__SRatio'${sampleRatio}'__ROUND'${round} highSelectivityRatio=0 interestingness=1.5 skipEnum=false dataset=${task[${dataID}]}'Sampling' topK=${topK} ifPrune=0 round=1 maxTuplePerREE=${tnum} outputResultFile=${outputFile_rw_rl} algOption="constantRecovery" numOfProcessors=${processor} MLOption=1 confFilterThr=${confFilterThr} ifConfFilter=0 filterEnumNumber=${filterEnumNumber} ifClusterWorkunits=0

rm ${resFile}${outputFile_rw_rl_cr}
hdfs dfs -get "/tmp/rulefind/"${task[${dataID}]}'__RW__SRatio'${sampleRatio}'__ROUND'${round}"/rule_all/"${outputFile_rw_rl} ${resFile}${outputFile_rw_rl_cr}

