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

exp=(
"vary_n"
"vary_supp"
"vary_conf"
"vary_k"
"vary_tuples"
"vary_topk"
"vary_synD"
"vary_synD_n"
)


objectiveFea1=0.3
objectiveFea2=0.2
objectiveFea3=0.2
objectiveFea4=0.2
subjectiveFea=0.1
filterEnumNumber=10


dataID=$1

interestingness_directory="/tmp/rulefind/interestingness/"${task[${dataID}]}"_topk"
tokenToIDFile=${interestingness_directory}"/tokenVobs.txt"
interestingnessModelFile=${interestingness_directory}"/interestingnessModel.txt"
filterRegressionFile=${interestingness_directory}"/filterRegressionModel.txt"

expOption=$2

supp=$3
conf=$4
topK=$5
tnum=$6
processor=$7

confFilterThr=$8


cd ..

resRootFile="./discoveryResults/"

mkdir ${resRootFile}

resFile=${resRootFile}${task[${dataID}]}"/"

mkdir ${resFile}

echo -e "result output file "${resFile}

tailFile="_supp"${supp}"_conf"${conf}"_topK"${topK}"_tnum"${tnum}"_processor"${processor}".txt"

echo -e "information : "${tailFile}

outputFile_ptopkminer='outputResult_'${task[${dataID}]}"_ptopkminer_"${expOption}${tailFile}
outputFile_ptopkminernop='outputResult_'${task[${dataID}]}"_ptopkminer_nop_"${expOption}${tailFile}
outputFile_ptopkminernoL='outputResult_'${task[${dataID}]}"_ptopkminer_noL_"${expOption}${tailFile}
outputFile_dcfinder='outputResult_'${task[${dataID}]}"_dcfinder_"${expOption}${tailFile}



echo -e "---------- PTopk-Miner algorithm ----------"

./run.sh  support=${supp} confidence=${conf} taskID=${task[${dataID}]} highSelectivityRatio=0 interestingness=1.5 skipEnum=false dataset=${task[${dataID}]} topK=${topK} round=1 maxTuplePerREE=${tnum} ifPrune=1 confFilterThr=${confFilterThr} objectiveFea1=${objectiveFea1} objectiveFea2=${objectiveFea2} objectiveFea3=${objectiveFea3} objectiveFea4=${objectiveFea4} subjectiveFea=${subjectiveFea} outputResultFile=${outputFile_ptopkminer} algOption="discoveryNew" numOfProcessors=${processor} MLOption=1 ifRL=0 ifOnlineTrainRL=0 ifOfflineTrainStage=0 ifClusterWorkunits=0 filterEnumNumber=${filterEnumNumber} ifDQN=false ifConfFilter=0 tokenToIDFile=${tokenToIDFile} interestingnessModelFile=${interestingnessModelFile} filterRegressionFile=${filterRegressionFile} topKOption="allFiltering" useConfHeuristic=true

rm ${resFile}${outputFile_ptopkminer}
hdfs dfs -get "/tmp/rulefind/"${task[${dataID}]}"/rule_all/"${outputFile_ptopkminer} ${resFile}




echo -e "---------- PTopk-Miner-noL algorithm ----------"

./run.sh  support=${supp} confidence=${conf} taskID=${task[${dataID}]} highSelectivityRatio=0 interestingness=1.5 skipEnum=false dataset=${task[${dataID}]} topK=${topK} round=1 maxTuplePerREE=${tnum} ifPrune=0 confFilterThr=${confFilterThr} objectiveFea1=${objectiveFea1} objectiveFea2=${objectiveFea2} objectiveFea3=${objectiveFea3} objectiveFea4=${objectiveFea4} subjectiveFea=${subjectiveFea} outputResultFile=${outputFile_ptopkminernoL} algOption="discoveryNew" numOfProcessors=${processor} MLOption=1 ifRL=0 ifOnlineTrainRL=0 ifOfflineTrainStage=0 ifClusterWorkunits=0 filterEnumNumber=${filterEnumNumber} ifDQN=false ifConfFilter=0 tokenToIDFile=${tokenToIDFile} interestingnessModelFile=${interestingnessModelFile} filterRegressionFile=${filterRegressionFile} topKOption="partialFiltering"

rm ${resFile}${outputFile_ptopkminernoL}
hdfs dfs -get "/tmp/rulefind/"${task[${dataID}]}"/rule_all/"${outputFile_ptopkminernoL} ${resFile}




echo -e "---------- PTopk-Miner-nop algorithm ----------"

./run.sh  support=${supp} confidence=${conf} taskID=${task[${dataID}]} highSelectivityRatio=0 interestingness=1.5 skipEnum=false dataset=${task[${dataID}]} topK=${topK} round=1 maxTuplePerREE=${tnum} ifPrune=0 confFilterThr=${confFilterThr} objectiveFea1=${objectiveFea1} objectiveFea2=${objectiveFea2} objectiveFea3=${objectiveFea3} objectiveFea4=${objectiveFea4} subjectiveFea=${subjectiveFea} outputResultFile=${outputFile_ptopkminernop} algOption="discoveryNew" numOfProcessors=${processor} MLOption=1 ifRL=0 ifOnlineTrainRL=0 ifOfflineTrainStage=0 ifClusterWorkunits=0 filterEnumNumber=${filterEnumNumber} ifDQN=false ifConfFilter=0 tokenToIDFile=${tokenToIDFile} interestingnessModelFile=${interestingnessModelFile} filterRegressionFile=${filterRegressionFile} topKOption="noFiltering"

rm ${resFile}${outputFile_ptopkminernop}
hdfs dfs -get "/tmp/rulefind/"${task[${dataID}]}"/rule_all/"${outputFile_ptopkminernop} ${resFile}




echo -e "---------- DCFinder algorithm ----------"

./run.sh  support=${supp} confidence=${conf} taskID=${task[${dataID}]} highSelectivityRatio=0 interestingness=1.5 skipEnum=false dataset=${task[${dataID}]} topK=${topK} round=1 maxTuplePerREE=2 ifPrune=1 confFilterThr=${confFilterThr} objectiveFea1=${objectiveFea1} objectiveFea2=${objectiveFea2} objectiveFea3=${objectiveFea3} objectiveFea4=${objectiveFea4} subjectiveFea=${subjectiveFea} outputResultFile=${outputFile_dcfinder} algOption="discoveryES" numOfProcessors=${processor} MLOption=0

rm ${resFile}${outputFile_dcfinder}
hdfs dfs -get "/tmp/rulefind/"${task[${dataID}]}"/rule_all/"${outputFile_dcfinder} ${resFile}

