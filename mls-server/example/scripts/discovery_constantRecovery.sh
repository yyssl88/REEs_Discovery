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


dataID=1
expOption="vary_sr"
./task_constantRecovery.sh ${dataID} ${expOption}

dataID=1
expOption="vary_round"
./task_constantRecovery.sh ${dataID} ${expOption}
