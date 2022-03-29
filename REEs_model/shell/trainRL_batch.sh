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


for tid in 1 3 4 5 6 8 13
do

echo -e "tid: "${tid}
log='log/log_'${task[${tid}]}'.txt'
> ${log}
./trainRL_new.sh ${tid} >> ${log}

done
