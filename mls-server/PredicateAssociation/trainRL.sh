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


for dataID in 1 3 4 5 6 8 9 10 11 12; do

  python train_from_file.py -data_name ${task[${dataID}]} -N 100 -epoch 1000

done


dataID=4
for N_num in 10 100 200 500 1000; do

  python train_from_file.py -data_name ${task[${dataID}]} -N ${N_num} -epoch 1000

done