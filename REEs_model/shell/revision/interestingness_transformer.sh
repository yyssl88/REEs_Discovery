#!/bin/bash

task=(
'airports'
'hospital'
'ncvoter'
'inspection'
'aminer'
)

tid=$1
cid=$2
dirpath=$3
cuda=$4

cd ../../PredicateInterestingnessFilter

echo -e "---------------------------------------------- TRAIN transformer interestingness model -------------------------------------------------------------------"
# 4. Transformer
#for((cid=0;cid<5;cid++)); do
result_file=${dirpath}'/REEs_model_data/revision/results/result_ROUND'${cid}'_transformer_'${task[${tid}]}'.txt'
    #> ${result_file}
ipython ${dirpath}/REEs_model/baselines/transformer_interestingness.ipynb ${task[${tid}]} ${result_file} ${cuda} ${cid}
#done
