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

mkdir ${dirpath}'/REEs_model_data/revision/labeled_data_400/'${task[${tid}]}'/train'
mkdir ${dirpath}'/REEs_model_data/revision/results/'

cd ../../PredicateInterestingnessFilter

echo -e "---------------------------------------------- TRAIN the BERT MLM interestingness model -------------------------------------------------------------------"
# 2. Bert model
result_file=${dirpath}'/REEs_model_data/revision/results/result_ROUND'${cid}'_bert_'${task[${tid}]}'.txt'
> ${result_file}

ipython ${dirpath}/REEs_model/baselines/bert_mlm.ipynb ${task[tid]}

ipython ${dirpath}/REEs_model/baselines/bert_interestingness.ipynb ${task[${tid}]} True ${cuda} ${result_file} ${cid}






