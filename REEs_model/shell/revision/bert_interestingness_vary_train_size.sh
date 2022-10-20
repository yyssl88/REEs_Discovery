#!/bin/bash

task=(
'airports'
'hospital'
'ncvoter'
'inspection'
'aminer'
)

tid=$1
train_ratio=$2
dirpath=$3

cd ../../PredicateInterestingnessFilter

echo -e "---------------------------------------------- TRAIN the BERT interestingness model -------------------------------------------------------------------"
# 2. Bert model
result_file=${dirpath}'/REEs_model_data/revision/results_ts/result_bert_TS'${train_ratio}'_'${task[${tid}]}'.txt'
> ${result_file}
ipython ${dirpath}/REEs_model/baselines/bert_interestingness_vary_train_size.ipynb ${task[${tid}]} False ${result_file} ${train_ratio}


