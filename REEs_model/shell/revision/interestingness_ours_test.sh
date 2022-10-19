#!/bin/bash

task=(
'airports'
'hospital'
'ncvoter'
'inspection'
'aminer'
)

tid=$1
cid=10 ## cross validation

mkdir ${dirpath}'/REEs_model_data/revision/test/'${task[${tid}]}'/train'

# 1. Our model
echo -e "---------------------------------------------- TRAIN the rule interestingness model -------------------------------------------------------------------"
result_file=${dirpath}'/REEs_model_data/revision/results/result_TEST'${cid}'_Mbi_'${task[${tid}]}'.txt'
#> ${result_file}
# 1.1 bert prepare
cd ../../PredicateInterestingnessFilter
#ipython interestingnessBertEmbedsPrepare.ipynb ${task[${tid}]}

# 1.2 run M_bi
rules_file=${dirpath}'/REEs_model_data/revision/test/'${task[${tid}]}'/rules.txt'
train_file=${dirpath}'/REEs_model_data/revision/test/'${task[${tid}]}'/train_'${cid}'/train.csv'
valid_file=${dirpath}'/REEs_model_data/revision/test/'${task[${tid}]}'/train_'${cid}'/valid.csv'
test_file=${dirpath}'/REEs_model_data/revision/test/'${task[${tid}]}'/train_'${cid}'/test.csv'
predicates_path=${dirpath}'/REEs_model_data/revision/labeled_data_400/'${task[${tid}]}'/train/all_predicates.txt'
interestingness_model_path=${dirpath}"/REEs_model_data/revision/test/"${task[${tid}]}"/train_"${cid}"/model/"
vobs_file=${dirpath}'/REEs_model_data/revision/labeled_data_400/'${task[${tid}]}'/train/tokenVobs.txt'
mkdir ${interestingness_model_path}
interestingness_model_txt_path=${dirpath}'/REEs_model_data/revision/test/'${task[${tid}]}'/train_'${cid}'/model.txt'
pretrained_matrix_file=${dirpath}'/REEs_model_data/revision/labeled_data_400/'${task[${tid}]}'/train/tokenEmbedds.pkl'


python interestingnessFixedEmbedsWithObj_main_test.py -rules_file ${rules_file} -train_file ${train_file} --train_ratio 1.0 -valid_file ${valid_file} -test_file ${test_file} -model_file  ${interestingness_model_path} -vobs_file ${vobs_file} -predicates_path ${predicates_path} -model_txt_file ${interestingness_model_txt_path} -pretrained_matrix_file ${pretrained_matrix_file} #>> ${result_file}




