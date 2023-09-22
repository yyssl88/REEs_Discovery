#!/bin/bash

tid=$1

destdir=$2

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

directory=$3
# data_dir=${directory}"REEs_model_data/diversified_data/"${task[${tid}]}'/'
data_dir=${directory}"REEs_model_data/labeled_data_400/"${task[${tid}]}'/train/'

interestingness_model_txt_path=${directory}"REEs_model_data/temp_results/Interestingness/interestingness_txt_model/"${task[${tid}]}"/model.txt"

vobs_file=${directory}"REEs_model_data/temp_results/Interestingness/interestingness_model/"${task[${tid}]}"/tokenVobs.txt"

predicates_path=${data_dir}"all_predicates.txt"

filter_model_path=${directory}'REEs_model_data/temp_results/Interestingness/filter_interestingness_model/'${task[${tid}]}'/model.txt'


dest=${destdir}'/'${task[${tid}]}'_topk/'
mkdir ${dest}

interestingnessDestTxtFile=${dest}'interestingnessModel.txt'
vobsDestFile=${dest}'tokenVobs.txt'
predicatesDestFile=${dest}${task[${tid}]}'_predicates.txt'
filterDestFile=${dest}'filterRegressionModel.txt'

cp ${interestingness_model_txt_path} ${interestingnessDestTxtFile}

cp ${vobs_file} ${vobsDestFile}

cp ${predicates_path} ${predicatesDestFile}

cp ${filter_model_path} ${filterDestFile}

