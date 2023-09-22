#!/bin/bash

tid=$1

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

directory=$2
# data_dir=${directory}"REEs_model_data/diversified_data/"${task[${tid}]}'/'
data_dir=${directory}"REEs_model_data/labeled_data_400/"${task[${tid}]}'/train/'

# 1. train the rule interestingness model
echo -e "---------------------------------------------- TRAIN the rule interestingness model -------------------------------------------------------------------"
rules_file=${data_dir}'rules.txt'
train_file=${data_dir}'train.csv'
test_file=${data_dir}'test.csv'
predicates_path=${data_dir}"all_predicates.txt"

optionIfObj=True
interestingness_model_path=${directory}"REEs_model_data/temp_results/Interestingness/interestingness_model/"${task[${tid}]}"/"
vobs_file=${directory}"REEs_model_data/temp_results/Interestingness/interestingness_model/"${task[${tid}]}"/tokenVobs.txt"
mkdir -p ${interestingness_model_path}
interestingness_model_txt_path=${directory}"REEs_model_data/temp_results/Interestingness/interestingness_txt_model/"${task[${tid}]}"/model.txt"
mkdir -p ${directory}"REEs_model_data/temp_results/Interestingness/interestingness_txt_model/"${task[${tid}]}'/'

python ./REEs_model/PredicateInterestingnessFilter/interestingnessFixedEmbedsWithObj_main.py -rules_file ${rules_file} -train_file ${train_file} -test_file ${test_file} -model_file ${interestingness_model_path} -vobs_file ${vobs_file} -predicates_path ${predicates_path} -model_txt_file ${interestingness_model_txt_path} -optionIfObj ${optionIfObj} -epoch 500 -lr 0.001

# 2. learn DQNInterestingness
echo -e "---------------------------------------------- LEARN the dqn interestingness model -------------------------------------------------------------------"
dqn_model_dir=${directory}'REEs_model_data/temp_results/Interestingness/dqn_interestingness_model/'${task[${tid}]}$'/'
mkdir -p ${dqn_model_dir}
python ./REEs_model/PredicateInterestingnessFilter/RL_interestingness_main.py -predicates_path ${predicates_path} -model_path ${dqn_model_dir} -vobs_file ${vobs_file} -interestingness_model_path ${interestingness_model_path} -optionIfObj ${optionIfObj}

# 3. generate filter interestingness
echo -e "---------------------------------------------- GENERATE the training instances of filter model -------------------------------------------------------------------"
filter_data_dir=${directory}'REEs_model_data/temp_results/Interestingness/filter_interestingness_data/'${task[${tid}]}'/'
mkdir -p ${filter_data_dir}
python ./REEs_model/PredicateInterestingnessFilter/Filter_generate_interestingness_main.py -interestingness_model_path ${interestingness_model_path} -dqn_model_path ${dqn_model_dir} -filter_dir ${filter_data_dir} -predicates_path ${predicates_path} -vobs_file ${vobs_file} -optionIfObj ${optionIfObj}


# 4. train the filter regression model
echo -e "---------------------------------------------- TRAIN the filter regressor model -------------------------------------------------------------------"
filter_model_dir=${directory}'REEs_model_data/temp_results/Interestingness/filter_interestingness_model/'${task[${tid}]}'/'
mkdir -p ${filter_model_dir}
filter_model_path=${filter_model_dir}'model.txt'
python ./REEs_model/PredicateInterestingnessFilter/Filter_regressor_main.py -epoch 300 -model_path ${filter_model_path} -filter_dir ${filter_data_dir}


# collect all needed files
destdir='./Interestingness/final_files'
mkdir ${destdir}
./transferInterestingnessFiles.sh ${tid} ${destdir}
