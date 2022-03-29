#!/bin/bash

tid=1

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

# 1. train the rule interestingness model
echo -e "---------------------------------------------- TRAIN the rule interestingness model -------------------------------------------------------------------"
rules_file="/opt/disk1/yaoshuw/discovery/trainDQN/Interestingness/interestingness_labels/"${task[${tid}]}'/rules.csv'
train_file="/opt/disk1/yaoshuw/discovery/trainDQN/Interestingness/interestingness_labels/"${task[${tid}]}'/train/train.csv'
test_file="/opt/disk1/yaoshuw/discovery/trainDQN/Interestingness/interestingness_labels/"${task[${tid}]}'/train/test.csv'
predicates_path="/opt/disk1/yaoshuw/discovery/trainDQN/all_predicates/"${task[${tid}]}"_predicates.txt"
interestingness_model_path="/opt/disk1/yaoshuw/discovery/trainDQN/Interestingness/interestingness_model/"${task[${tid}]}"/"
vobs_file="/opt/disk1/yaoshuw/discovery/trainDQN/Interestingness/interestingness_model/"${task[${tid}]}"/tokenVobs.txt"
mkdir ${interestingness_model_path}
interestingness_model_txt_path="/opt/disk1/yaoshuw/discovery/trainDQN/Interestingness/interestingness_txt_model/"${task[${tid}]}"/model.txt"
mkdir '/opt/disk1/yaoshuw/discovery/trainDQN/Interestingness/interestingness_txt_model/'
mkdir '/opt/disk1/yaoshuw/discovery/trainDQN/Interestingness/interestingness_txt_model/'${task[${tid}]}'/'
/root/anaconda3/bin/python3 REEs_model/PredicateInterestingnessFilter/interestingnessFixedEmbeds_main.py -rules_file ${rules_file} -train_file ${train_file} -test_file ${test_file} -model_file ${interestingness_model_path} -vobs_file ${vobs_file} -all_predicates_file ${predicates_path} -model_txt_file ${interestingness_model_txt_path}

# 2. learn DQNInterestingness
echo -e "---------------------------------------------- LEARN the dqn interestingness model -------------------------------------------------------------------"
dqn_model_dir='/opt/disk1/yaoshuw/discovery/trainDQN/Interestingness/dqn_interestingness_model/'${task[${tid}]}$'/'
mkdir '/opt/disk1/yaoshuw/discovery/trainDQN/Interestingness/dqn_interestingness_model/'
mkdir ${dqn_model_dir}
#/root/anaconda3/bin/python3 REEs_model/PredicateInterestingnessFilter/RL_interestingness_main.py -predicates_path ${predicates_path} -model_path ${dqn_model_dir} -vobs_file ${vobs_file} -interestingness_model_path ${interestingness_model_path}

# 3. generate filter interestingness
echo -e "---------------------------------------------- GENERATE the training instances of filter model -------------------------------------------------------------------"
filter_data_dir='/opt/disk1/yaoshuw/discovery/trainDQN/Interestingness/filter_interestingness_data/'${task[${tid}]}'/'
mkdir '/opt/disk1/yaoshuw/discovery/trainDQN/Interestingness/filter_interestingness_data/'
mkdir ${filter_data_dir}
#/root/anaconda3/bin/python3 REEs_model/PredicateInterestingnessFilter/Filter_generate_interestingness_main.py -interestingness_model_path ${interestingness_model_path} -dqn_model_path ${dqn_model_dir} -filter_dir ${filter_data_dir} -predicates_path ${predicates_path} -vobs_file ${vobs_file}

# 4. train the filter regression model
echo -e "---------------------------------------------- TRAIN the filter regressor model -------------------------------------------------------------------"
filter_model_dir='/opt/disk1/yaoshuw/discovery/trainDQN/Interestingness/filter_interestingness_model/'${task[${tid}]}'/'
mkdir '/opt/disk1/yaoshuw/discovery/trainDQN/Interestingness/filter_interestingness_model'
mkdir ${filter_model_dir}
#/root/anaconda3/bin/python3 REEs_model/PredicateInterestingnessFilter/Filter_regressor_main.py -epoch 200 -model_path ${filter_model_dir} -filter_dir ${filter_data_dir} -predicates_path ${predicates_path}






