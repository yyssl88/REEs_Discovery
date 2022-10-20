#!/bin/bash

task=(
"airports"
"hospital"
"ncvoter"
"inspection"
)

tid=$1

mkdir ${dirpath}'/REEs_model_data/revision/rank_sub/'${task[${tid}]}'/train'

cd ../../PredicateInterestingnessFilter
ipython interestingnessBertEmbedsPrepareForRank.ipynb ${task[${tid}]}

# train our model
for((cid=0;cid<10;cid++));do
    echo '---------------------------------------------------- task '${cid}'------------------------------------------------------------------------------'
    rules_file=${dirpath}'/REEs_model_data/revision/rank/'${task[${tid}]}'/train/rules.txt'
    train_file=${dirpath}'/REEs_model_data/revision/rank/'${task[${tid}]}'/train/train_'${cid}'.csv'
    valid_file=${dirpath}'/REEs_model_data/revision/rank/'${task[${tid}]}'/train/valid_'${cid}'.csv'
    test_file=${dirpath}'/REEs_model_data/revision/rank/'${task[${tid}]}'/train/test_'${cid}'.csv'
    predicates_path=${dirpath}'/REEs_model_data/revision/rank/'${task[${tid}]}'/train/all_predicates.txt'
    interestingness_model_path=${dirpath}"/REEs_model_data/revision/rank/"${task[${tid}]}"/train/model_'${cid}'/"
    vobs_file=${dirpath}'/REEs_model_data/revision/rank/'${task[${tid}]}'/train/tokenVobs.txt'
    mkdir ${interestingness_model_path}
    interestingness_model_txt_path=${dirpath}'/REEs_model_data/revision/rank/'${task[${tid}]}'/train/model_'${cid}'.txt'
    pretrained_matrix_file=${dirpath}'/REEs_model_data/revision/rank/'${task[${tid}]}'/train/tokenEmbedds.pkl'
    rule_interestingness_file=${dirpath}'/REEs_model_data/revision/results_rank_sub/'${task[${tid}]}'_rule_interestingness_scores_'${cid}'.py'

    python interestingnessFixedEmbedsWithObj_main.py -rules_file ${rules_file} -train_file ${train_file} -valid_file ${valid_file} -test_file ${test_file} -model_file  ${interestingness_model_path} -vobs_file ${vobs_file} -predicates_path ${predicates_path} -model_txt_file ${interestingness_model_txt_path} -pretrained_matrix_file ${pretrained_matrix_file}
    # compute interestingness score
    python interestingnessFixedEmbedsWithObjScore_main.py -rules_file ${rules_file} -model_file  ${interestingness_model_path} -optionIfObj False -vobs_file ${vobs_file} -predicates_path ${predicates_path} -pretrained_matrix_file ${pretrained_matrix_file} -rule_interestingness_file ${rule_interestingness_file}
done


