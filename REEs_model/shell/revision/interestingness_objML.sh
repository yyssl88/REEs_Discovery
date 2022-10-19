#!/bin/bash

task=(
'airports'
'hospital'
'ncvoter'
'inspection'
'aminer'
)

tid=0

cd ../../PredicateInterestingnessFilter

echo -e "---------------------------------------------- TRAIN objML interestingness model -------------------------------------------------------------------"
# 5. objective ML
for((cid=0;cid<10;cid++));do
    result_file=${dirpath}'/REEs_model_data/revision/results/result_ROUND'${cid}'_objML_'${task[${tid}]}'.txt'
    #> ${result_file}
    ipython ${dirpath}/REEs_model/baselines/ML_interestingness.ipynb ${task[${tid}]} ${result_file} ${cid}
done

tid=1
echo -e "---------------------------------------------- TRAIN objML interestingness model -------------------------------------------------------------------"
# 5. objective ML
for((cid=0;cid<10;cid++));do
    result_file=${dirpath}'/REEs_model_data/revision/results/result_ROUND'${cid}'_objML_'${task[${tid}]}'.txt'
    #> ${result_file}
    ipython ${dirpath}/REEs_model/baselines/ML_interestingness.ipynb ${task[${tid}]} ${result_file} ${cid}
done

tid=2
echo -e "---------------------------------------------- TRAIN objML interestingness model -------------------------------------------------------------------"
# 5. objective ML
for((cid=0;cid<10;cid++));do
    result_file=${dirpath}'/REEs_model_data/revision/results/result_ROUND'${cid}'_objML_'${task[${tid}]}'.txt'
    #> ${result_file}
    ipython ${dirpath}/REEs_model/baselines/ML_interestingness.ipynb ${task[${tid}]} ${result_file} ${cid}
done


tid=4
echo -e "---------------------------------------------- TRAIN objML interestingness model -------------------------------------------------------------------"
# 5. objective ML
for((cid=0;cid<10;cid++));do
    result_file=${dirpath}'/REEs_model_data/revision/results/result_ROUND'${cid}'_objML_'${task[${tid}]}'.txt'
    #> ${result_file}
    ipython ${dirpath}/REEs_model/baselines/ML_interestingness.ipynb ${task[${tid}]} ${result_file} ${cid}
done


