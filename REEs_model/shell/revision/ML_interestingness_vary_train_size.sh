
#!/bin/bash

task=(
'airports'
'hospital'
'ncvoter'
'inspection'
'aminer'
)

tid=$1

cd ../../PredicateInterestingnessFilter

cid=0

train_ratio=20
result_file=${dirpath}'/REEs_model_data/revision/results/result_TRAIN'${train_ratio}'_objML_'${task[${tid}]}'.txt'
#> ${result_file}
ipython ${dirpath}/REEs_model/baselines/ML_interestingness_vary_train_size.ipynb ${task[${tid}]} ${result_file} ${cid} ${train_ratio}


train_ratio=40
result_file=${dirpath}'/REEs_model_data/revision/results/result_TRAIN'${train_ratio}'_objML_'${task[${tid}]}'.txt'
#> ${result_file}
ipython ${dirpath}/REEs_model/baselines/ML_interestingness_vary_train_size.ipynb ${task[${tid}]} ${result_file} ${cid} ${train_ratio}

train_ratio=60
result_file=${dirpath}'/REEs_model_data/revision/results/result_TRAIN'${train_ratio}'_objML_'${task[${tid}]}'.txt'
#> ${result_file}
ipython ${dirpath}/REEs_model/baselines/ML_interestingness_vary_train_size.ipynb ${task[${tid}]} ${result_file} ${cid} ${train_ratio}


train_ratio=80
result_file=${dirpath}'/REEs_model_data/revision/results/result_TRAIN'${train_ratio}'_objML_'${task[${tid}]}'.txt'
#> ${result_file}
ipython ${dirpath}/REEs_model/baselines/ML_interestingness_vary_train_size.ipynb ${task[${tid}]} ${result_file} ${cid} ${train_ratio}






