
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

echo -e "---------------------------------------------- TRAIN the BERT MLM interestingness model -------------------------------------------------------------------"
# 3. Bert model MLM
result_file=${dirpath}'/REEs_model_data/revision/results_ts/result_bertMLM_TS'${train_ratio}'_'${task[${tid}]}'.txt'
> ${result_file}
# 3.1 MLM
ipython ${dirpath}/REEs_model/baselines/bert_mlm.ipynb ${task[${tid}]}

# 3.2 fine-tune
ipython ${dirpath}/REEs_model/baselines/bert_interestingness_vary_train_size.ipynb ${task[${tid}]} True ${result_file} ${train_ratio}
