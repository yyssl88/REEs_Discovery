
#!/bin/bash

task=(
'airports'
'hospital'
'ncvoter'
'inspection'
'aminer'
)

tid=$1
#train_ratio=$2
cuda=$2

cd ../../PredicateInterestingnessFilter

echo -e "---------------------------------------------- TRAIN transformer interestingness model -------------------------------------------------------------------"
# 4. Transformer
for train_ratio in 20 40 60 80; do
    result_file=${dirpath}'/REEs_model_data/revision/results/result_TRAIN'${train_ratio}'_transformer_'${task[${tid}]}'.txt'
    #> ${result_file}
    ipython ${dirpath}/REEs_model/baselines/transformer_interestingness_vary_train_size.ipynb ${task[${tid}]} ${result_file} ${cuda} 0 ${train_ratio}
done

