#!/bin/bash

original_data_dir=$1
train_original_file=$2
infer_original_file=$3
sample_ratio=$4
infer_embed_file=$5
model_file=$6
clustering_ids_file=$7
cluster_num=$8
embedding_ids_file=$9
sample_data_dir=$10

# generate training and inference data
python proc.py ${original_data_dir} ${train_original_file} ${infer_original_file} ${sample_ratio}

# train

# cluster (k-means)
python ../text-autoencoders/test_cluster.py --clustering --C ${cluster_num} --clustering_file ${clustering_ids_file} --checkpoint ${model_file}

# generate emebdding
python ../text-autoencoders/text_cluster.py --embedding --embedding_file ${infer_embed_file} --embedding_ids ${embedding_ids_file} --checkpoint ${model_file}

# finally sample
python sample.py ${clustering_ids_file} ${infer_embed_file} ${sample_ratio} ${original_data_dir} ${sample_data_dir}


