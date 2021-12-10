#!/bin/bash

model_path=$1

model_path="/root/PredicateAssociation/model/" # Fix path. Removing this is ok.

data_name=$2

if [ "$model_path" = "" ]
then
  echo -e "empty model_path"
elif [ "$model_path" = "/" ]
then
   echo -e "wrong model_path"
elif [ "$data_name" = "" ]
then
  echo -e "empty data_name"
else
  echo -e ${model_path}
  echo -e ${data_name}

  mkdir -p ${model_path}
  mkdir -p ${model_path}${data_name}/

  rm ${model_path}${data_name}/*
  echo -e "rm ${model_path}${data_name}/*"

  hdfs dfs -get /tmp/zhangjun/RL_model/${data_name}/checkpoint ${model_path}${data_name}/

  hdfs dfs -get /tmp/zhangjun/RL_model/${data_name}/model.ckpt.index ${model_path}${data_name}/

  hdfs dfs -get /tmp/zhangjun/RL_model/${data_name}/model.ckpt.meta ${model_path}${data_name}/

  hdfs dfs -get /tmp/zhangjun/RL_model/${data_name}/model.ckpt.data-00000-of-00001 ${model_path}${data_name}/

fi
