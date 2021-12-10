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

  hdfs dfs -mkdir /tmp/zhangjun/RL_model/

  hdfs dfs -rm -r /tmp/zhangjun/RL_model/${data_name}/

  hdfs dfs -mkdir /tmp/zhangjun/RL_model/${data_name}/

  hdfs dfs -put ${model_path}${data_name}/checkpoint /tmp/zhangjun/RL_model/${data_name}/

  hdfs dfs -put ${model_path}${data_name}/model.ckpt.index /tmp/zhangjun/RL_model/${data_name}/

  hdfs dfs -put ${model_path}${data_name}/model.ckpt.meta /tmp/zhangjun/RL_model/${data_name}/

  hdfs dfs -put ${model_path}${data_name}/model.ckpt.data-00000-of-00001 /tmp/zhangjun/RL_model/${data_name}/

fi