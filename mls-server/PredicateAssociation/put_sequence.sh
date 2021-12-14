#!/bin/bash

model_path=$1

model_path="/root/PredicateAssociation/model/" # Fix path. Removing this is ok.

data_name=$2

N=$3

if [ "$model_path" = "" ]
then
  echo -e "empty model_path"
elif [ "$model_path" = "/" ]
then
   echo -e "wrong model_path"
elif [ "$data_name" = "" ]
then
  echo -e "empty data_name"
elif [ "$N" = "" ]
then
  echo -e "empty N"
else
  echo -e "model_path: "${model_path}
  echo -e "data_name: "${data_name}
  echo -e "N: "${N}

  sequence_name="sequence_"${data_name}"_N"${N}".txt"

  echo -e "sequence_name: "${sequence_name}

  hdfs dfs -mkdir /tmp/zhangjun/RL_model/

  hdfs dfs -rm /tmp/zhangjun/RL_model/${sequence_name}

  hdfs dfs -put ${model_path}${sequence_name} /tmp/zhangjun/RL_model/

fi