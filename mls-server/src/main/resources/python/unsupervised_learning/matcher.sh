#!/bin/bash

export CUDA_VISIBLE_DEVICES=2
#python matcher.py './result/candidate_pairs.txt' './models/' './result/result.txt' train
#python matcher.py './result/candidate_pairs.txt' './models/' './result/result.txt' test
python matcher.py './result/test_data.txt' './models/' './result/result.txt' test
