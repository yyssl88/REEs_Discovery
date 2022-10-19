#!/bin/bash

data="airports"

#python split.py ${data}"/labeled_data.txt" 0.6


mkdir ${data}'/train/'
cp ${data}'/'${data}'_rules.txt' ${data}'/train/rules.txt'

cp ${data}'/labeled_data.txt_train.csv' ${data}'/train/train.csv'
cp ${data}'/labeled_data.txt_valid.csv' ${data}'/train/valid.csv'
cp ${data}'/labeled_data.txt_test.csv' ${data}'/train/test.csv'

