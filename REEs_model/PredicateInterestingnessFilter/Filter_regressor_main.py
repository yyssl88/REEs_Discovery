#from predicateAssoc import PAssoc
from predicateAgentInterestingness import *
import sys
sys.path.append('../../REEs_model')
from REEs_model.Filter_mlp import FilterRegressor
import argparse
import logging
import time
import numpy as np
import os
import json

import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()

MAX_LHS_PREDICATES = 7


def imbalance(trainDataMore, trainDataSmall, trainLabelsMore, trainLabelsSmall):
    sc = len(trainDataMore)
    sc_ = len(trainDataSmall)
    ratio = sc // sc_
    trainDataSmall = np.concatenate([trainDataSmall] * ratio, 0)
    trainLabelsSmall = np.concatenate([trainLabelsSmall] * ratio, 0)
    trainData = np.concatenate([trainDataMore, trainDataSmall], 0)
    trainLabels = np.concatenate([trainLabelsMore, trainLabelsSmall], 0)
    return trainData, trainLabels

def main():
    start = time.time()

    parser = argparse.ArgumentParser(description="Learn Predicate Association")

    parser.add_argument('-learning_rate', '--learning_rate', type=float, default=0.0001)
    parser.add_argument('-batch_size', '--batch_size', type=int, default=64)
    parser.add_argument('-hidden_dim', '--hidden_dim', type=int, default=200)

    parser.add_argument('-model_path', '--model_path', type=str, default="FilterModel/filtermodel.txt")
    parser.add_argument('-filter_dir', '--filter_dir', type=str, default="FilterData/")
    parser.add_argument('-epoch', '--epoch', type=int, default=300)


    args = parser.parse_args()
    arg_dict = args.__dict__
    for k, v in sorted(arg_dict.items()):
        logging.info('[Argument] %s=%r' % (k, v))
        print("k:", k, ", v:", v)


    # load json statistic of predicates
    json_file = os.path.join(arg_dict['filter_dir'], 'statistic.json')
    with open(json_file, 'r') as f:
        statistic = json.load(f)
    p_num = statistic['p_num']

    ### train the FilterClassifier
    filterRegressor = FilterRegressor(p_num * 2, arg_dict['learning_rate'], arg_dict['hidden_dim'],
                                        arg_dict['epoch'], arg_dict['batch_size'])

    # save training data
    trainData, validData, trainLabels, validLabels = filterRegressor.loadFilterData(arg_dict['filter_dir'])

    trainLabels.shape = -1, 1
    validLabels.shape = -1, 1

    start = time.time()

    filterRegressor.train(trainData, trainLabels, validData, validLabels)
    end = time.time()
    print("TEST!!!!!! finish training Filter Classifier, using time: ", str(end - start))
    # save the model for JAVA
    filterRegressor.saveModelToText(arg_dict['model_path'])

if __name__ == "__main__":
    main()

