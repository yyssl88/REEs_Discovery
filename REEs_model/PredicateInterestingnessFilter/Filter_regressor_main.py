#from predicateAssoc import PAssoc
from predicateAgent import *
import sys
sys.path.append('../../REEs_model')
from DQN_mlp import DeepQNetwork
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

def main():
    start = time.time()

    parser = argparse.ArgumentParser(description="Learn Predicate Association")

    parser.add_argument('-learning_rate', '--learning_rate', type=float, default=0.01)
    parser.add_argument('-batch_size', '--batch_size', type=int, default=32)
    parser.add_argument('-hidden_dim', '--hidden_dim', type=int, default=100)

    parser.add_argument('-model_path', '--model_path', type=str, default="FilterModel/filtermodel.txt")
    parser.add_argument('-filter_dir', '--filter_dir', type=str, default="FilterData/")
    parser.add_argument('-epoch', '--epoch', type=int, default=200)
    parser.add_argument('-predicates_path', '--predicates_path', type=str, default="allPredicates/predicates_airport.txt")


    args = parser.parse_args()
    arg_dict = args.__dict__
    for k, v in sorted(arg_dict.items()):
        logging.info('[Argument] %s=%r' % (k, v))
        print("k:", k, ", v:", v)

    # initialize predicates
    predicateStrArr = []
    for line in open(arg_dict['predicates_path']):
        predicateStrArr.append(line.strip())

    if "" in predicateStrArr:
        predicateStrArr.remove("")
    print("All Predicates : ", predicateStrArr)

    p_num = len(predicateStrArr)

    ### train the FilterClassifier
    filterRegressor = FilterRegressor(p_num * 2, arg_dict['learning_rate'], arg_dict['hidden_dim'],
                                        arg_dict['epoch'], arg_dict['batch_size'])

    # save training data
    trainData, validData, trainLabels, validLabels = filterRegressor.loadFilterData(arg_dict['filter_dir'])

    filterRegressor.train(trainData, trainLabels, validData, validLabels)
    # save the model for JAVA
    filterRegressor.saveModelToText(arg_dict['model_path'])

if __name__ == "__main__":
    main()

