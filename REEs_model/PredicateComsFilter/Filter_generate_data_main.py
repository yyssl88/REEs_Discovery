#from predicateAssoc import PAssoc
from predicateAgent import *
import sys
sys.path.append('../../REEs_model')
from DQN_mlp import DeepQNetwork
from REEs_model.Filter_mlp import FilterClassifier
from REEs_model.utils import *
import argparse
import logging
import time
import numpy as np
import jpype
import json
import os

import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()

MAX_LHS_PREDICATES = 6

def main():
    start = time.time()

    parser = argparse.ArgumentParser(description="Learn Predicate Association")
    parser.add_argument('-supp_thr', '--supp_thr', type=float, default=0.00001)

    parser.add_argument('-learning_rate', '--learning_rate', type=float, default=0.01)
    parser.add_argument('-reward_decay', '--reward_decay', type=float, default=0.9)
    parser.add_argument('-e_greedy', '--e_greedy', type=float, default=0.8)
    parser.add_argument('-replace_target_iter', '--replace_target_iter', type=int, default=50)
    parser.add_argument('-memory_size', '--memory_size', type=int, default=50)
    parser.add_argument('-batch_size', '--batch_size', type=int, default=32)

    parser.add_argument('-model_path', '--model_path', type=str, default="FilterModel/filtermodel.txt")
    parser.add_argument('-dqn_model_path', '--dqn_model_path', type=str, default="model/model.ckpt")
    parser.add_argument('-filter_dir', '--filter_dir', type=str, default="FilterData/")
    parser.add_argument('-epoch', '--epoch', type=int, default=200)

    parser.add_argument('-predicates_path', '--predicates_path', type=str, default="allPredicates/predicates_airport.txt")

    parser.add_argument('-if_interactive', '--if_interactive', type=int, default=1)
    parser.add_argument('-directory_path', '--directory_path', type=str, default="")
    parser.add_argument('-constant_file', '--constant_file', type=str, default="")
    parser.add_argument('-chunkLength', '--chunkLength', type=int, default=500000)
    parser.add_argument('-maxTupleNum', '--maxTupleNum', type=int, default=2)
    parser.add_argument('-conf_thr', '--conf_thr', type=int, default=0.8)

    parser.add_argument('-classpath', '--classpath', type=str, default="")
    parser.add_argument('-hidden_dim', '--hidden_dim', type=int, default=200)
    parser.add_argument('-combine_num', '--combine_num', type=int, default=500)

    args = parser.parse_args()
    arg_dict = args.__dict__
    for k, v in sorted(arg_dict.items()):
        logging.info('[Argument] %s=%r' % (k, v))
        print("k:", k, ", v:", v)

    # read the original data
    datasets = readDataFile(arg_dict['directory_path'])
    data_num = 0
    for dataset in datasets:
        data_num += len(dataset)
    support = data_num * data_num * arg_dict['supp_thr']

    # initialize predicates
    predicateStrArr = []
    for line in open(arg_dict['predicates_path'], 'r'):
        predicateStrArr.append(line.strip())
    if "" in predicateStrArr:
        predicateStrArr.remove("")

    print("The number of predicates is {} with file {}".format(len(predicateStrArr), arg_dict['predicates_path']))

    # start Java
    classpath = arg_dict["classpath"].replace(":", ";")
    print("classpath: ", classpath)
    jpype.startJVM(jpype.getDefaultJVMPath(), "-ea", "-Djava.classpath={}".format(classpath))
    args_ = ["directory_path={}".format(arg_dict["directory_path"]), "constant_file={}".format(arg_dict["constant_file"]),
             "chunkLength={}".format(arg_dict["chunkLength"]), "maxTupleNum={}".format(arg_dict["maxTupleNum"]), "filterEnumNumber=5"]
    java_validator = jpype.JClass('sics.seiois.mlsserver.biz.der.mining.validation.CalculateRuleSuppConf')
    validator = java_validator()
    validator.preparePredicates(args_)
    # load predicatesMap of DQN
    validator.readAllPredicates(arg_dict['predicates_path'])

    ''' 
    # initialize predicates
    #predicatesString = str(validator.getAllPredicates())
    #predicateStrArr = [e.strip() for e in predicatesString.split(";")];
    #if "" in predicateStrArr:
        #predicateStrArr.remove("")
    #print("All Predicates : ", predicateStrArr)
    '''
    # initialize RHSs
    rhssString = str(validator.getApplicationRHSs())
    rhssStrArr = [e.strip() for e in rhssString.split(";")]
    if "" in rhssStrArr:
        rhssStrArr.remove("")

    # run
    p_num = len(predicateStrArr)
    '''
    nonConstantPredicateIDs = None
    if arg_dict["if_interactive"] == 1:
        p_num = validator.getAllPredicatesNum()
        nonConstantPredicateIDs = validator.getNonConstantPredicateIDs()
    print("p_num: ", p_num)
    print("nonConstantPredicateIDs: ", nonConstantPredicateIDs)
    '''

    # make statistic = 'p_num': XX, 'all_predicates': XX, 'rhsPIDs': XX
    statistic = dict()
    statistic['p_num'] = len(predicateStrArr)
    statistic['all_predicates'] = predicateStrArr
    statistic['rhsPIDs'] = rhssStrArr

    # save statistic
    json_file = os.path.join(arg_dict['filter_dir'], 'statistic.json')
    with open(json_file, 'w') as f:
        json.dump(statistic, f)    
        print('Load json successfully ...')

    #pAssoc = PAssoc(arg_dict["if_interactive"], p_num, arg_dict["supp_thr"], arg_dict["conf_thr"], arg_dict["data_dir"])

    pAgent = PredicateAgent(p_num, support, arg_dict['conf_thr'], predicateStrArr)
    model = DeepQNetwork(p_num, p_num * 2,
                         learning_rate=arg_dict["learning_rate"],
                         reward_decay=arg_dict["reward_decay"],
                         e_greedy=arg_dict["e_greedy"],
                         replace_target_iter=arg_dict["replace_target_iter"],
                         memory_size=arg_dict["memory_size"],
                         batch_size=arg_dict["batch_size"],
                         # output_graph=True
                         )
    # load the model
    model.loadModel(arg_dict['dqn_model_path'])
    end = time.time()
    print("finish training, using time: ", str(end - start), arg_dict["model_path"])


    ### train the FilterClassifier
    filterClassifier = FilterClassifier(p_num * 2, arg_dict['learning_rate'], arg_dict['hidden_dim'],
                                        arg_dict['epoch'], arg_dict['batch_size'])

    start = time.time()
    trainData, validData, trainLabels, validLabels = filterClassifier.generateAllTrainingInstances(pAgent,
                                                                                                   model,
                                                                                                   validator,
                                                                                                   MAX_LHS_PREDICATES,
                                                                                                   arg_dict['combine_num'])
    end = time.time()
    print("TEST!!!!!! finish generating Filter Data, using time: ", str(end - start))
    # save training data
    filterClassifier.saveFilterData(arg_dict['filter_dir'], trainData, validData, trainLabels, validLabels)

if __name__ == "__main__":
    main()

