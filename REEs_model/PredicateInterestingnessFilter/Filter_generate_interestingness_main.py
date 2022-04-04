#from predicateAssoc import PAssoc
from predicateAgentInterestingness import *
import sys
sys.path.append('../../REEs_model')
from DQNInterest_mlp import DeepQNetwork
from REEs_model.Filter_mlp import FilterRegressor
from interestingnessFixedEmbedsWithObj import *
from REEs_model.parameters import *
import argparse
import logging
import time
import json
import os

import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()


def main():
    start = time.time()

    parser = argparse.ArgumentParser(description="Learn Predicate Association")

    parser.add_argument('-learning_rate', '--learning_rate', type=float, default=0.01)
    parser.add_argument('-reward_decay', '--reward_decay', type=float, default=0.9)
    parser.add_argument('-e_greedy', '--e_greedy', type=float, default=0.8)
    parser.add_argument('-replace_target_iter', '--replace_target_iter', type=int, default=50)
    parser.add_argument('-memory_size', '--memory_size', type=int, default=50)
    parser.add_argument('-batch_size', '--batch_size', type=int, default=32)

    parser.add_argument('-interestingness_model_path', '--interestingness_model_path', type=str, default="interestingness_model/model.ckpt")
    parser.add_argument('-model_path', '--model_path', type=str, default="FilterModel/filtermodel.txt")
    parser.add_argument('-dqn_model_path', '--dqn_model_path', type=str, default="model/model.ckpt")
    parser.add_argument('-filter_dir', '--filter_dir', type=str, default="FilterData/")
    parser.add_argument('-epochs', '--epochs', type=int, default=200)
    parser.add_argument('-optionIfObj', '--optionIfObj', type=bool, default=True)

    parser.add_argument('-predicates_path', '--predicates_path', type=str, default="allPredicates/predicates_airport.txt")

    parser.add_argument('-hidden_dim', '--hidden_dim', type=int, default=200)
    parser.add_argument('-combine_num', '--combine_num', type=int, default=500)

    parser.add_argument('-token_embed_dim', '--token_embed_dim', type=int, default=100)
    parser.add_argument('-hidden_size', '--hidden_size', type=int, default=100)
    parser.add_argument('-rees_embed_dim', '--rees_embed_dim', type=int, default=50)
    parser.add_argument('-vobs_file', '--vobs_file', type=str, default="")

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
    '''
    rhssString = str(validator.getApplicationRHSs())
    rhssStrArr = [e.strip() for e in rhssString.split(";")]
    if "" in rhssStrArr:
        rhssStrArr.remove("")
    '''

    vob_size = 0
    tokenVobs = defaultdict(int)
    for line in open(arg_dict['vobs_file']):
        token, count = ' '.join(line.split()[:-1]), int(line.split()[-1])
        tokenVobs[token] = count

    vob_size = len(tokenVobs.keys())

    # run
    p_num = len(predicateStrArr)
    # make statistic
    statistic = dict()
    statistic['p_num'] = len(predicateStrArr)
    statistic['all_predicates'] = predicateStrArr
    statistic['rhsPIDs'] = []

    # save statistic
    json_file = os.path.join(arg_dict['filter_dir'], 'statistic.json')
    with open(json_file, 'w') as f:
        json.dump(statistic, f)
        print('Load json successfully ...')


    # load the rule interestingness model
    InterestingnessModel = InterestingnessEmbedsWithObj(vob_size, arg_dict['token_embed_dim'], arg_dict['hidden_size'],
                                  arg_dict['rees_embed_dim'], MAX_LHS_PREDICATES,
                                  MAX_RHS_PREDICATES, 3, arg_dict['optionIfObj'], arg_dict['learning_rate'], arg_dict['epochs'], arg_dict['batch_size'])
    InterestingnessModel.loadModel(arg_dict['interestingness_model_path'])

    print("The interestingness model is ", InterestingnessModel.vob_size)

    pAgent = PredicateAgentInterestingness(p_num, predicateStrArr)
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
    filterRegressor = FilterRegressor(p_num * 2, arg_dict['learning_rate'], arg_dict['hidden_dim'],
                                        arg_dict['epochs'], arg_dict['batch_size'])

    start = time.time()
    trainData, validData, trainLabels, validLabels = filterRegressor.generateAllTrainingInstances(pAgent, model, InterestingnessModel, MAX_LHS_PREDICATES, tokenVobs, arg_dict['combine_num'])
    end = time.time()
    print("TEST!!!!!! finish generating Filter Data, using time: ", str(end - start))
    # save training data
    filterRegressor.saveFilterData(arg_dict['filter_dir'], trainData, validData, trainLabels, validLabels)

if __name__ == "__main__":
    main()

