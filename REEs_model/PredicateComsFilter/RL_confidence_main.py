#from predicateAssoc import PAssoc
from predicateAgent import *
from DQN_mlp import DeepQNetwork
import argparse
import logging
import time
import numpy as np
import jpype
import sys
sys.path.append('../../REEs_model')
from REEs_model.utils import *

import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()


MAX_LHS_PREDICATES = 6

def checkSameRelation(r_1, r_2):
    if "t0" in r_1 and "t0" in r_2:
        if r_1["t0"] == r_2["t0"]:
            return True
    if "t1" in r_1 and "t1" in r_2:
        if r_1["t1"] == r_2["t1"]:
            return True
    return False

def test(pComb, model, validator, predicatesArr):
    for ll in range(1, 6, 1):
        for z in range(10):
            #rhs_id = np.random.randint(0, pComb.predicates_num)
            rhs_id = pComb.randomRHSID()
            print("The RHS predicate is ", predicatesArr[rhs_id].print(), predicatesArr[rhs_id].constant, predicatesArr[rhs_id].isConstantPredicate())
            relationRHS = predicatesArr[rhs_id].getRelations()
            # first select one nonConstantPredicate
            unselected = []
            unselected_new = []
            for pid, p in enumerate(predicatesArr):
                # remove rhs
                relation_p = p.getRelations()
                if pid == rhs_id or not checkSameRelation(relationRHS, relation_p):
                    continue
                unselected_new.append(pid)
                if not p.isConstantPredicate():
                    unselected.append(pid)
            sc = np.random.choice(len(unselected), 1)[0]
            observation = np.zeros(pComb.predicates_num)
            observation[unselected[sc]] = 1.0
            if ll - 1 > 0:
                #sc = np.random.choice(pComb.predicates_num, ll - 1, replace=False)
                #observation[sc] = 1.0
                if len(unselected_new) < ll - 1:
                    continue
                sc = np.random.choice(len(unselected_new), ll - 1, replace=False)
                for e in sc:
                    observation[unselected_new[e]] = 1.0
            observation[rhs_id] = 0.0
            if np.sum(observation) == 0:
                continue
            rhs = np.zeros(pComb.predicates_num)
            rhs[rhs_id] = 1.0
            observation_lrhs = np.concatenate([observation, rhs])
            learned_rewards = model.evaluate(observation_lrhs)
            confidence = pComb.calculateConf(observation, rhs_id, validator)
            for r in learned_rewards:
                print("UB of confidence with length {}, current confidence {}, future reward {} and UB {} ".format(ll, confidence, r[1], confidence + r[1]))

def run(pComb, model, validator, epoch, model_path):
    step = 0
    for episode in range(epoch):
        # initial observation
        observation = pComb.reset()
        #rhs_id = np.random.randint(0, pComb.predicates_num)
        rhs_id = pComb.randomRHSID()
        #observation = pComb.initialAction(rhs_id)

        rhs = np.zeros(pComb.predicates_num)
        rhs[rhs_id] = 1.0
        observation = np.concatenate([observation, rhs])

        '''
        # calculate support and confidence
        confidence, done = pComb.stepBegin(rhs_id, validator)
        if done:
            continue
        current_conf = confidence
        '''
        current_conf = 0.0

        print("rhs_id: {}".format(rhs_id))
        print("observation: {}".format(observation))

        while True:
            # find action
            action = model.choose_action(observation, rhs_id, pComb.predicatesArr)
            if action == -1:
                break
            # take action and get next observation and reward
            observation_, reward, done, conf = pComb.step(action, rhs_id, validator, MAX_LHS_PREDICATES, current_conf)
            # store the current confidence
            current_conf = conf
            observation_ = np.concatenate([observation_, rhs])

            print("Epoch {} : {}, {}, {}, {}".format(episode, observation_, reward, done, action))

            model.store_transition(observation, action, reward, observation_)

            if (step > 30) and (step % 2 == 0):
                #for z in range(10):
                model.learn()

            # swap observation
            observation = observation_
            if len(observation[observation==1]) > MAX_LHS_PREDICATES:
                break
            if done:
                break
            step += 1

    # test
    #test(pComb, model, validator, pComb.getAllPredicates())

    # save RL model
    #saver = tf.train.Saver()
    #save_path = saver.save(model.sess, model_path)
    #print("Model saved in path: ", save_path)
    # save to txt file
    model.saveModelToText(model_path)

def main():
    #start = time.time()

    parser = argparse.ArgumentParser(description="Learn Predicate Association")
    parser.add_argument('-data_dir', '--data_dir', type=str, default="")
    parser.add_argument('-supp_thr', '--supp_thr', type=float, default=0.00001)

    parser.add_argument('-learning_rate', '--learning_rate', type=float, default=0.01)
    parser.add_argument('-reward_decay', '--reward_decay', type=float, default=0.9)
    parser.add_argument('-e_greedy', '--e_greedy', type=float, default=0.8)
    parser.add_argument('-replace_target_iter', '--replace_target_iter', type=int, default=50)
    parser.add_argument('-memory_size', '--memory_size', type=int, default=50)
    parser.add_argument('-batch_size', '--batch_size', type=int, default=32)

    parser.add_argument('-model_path', '--model_path', type=str, default="model/model.ckpt")
    parser.add_argument('-epoch', '--epoch', type=int, default=5000)

    parser.add_argument('-predicates_path', '--predicates_path', type=str, default="allPredicates/predicates_airport.txt")

    parser.add_argument('-if_interactive', '--if_interactive', type=int, default=1)
    parser.add_argument('-directory_path', '--directory_path', type=str, default="")
    parser.add_argument('-constant_file', '--constant_file', type=str, default="")
    parser.add_argument('-chunkLength', '--chunkLength', type=int, default=200000)
    parser.add_argument('-maxTupleNum', '--maxTupleNum', type=int, default=2)
    parser.add_argument('-conf_thr', '--conf_thr', type=int, default=0.8)

    parser.add_argument('-classpath', '--classpath', type=str, default="")

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
    for line in open(arg_dict['predicates_path']):
        predicateStrArr.append(line.strip())

    if "" in predicateStrArr:
        predicateStrArr.remove("")

    # start Java
    classpath = arg_dict["classpath"].replace(":", ";")
    print("classpath: ", classpath)
    jpype.startJVM(jpype.getDefaultJVMPath(), "-ea", "-Djava.classpath={}".format(classpath))
    args_ = ["directory_path={}".format(arg_dict["directory_path"]), "constant_file={}".format(arg_dict["constant_file"]),
             "chunkLength={}".format(arg_dict["chunkLength"]), "maxTupleNum={}".format(arg_dict["maxTupleNum"]), "filterEnumNumber=0"]
    java_validator = jpype.JClass('sics.seiois.mlsserver.biz.der.mining.validation.CalculateRuleSuppConf')
    validator = java_validator()
    validator.preparePredicates(args_)
    # load predicatesMap of DQN
    validator.readAllPredicates(arg_dict['predicates_path'])

    '''
    # initialize predicates
    predicateStrArr = []
    for line in open(arg_dict['predicates_path']):
        predicateStrArr.append(line.strip())

    if "" in predicateStrArr:
        predicateStrArr.remove("")
    '''
    print("All Predicates : ", predicateStrArr)

    '''
    # initialize predicates
    predicatesString = str(validator.getAllPredicates())
    predicateStrArr = [e.strip() for e in predicatesString.split(";")];
    if "" in predicateStrArr:
        predicateStrArr.remove("")
    print("All Predicates : ", predicateStrArr)
    # initialize RHSs
    rhssString = str(validator.getApplicationRHSs())
    rhssStrArr = [e.strip() for e in rhssString.split(";")]
    if "" in rhssStrArr:
        rhssStrArr.remove("")
    '''

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


    pAgent = PredicateAgent(p_num, support, arg_dict['conf_thr'], predicateStrArr)
    # print all predicates
    pAgent.printAllPredicates()

    model = DeepQNetwork(p_num, p_num * 2,
                         learning_rate=arg_dict["learning_rate"],
                         reward_decay=arg_dict["reward_decay"],
                         e_greedy=arg_dict["e_greedy"],
                         replace_target_iter=arg_dict["replace_target_iter"],
                         memory_size=arg_dict["memory_size"],
                         batch_size=arg_dict["batch_size"],
                         # output_graph=True
                         )

    #run(pAssoc, model, nonConstantPredicateIDs, validator, arg_dict["epoch"], arg_dict["model_path"])
    start = time.time()
    run(pAgent, model, validator, arg_dict["epoch"], arg_dict["model_path"])

    end = time.time()
    print("TEST!!!!!! finish training DQN, using time: ", str(end - start))
    #sys.stderr.write("finish training, using time: ", str(end - start), arg_dict["model_path"])

    # save the DQN model
    model.saveModel(arg_dict["model_path"])

if __name__ == "__main__":
    main()

