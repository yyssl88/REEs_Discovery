import sys
sys.path.append('../../REEs_model')
from predicateAgentInterestingness import *
from DQNInterest_mlp import DeepQNetwork
from REEs_model.Filter_mlp import FilterRegressor
from REEs_model.parameters import *
from interestingnessFixedEmbedsWithObj import *
import argparse
import logging
import time
import numpy as np

import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()


def checkSameRelation(r_1, r_2):
    if "t0" in r_1 and "t0" in r_2:
        if r_1["t0"] == r_2["t0"]:
            return True
    if "t1" in r_1 and "t1" in r_2:
        if r_1["t1"] == r_2["t1"]:
            return True
    return False

def test(pComb, model, InterestingnessModel, predicatesArr, tokenVobs):
    for ll in range(1, 6, 1):
        for z in range(10):
            #rhs_id = np.random.randint(0, pComb.predicates_num)
            rhs_id = pComb.randomRHSID()
            # first select one nonConstantPredicate
            relationRHS = predicatesArr[rhs_id].getRelations()
            unselected = []
            unselected_new = []
            for pid, p in enumerate(predicatesArr):
                # remove rhs
                relation_p = p.getRelations()
                if pid == rhs_id or not checkSameRelation(relationRHS, relation_p):
                    continue
                unselected_new.append(pid)
                if p.isConstantPredicate():
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
            selectedPredicateIDs = np.nonzero(observation)[0]
            score = pComb.calculateInterestingness(selectedPredicateIDs, rhs_id, InterestingnessModel, tokenVobs)
            for r in learned_rewards:
                print("UB of interestingness score with length {}, current interestingness {}, future reward {} and UB {} ".format(ll, score, r[1], score + r[1]))

def run(pComb, dqn, InterestingnessModel, epoch, model_path, tokenVobs):
    step = 0
    for episode in range(epoch):
        # initial observation
        observation = pComb.reset()
        # rhs_id = np.random.randint(0, pComb.predicates_num)
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
        current_interestingness = 0.0

        print("rhs_id: {}".format(rhs_id))
        print("observation: {}".format(observation))

        while True:
            # find action
            action = dqn.choose_action(observation, rhs_id, pComb.predicatesArr)
            if action == -1:
                break
            # take action and get next observation and reward
            observation_, reward, done, interestingnessScore = pComb.step(action, rhs_id, InterestingnessModel, MAX_LHS_PREDICATES, current_interestingness, tokenVobs)
            # store the current confidence
            current_interestingness = interestingnessScore
            observation_ = np.concatenate([observation_, rhs])

            print("Epoch {} : {}, {}, {}, {}".format(episode, observation_, reward, done, action))

            dqn.store_transition(observation, action, reward, observation_)

            if (step > 30) and (step % 2 == 0):
                #for z in range(10):
                dqn.learn()

            # swap observation
            observation = observation_
            if len(observation[observation==1]) > MAX_LHS_PREDICATES:
                break
            if done:
                break
            step += 1

    test(pComb, dqn, InterestingnessModel, pComb.getAllPredicates(), tokenVobs)

    # save RL model
    #saver = tf.train.Saver()
    #save_path = saver.save(model.sess, model_path)
    #print("Model saved in path: ", save_path)
    # save to txt file
    # dqn.saveModelToText(model_path)

def main():
    start = time.time()

    parser = argparse.ArgumentParser(description="Learn Predicate Association")
    parser.add_argument('-data_dir', '--data_dir', type=str, default="")
    parser.add_argument('-supp_thr', '--supp_thr', type=float, default=0.2)

    parser.add_argument('-learning_rate', '--learning_rate', type=float, default=0.01)
    parser.add_argument('-reward_decay', '--reward_decay', type=float, default=0.9)
    parser.add_argument('-e_greedy', '--e_greedy', type=float, default=0.8)
    parser.add_argument('-replace_target_iter', '--replace_target_iter', type=int, default=50)
    parser.add_argument('-memory_size', '--memory_size', type=int, default=50)
    parser.add_argument('-batch_size', '--batch_size', type=int, default=32)

    parser.add_argument('-interestingness_model_path', '--interestingness_model_path', type=str, default="interestingness_model/model.ckpt")
    parser.add_argument('-model_path', '--model_path', type=str, default="model/model.ckpt")
    parser.add_argument('-epochs', '--epochs', type=int, default=500)

    parser.add_argument('-predicates_path', '--predicates_path', type=str, default="allPredicates/predicates_airport.txt")

    parser.add_argument('-classpath', '--classpath', type=str, default="")
    parser.add_argument('-hidden_dim', '--hidden_dim', type=int, default=200)
    parser.add_argument('-combine_num', '--combine_num', type=int, default=500)
    parser.add_argument('-optionIfObj', '--optionIfObj', type=bool, default=True)


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


    vob_size = 0
    tokenVobs = defaultdict(int)
    for line in open(arg_dict['vobs_file']):
        token, count = ' '.join(line.split()[:-1]), int(line.split()[-1])
        tokenVobs[token] = count

    print('Token Vobs are : ', tokenVobs)

    vob_size = len(tokenVobs.keys())
    # interestingness model
    InterestingnessModel = InterestingnessEmbedsWithObj(vob_size, arg_dict['token_embed_dim'], arg_dict['hidden_size'],
                                  arg_dict['rees_embed_dim'], MAX_LHS_PREDICATES,
                                  MAX_RHS_PREDICATES, 3, arg_dict['optionIfObj'], arg_dict['learning_rate'], arg_dict['epochs'], arg_dict['batch_size'])
    InterestingnessModel.loadModel(arg_dict['interestingness_model_path'])

    # run
    p_num = len(predicateStrArr)

    pAgent = PredicateAgentInterestingness(p_num, predicateStrArr)
    dqnInterestingness = DeepQNetwork(p_num, p_num * 2,
                         learning_rate=arg_dict["learning_rate"],
                         reward_decay=arg_dict["reward_decay"],
                         e_greedy=arg_dict["e_greedy"],
                         replace_target_iter=arg_dict["replace_target_iter"],
                         memory_size=arg_dict["memory_size"],
                         batch_size=arg_dict["batch_size"],
                         # output_graph=True
                         )

    start = time.time()
    run(pAgent, dqnInterestingness, InterestingnessModel, arg_dict["epochs"], arg_dict["model_path"], tokenVobs)

    end = time.time()
    print("TEST!!!!!! finish training DQN, using time: ", str(end - start))

    # save the DQN model
    dqnInterestingness.saveModel(arg_dict['model_path'])

if __name__ == "__main__":
    main()

