# import sys
# print(sys.version)
# print("In run.py:", sys.path, "\n")

from model import DeepQNetwork
import argparse
import logging
import numpy as np

import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()

# input: sequence of (state, action, reward), i.e., "Psel, p, reward; Psel, p, reward; ...",
#        in form of "id1 id2 id3,p1,reward1;id1 id2 id4,p2,reward2;...", where reward = support - threshold.
# state: Psel - pid1  pid2  ...
# action: pid
# pnum: whole predicate num
def run(model, pnum, sequence, RL_model_path):
    triples = sequence.split(";")
    for triple in triples:
        seq = triple.split(",")

        stateId = seq[0].split(" ")
        action = int(seq[1])
        reward = int(seq[2])

        observation = np.zeros(pnum, dtype=int)
        observation_ = np.zeros(pnum, dtype=int)
        for idx in stateId:
            observation[int(idx)] = 1
            observation_[int(idx)] = 1

        observation_[action] = 1

        print("{}, {}, {}, {}".format(observation, action, reward, observation_))

        model.store_transition(observation, action, reward, observation_)

    # train RL model
    model.learn()

    # save RL model
    saver = tf.train.Saver()
    save_path = saver.save(model.sess, RL_model_path)
    print("Model saved in path: %s" % save_path)


def main():
    parser = argparse.ArgumentParser(description="Learn Predicate Association")
    parser.add_argument('-init_train', '--init_train', type=int, default=1)
    parser.add_argument('-predicate_num', '--predicate_num', type=int, default=0)
    parser.add_argument('-sequence', '--sequence', type=str, default="") # "Psel, p, reward; Psel, p, reward; ..."
    parser.add_argument('-legal_nextP_indices', '--legal_nextP_indices', type=str, default="")
    parser.add_argument('-learning_rate', '--learning_rate', type=float, default=0.01)
    parser.add_argument('-reward_decay', '--reward_decay', type=float, default=0.9)
    parser.add_argument('-e_greedy', '--e_greedy', type=float, default=0.9)
    parser.add_argument('-replace_target_iter', '--replace_target_iter', type=int, default=200)
    parser.add_argument('-memory_size', '--memory_size', type=int, default=2000)
    parser.add_argument('-batch_size', '--batch_size', type=int, default=32)
    parser.add_argument('-model_path', '--model_path', type=str, default="")

    args = parser.parse_args()
    arg_dict = args.__dict__
    for k, v in sorted(arg_dict.items()):
        logging.info('[Argument] %s=%r' % (k, v))
        print("k:", k, ", v:", v)

    # run
    model = DeepQNetwork(arg_dict["predicate_num"], arg_dict["predicate_num"],
                      learning_rate=arg_dict["learning_rate"],
                      reward_decay=arg_dict["reward_decay"],
                      e_greedy=arg_dict["e_greedy"],
                      replace_target_iter=arg_dict["replace_target_iter"],
                      memory_size=arg_dict["memory_size"],
                      batch_size=arg_dict["batch_size"],
                      # output_graph=True
                      )

    RL_model_path = arg_dict["model_path"]

    if arg_dict["init_train"] == 0:
        saver = tf.train.Saver()
        saver.restore(model.sess, RL_model_path)
        print("Model restored from: ", RL_model_path)

    run(model, arg_dict["predicate_num"], arg_dict["sequence"], RL_model_path)
    print("finish training...")


if __name__ == "__main__":
    main()

