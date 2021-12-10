# import sys
# print(sys.version)
# print("In run.py:", sys.path, "\n")

from model import DeepQNetwork
import argparse
import logging
import numpy as np
import time

import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()

# input: sequence of (state, action, reward), i.e., "Psel, p, reward; Psel, p, reward; ...",
#        in form of "id1 id2 id3,p1,reward1;id1 id2 id4,p2,reward2;...", where reward = support - threshold.
# state: Psel - pid1  pid2  ...
# action: pid
# pnum: whole predicate num
def run(model, sequence_file, N, RL_model_path):
    pnum = 0
    line_id = 0
    file = open(sequence_file, 'r')
    for line in file:
        if (line_id == N):
            break
        if line_id == 0:
            pnum = int(line)
        else:
            seq = line.split(",")

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
        line_id = line_id + 1
    file.close()

    # train RL model
    model.learn()

    # save RL model
    saver = tf.train.Saver()
    save_path = saver.save(model.sess, RL_model_path)
    print("Model saved in path: %s" % save_path)


def main():
    start = time.time()

    parser = argparse.ArgumentParser(description="Learn Predicate Association")
    parser.add_argument('-learning_rate', '--learning_rate', type=float, default=0.01)
    parser.add_argument('-reward_decay', '--reward_decay', type=float, default=0.9)
    parser.add_argument('-e_greedy', '--e_greedy', type=float, default=0.9)
    parser.add_argument('-replace_target_iter', '--replace_target_iter', type=int, default=200)
    parser.add_argument('-memory_size', '--memory_size', type=int, default=2000)
    parser.add_argument('-batch_size', '--batch_size', type=int, default=32)
    parser.add_argument('-data_name', '--data_name', type=str, default="airports")
    parser.add_argument('-N', '--N', type=int, default=1000)

    args = parser.parse_args()
    arg_dict = args.__dict__
    for k, v in sorted(arg_dict.items()):
        logging.info('[Argument] %s=%r' % (k, v))
        print("k:", k, ", v:", v)


    data_name = arg_dict["data_name"]
    N = arg_dict["N"]

    sequence_file = "./model/sequence_" + data_name + ".txt"
    RL_model_path = "./model/" + data_name + "_N" + str(N) + "/model.ckpt"

    pnum = 0
    with open(sequence_file, "r") as f:
        pnum = int(f.readline())
        f.close()

    # run
    model = DeepQNetwork(pnum, pnum,
                      learning_rate=arg_dict["learning_rate"],
                      reward_decay=arg_dict["reward_decay"],
                      e_greedy=arg_dict["e_greedy"],
                      replace_target_iter=arg_dict["replace_target_iter"],
                      memory_size=arg_dict["memory_size"],
                      batch_size=arg_dict["batch_size"],
                      # output_graph=True
                      )


    run(model, sequence_file, N + 1, RL_model_path)

    end = time.time()

    print("finish training, using time: ", str(end - start))


if __name__ == "__main__":
    main()

