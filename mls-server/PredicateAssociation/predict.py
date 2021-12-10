import numpy as np
import argparse

import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()

from model import DeepQNetwork


def main():
    parser = argparse.ArgumentParser(description="Learn Predicate Association")
    parser.add_argument('-init_train', '--init_train', type=int, default=1)
    parser.add_argument('-predicate_num', '--predicate_num', type=int, default=0)
    parser.add_argument('-sequence', '--sequence', type=str, default="") # "Psel, p, reward; Psel, p, reward; ..."
    parser.add_argument('-legal_nextP_indices', '--legal_nextP_indices', type=str, default="") # "idx1 idx2 ...;idx1 idx2 ...;..."
    parser.add_argument('-learning_rate', '--learning_rate', type=float, default=0.01)
    parser.add_argument('-reward_decay', '--reward_decay', type=float, default=0.9)
    parser.add_argument('-e_greedy', '--e_greedy', type=float, default=0.9)
    parser.add_argument('-replace_target_iter', '--replace_target_iter', type=int, default=200)
    parser.add_argument('-memory_size', '--memory_size', type=int, default=2000)
    parser.add_argument('-batch_size', '--batch_size', type=int, default=32)
    parser.add_argument('-model_path', '--model_path', type=str, default="")

    args = parser.parse_args()
    arg_dict = args.__dict__

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
    saver = tf.train.Saver()
    saver.restore(model.sess, RL_model_path)
    print("Model restored from: ", RL_model_path)

    data = arg_dict["sequence"].split(";")
    legal_nextP_idx = arg_dict["legal_nextP_indices"].split(";")
    results = ""
    # 1. data form: "pid1 pid2 pid5;pid2 pid3 pid10;..." --- predict next_p with maximum predicted reward
    #    legal_nextP_index: "idx1 idx2 idx3...;idx3 idx6 idx9...;..."
    if "," not in data[0]:
        for Psel, legal_nextP in zip(data, legal_nextP_idx):
            if legal_nextP == "":
                results += "-1;"
                continue
            pset = Psel.split(" ")
            state = np.zeros(arg_dict["predicate_num"], dtype=int)
            for idx in pset:
                state[int(idx)] = 1
            legal_nextP = legal_nextP.split(" ")
            legal_nextP = list(map(int, legal_nextP))
            action = model.predictAction(state, legal_nextP)
            results += str(action)
            results += ";"

    # 2. data form: "pid1 pid2 pid5,p1;pid2 pid3 pid10,p2;..." --- predict whether Psel can be expanded with next_p
    else:
        for Psel_p in data:
            pset = Psel_p.split(",")[0].split(" ")
            next_p = int(Psel_p.split(",")[1])
            state = np.zeros(arg_dict["predicate_num"], dtype=int)
            for idx in pset:
                state[int(idx)] = 1
            ifExpand = model.predictCorrelation(state, next_p)
            results += str(ifExpand)
            results += ";"

    results = results[:-1] # remove last ";"
    print(results)


if __name__ == "__main__":
    main()

