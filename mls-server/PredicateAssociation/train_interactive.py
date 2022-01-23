from predicateAssoc import PAssoc
from model import DeepQNetwork
import argparse
import logging
import time
import numpy as np
import jpype

import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()


MAX_LHS_PREDICATES = 5


def run(pComb, model, nonConstantPredicateIDs, validator, epoch, model_path):
    select_rhs = np.random.randint(0, pComb.state_num)

    step = 0
    for episode in range(epoch):
        # initial observation
        pComb.reset()
        observation = pComb.initialAction(episode * 777, select_rhs)

        print(observation)

        while True:
            # find action
            action = model.choose_action(observation, nonConstantPredicateIDs, select_rhs)
            if action == -1:
                break
            # take action and get next observation and reward
            observation_, reward, done = pComb.step(action, select_rhs, validator)

            print("Epoch {} : {}, {}, {}, {}".format(episode, observation_, reward, done, action))

            model.store_transition(observation, action, reward, observation_)

            if (step > 30) and (step % 3 == 0):
                model.learn()

            # swap observation
            observation = observation_
            if len(observation[observation==1]) > MAX_LHS_PREDICATES:
                break
            if done:
                break
            step += 1

    # save RL model
    saver = tf.train.Saver()
    save_path = saver.save(model.sess, model_path)
    print("Model saved in path: ", save_path)


def main():
    start = time.time()

    parser = argparse.ArgumentParser(description="Learn Predicate Association")
    parser.add_argument('-data_dir', '--data_dir', type=str, default="")
    parser.add_argument('-supp_thr', '--supp_thr', type=float, default=0.2)

    parser.add_argument('-learning_rate', '--learning_rate', type=float, default=0.01)
    parser.add_argument('-reward_decay', '--reward_decay', type=float, default=0.9)
    parser.add_argument('-e_greedy', '--e_greedy', type=float, default=0.9)
    parser.add_argument('-replace_target_iter', '--replace_target_iter', type=int, default=200)
    parser.add_argument('-memory_size', '--memory_size', type=int, default=2000)
    parser.add_argument('-batch_size', '--batch_size', type=int, default=32)

    parser.add_argument('-model_path', '--model_path', type=str, default="model/model.ckpt")
    parser.add_argument('-epoch', '--epoch', type=int, default=1000)

    parser.add_argument('-if_interactive', '--if_interactive', type=int, default=1)
    parser.add_argument('-directory_path', '--directory_path', type=str, default="")
    parser.add_argument('-constant_file', '--constant_file', type=str, default="")
    parser.add_argument('-chunkLength', '--chunkLength', type=int, default=200000)
    parser.add_argument('-maxTupleNum', '--maxTupleNum', type=int, default=2)
    parser.add_argument('-conf_thr', '--conf_thr', type=int, default=0.9)

    parser.add_argument('-classpath', '--classpath', type=str, default="")


    args = parser.parse_args()
    arg_dict = args.__dict__
    for k, v in sorted(arg_dict.items()):
        logging.info('[Argument] %s=%r' % (k, v))
        print("k:", k, ", v:", v)

    # start Java
    classpath = arg_dict["classpath"].replace(":", ";")
    print("classpath: ", classpath)
    jpype.startJVM(jpype.getDefaultJVMPath(), "-ea", "-Djava.classpath={}".format(classpath))
    args_ = ["directory_path={}".format(arg_dict["directory_path"]), "constant_file={}".format(arg_dict["constant_file"]),
             "chunkLength={}".format(arg_dict["chunkLength"]), "maxTupleNum={}".format(arg_dict["maxTupleNum"])]
    java_validator = jpype.JClass('sics.seiois.mlsserver.biz.der.mining.validation.CalculateRuleSuppConf')
    validator = java_validator()
    validator.preparePredicates(args_)

    # run
    p_num = None
    nonConstantPredicateIDs = None
    if arg_dict["if_interactive"] == 1:
        p_num = validator.getAllPredicatesNum()
        nonConstantPredicateIDs = validator.getNonConstantPredicateIDs()
    print("p_num: ", p_num)
    print("nonConstantPredicateIDs: ", nonConstantPredicateIDs)
    pAssoc = PAssoc(arg_dict["if_interactive"], p_num, arg_dict["supp_thr"], arg_dict["conf_thr"], arg_dict["data_dir"])
    model = DeepQNetwork(p_num, p_num,
                         learning_rate=arg_dict["learning_rate"],
                         reward_decay=arg_dict["reward_decay"],
                         e_greedy=arg_dict["e_greedy"],
                         replace_target_iter=arg_dict["replace_target_iter"],
                         memory_size=arg_dict["memory_size"],
                         batch_size=arg_dict["batch_size"],
                         # output_graph=True
                         )

    run(pAssoc, model, nonConstantPredicateIDs, validator, arg_dict["epoch"], arg_dict["model_path"])

    end = time.time()
    print("finish training, using time: ", str(end - start), arg_dict["model_path"])


if __name__ == "__main__":
    main()

