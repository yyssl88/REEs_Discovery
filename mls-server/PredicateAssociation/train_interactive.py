from predicateAssoc import PAssoc
from model import DeepQNetwork
import argparse
import logging
import time
import os
import jpype


MAX_LHS_PREDICATES = 5

def run(pComb, model, validator):
    step = 0
    for episode in range(20):
        # initial observation
        pComb.reset()
        observation = pComb.initialAction(episode * 777)

        print(observation)

        while True:
            # find action
            action = model.choose_action(observation)
            if action == -1:
                break
            # take action and get next observation and reward
            observation_, reward, done = pComb.step(action, validator)

            print("Epoch {} : {}, {}, {}, {}".format(episode, observation_, reward, done, action))

            model.store_transition(observation, action, reward, observation_)

            if (step > 2) and (step % 2 == 0):
                model.learn()

            # swap observation
            observation = observation_
            if len(observation[observation==1]) > MAX_LHS_PREDICATES:
                break
            if done:
                break
            step += 1



def main():
    start = time.time()

    parser = argparse.ArgumentParser(description="Learn Predicate Association")
    parser.add_argument('-data_dir', '--data_dir', type=str, default="")
    parser.add_argument('-threshold', '--threshold', type=float, default=0.2)
    parser.add_argument('-predicate_num', '--predicate_num', type=int, default=0)
    parser.add_argument('-learning_rate', '--learning_rate', type=float, default=0.01)
    parser.add_argument('-reward_decay', '--reward_decay', type=float, default=0.9)
    parser.add_argument('-e_greedy', '--e_greedy', type=float, default=0.9)
    parser.add_argument('-replace_target_iter', '--replace_target_iter', type=int, default=200)
    parser.add_argument('-memory_size', '--memory_size', type=int, default=2000)
    parser.add_argument('-batch_size', '--batch_size', type=int, default=32)
    parser.add_argument('-model_path', '--model_path', type=str, default="model/model.ckpt")
    parser.add_argument('-epoch', '--epoch', type=int, default=1000)
    parser.add_argument('-if_interactive', '--if_interactive', type=int, default=1)


    args = parser.parse_args()
    arg_dict = args.__dict__
    for k, v in sorted(arg_dict.items()):
        logging.info('[Argument] %s=%r' % (k, v))
        print("k:", k, ", v:", v)

    # start Java
    jarpath = ""
    ext_dirs = ""
    classpath = jarpath + ";" + ext_dirs
    jpype.startJVM(jpype.getDefaultJVMPath(), "-ea", "-Djava.class.path={}".format(classpath))
    java_validator = jpype.JClass('sics.seiois.mlsserver.service.impl.RuleFinder') # to be check
    validator = java_validator()

    # run
    p_num = None
    if arg_dict["if_interactive"] == 1:
        p_num = validator.getAllPredicateNum()  # to be implemented!!!
    pAssoc = PAssoc(arg_dict["if_interactive"], p_num, arg_dict["threshold"], arg_dict["data_dir"])
    model = DeepQNetwork(arg_dict["predicate_num"], arg_dict["predicate_num"],
                         learning_rate=arg_dict["learning_rate"],
                         reward_decay=arg_dict["reward_decay"],
                         e_greedy=arg_dict["e_greedy"],
                         replace_target_iter=arg_dict["replace_target_iter"],
                         memory_size=arg_dict["memory_size"],
                         batch_size=arg_dict["batch_size"],
                         # output_graph=True
                         )

    run(pAssoc, model, validator)

    end = time.time()
    print("finish training, using time: ", str(end - start))


if __name__ == "__main__":
    main()

