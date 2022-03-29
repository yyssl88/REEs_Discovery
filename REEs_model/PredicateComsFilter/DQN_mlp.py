import sys
print("In model.py:", sys.version)
print("In model.py:", sys.path)

import numpy as np
from collections import defaultdict
import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()

np.random.seed(1)
tf.set_random_seed(1)


# Deep Q Network off-policy
class DeepQNetwork:
    def __init__(
            self,
            n_actions,
            n_features,
            learning_rate=0.01,
            reward_decay=0.99,
            e_greedy=0.7,
            replace_target_iter=300,
            memory_size=2048,
            batch_size=32,
            e_greedy_increment=None,
            output_graph=False,
            double_q=True,
    ):
        self.n_actions = n_actions
        self.n_features = n_features
        self.lr = learning_rate
        self.gamma = reward_decay
        self.epsilon_max = e_greedy
        self.replace_target_iter = replace_target_iter
        self.memory_size = memory_size
        self.batch_size = batch_size
        self.epsilon_increment = e_greedy_increment
        self.epsilon = 0 if e_greedy_increment is not None else self.epsilon_max

        # set Double DQN
        self.double_q = double_q

        # total learning step
        self.learn_step_counter = 0

        # initialize zero memory [s, a, r, s_]
        self.memory = np.zeros((self.memory_size, n_features * 2 + 2))

        # consist of [target_net, evaluate_net]
        self._build_net()
        t_params = tf.get_collection('target_net_params')
        e_params = tf.get_collection('eval_net_params')
        self.replace_target_op = [tf.assign(t, e) for t, e in zip(t_params, e_params)]

        self.sess = tf.Session()

        if output_graph:
            # $ tensorboard --logdir=logs
            # tf.train.SummaryWriter soon be deprecated, use following
            tf.summary.FileWriter("logs/", self.sess.graph)

        # initialize saver
        self.saver = tf.train.Saver()

        self.sess.run(tf.global_variables_initializer())
        self.cost_his = []

    def saveOneMatrix(self, fout, matrix):
        fout.write(str(matrix.shape[0]) + " " + str(matrix.shape[1]))
        fout.write("\n")
        for i in range(matrix.shape[0]):
            fout.write(' '.join([str(e) for e in matrix[i]]))
            fout.write("\n")
        fout.write("\n")

    def saveModelToText(self, output_file):
        w1_m, b1_m, w2_m, b2_m = self.sess.run([self.w1, self.b1, self.w2, self.b2])
        f = open(output_file, 'w')
        self.saveOneMatrix(f, w1_m)
        self.saveOneMatrix(f, b1_m)
        self.saveOneMatrix(f, w2_m)
        self.saveOneMatrix(f, b2_m)
        f.close()

    def _build_net(self):
        # ------------------ build evaluate_net ------------------
        self.s = tf.placeholder(tf.float32, [None, self.n_features], name='s')  # input
        self.q_target = tf.placeholder(tf.float32, [None, self.n_actions], name='Q_target')  # for calculating loss
        with tf.variable_scope('eval_net'):
            # c_names(collections_names) are the collections to store variables
            c_names, n_l1, w_initializer, b_initializer = \
                ['eval_net_params', tf.GraphKeys.GLOBAL_VARIABLES], 200, \
                tf.random_normal_initializer(0., 0.03), tf.constant_initializer(0.01)  # config of layers

            # first layer. collections is used later when assign to target net
            with tf.variable_scope('l1'):
                self.w1 = tf.get_variable('w1', [self.n_features, n_l1], initializer=w_initializer, collections=c_names)
                self.b1 = tf.get_variable('b1', [1, n_l1], initializer=b_initializer, collections=c_names)
                l1 = tf.nn.relu(tf.matmul(self.s, self.w1) + self.b1)

            # with tf.variable_scope('l1_m'):
            #     self.w1_m = tf.get_variable('w1_m', [n_l1, n_l1], initializer=w_initializer, collections=c_names)
            #     self.b1_m = tf.get_variable('b1_m', [1, n_l1], initializer=b_initializer, collections=c_names)
            #     l1 = tf.nn.relu(tf.matmul(l1, self.w1_m) + self.b1_m)

            # second layer. collections is used later when assign to target net
            with tf.variable_scope('l2'):
                self.w2 = tf.get_variable('w2', [n_l1, self.n_actions], initializer=w_initializer, collections=c_names)
                self.b2 = tf.get_variable('b2', [1, self.n_actions], initializer=b_initializer, collections=c_names)
                self.q_eval = tf.matmul(l1, self.w2) + self.b2

        with tf.variable_scope('loss'):
            self.loss = tf.reduce_mean(tf.squared_difference(self.q_target, self.q_eval))
        with tf.variable_scope('train'):
            self._train_op = tf.train.RMSPropOptimizer(self.lr).minimize(self.loss)

        # ------------------ build target_net ------------------
        self.s_ = tf.placeholder(tf.float32, [None, self.n_features], name='s_')    # input
        with tf.variable_scope('target_net'):
            # c_names(collections_names) are the collections to store variables
            c_names = ['target_net_params', tf.GraphKeys.GLOBAL_VARIABLES]

            # first layer. collections is used later when assign to target net
            with tf.variable_scope('l1'):
                w1 = tf.get_variable('w1', [self.n_features, n_l1], initializer=w_initializer, collections=c_names)
                b1 = tf.get_variable('b1', [1, n_l1], initializer=b_initializer, collections=c_names)
                l1 = tf.nn.relu(tf.matmul(self.s_, w1) + b1)

            # second layer. collections is used later when assign to target net
            with tf.variable_scope('l2'):
                w2 = tf.get_variable('w2', [n_l1, self.n_actions], initializer=w_initializer, collections=c_names)
                b2 = tf.get_variable('b2', [1, self.n_actions], initializer=b_initializer, collections=c_names)
                self.q_next = tf.matmul(l1, w2) + b2

    def store_transition(self, s, a, r, s_):
        if not hasattr(self, 'memory_counter'):
            self.memory_counter = 0

        transition = np.hstack((s, [a, r], s_))

        # replace the old memory with new memory
        index = self.memory_counter % self.memory_size
        self.memory[index, :] = transition

        self.memory_counter += 1

    def choose_action(self, observation_lrhs, selected_rhs, predicatesArr):
        unselected = []
        sameRelationsDict = defaultdict(int)
        # update relations of RHS
        r_dict = predicatesArr[selected_rhs].getRelations()
        for k, v in r_dict.items():
            if k not in sameRelationsDict:
                sameRelationsDict[k] = v
        selected_predicates = 0
        for sc, e in enumerate(observation_lrhs):
            if sc >= observation_lrhs.shape[0] / 2:
                break
            if e == 0.0:
                unselected.append(sc)
            else:
                selected_predicates += 1
                predicate = predicatesArr[sc]
                r_dict = predicate.getRelations()
                # update relations of tuple variables
                for k, v in r_dict.items():
                    if k not in sameRelationsDict:
                        sameRelationsDict[k] = v
        # remove the selected rhs
        if selected_rhs in unselected:
            unselected.remove(selected_rhs)

        unselected_new = []
        # if action is the first predicate, DO NOT select constant predicates
        if selected_predicates == 0:
            for pid in unselected:
                if predicatesArr[pid].isConstantPredicate():
                    continue
                # check valid
                r_dict = predicatesArr[pid].getRelations()
                isValid = True
                for k, v in r_dict.items():
                    if v != sameRelationsDict[k]:
                        isValid = False
                        break
                if isValid:
                    unselected_new.append(pid)
        else:
            # make sure that unselected are all valid
            unselected_new = []
            for pid in unselected:
                r_dict = predicatesArr[pid].getRelations()
                isValid = True
                for k, v in r_dict.items():
                    if v != sameRelationsDict[k]:
                        isValid = False
                        break
                if isValid:
                    unselected_new.append(pid)

        if len(unselected_new) <= 0:
            return -1

        # to have batch dimension when feed into tf placeholder
        observation_new = observation_lrhs[np.newaxis, :]

        if np.random.uniform() < self.epsilon:
            # forward feed the observation and get q value for every actions
            actions_value = self.sess.run(self.q_eval, feed_dict={self.s: observation_new})
            #action = np.argmax(actions_value)
            actions_value = np.hstack(actions_value)
            action_values_s = [[i, e] for i, e in enumerate(actions_value)]
            action_values_s = sorted(action_values_s, key = lambda x : x[1], reverse=1)
            action = -1
            for e in action_values_s:
                if e[0] in unselected_new:
                    action = e[0]
                    break
        else:
            #action = np.random.randint(0, self.n_actions)
            action_sc = np.random.randint(0, len(unselected_new))
            action = unselected_new[action_sc]
        return action

    def saveModel(self, model_path):
        self.saver.save(self.sess, model_path)

    def loadModel(self, model_path):
        self.saver.restore(self.sess, model_path)

    '''
    def choose_action(self, observation, nonConstantPredicateIDs, select_rhs):
        unselected = []
        containNonCPredicate = False
        for sc, e in enumerate(observation):
            if sc >= observation.shape[0] / 2:
                break
            if e == 0.0:
                unselected.append(sc)
            elif sc in nonConstantPredicateIDs:
                containNonCPredicate = True

        # make sure {Psel, next_action} contains at least one non-constant predicate
        if containNonCPredicate is False:
            unselected = list(set(unselected).intersection(set(nonConstantPredicateIDs)))

        if select_rhs in unselected:
            unselected.remove(select_rhs)

        if len(unselected) <= 0:
            return -1

        # to have batch dimension when feed into tf placeholder
        observation = observation[np.newaxis, :]

        if np.random.uniform() < self.epsilon:
            # forward feed the observation and get q value for every actions
            actions_value = self.sess.run(self.q_eval, feed_dict={self.s: observation})
            #action = np.argmax(actions_value)
            actions_value = np.hstack(actions_value)
            action_values_s = [[i, e] for i, e in enumerate(actions_value)]
            action_values_s = sorted(action_values_s, key = lambda x : x[1], reverse=1)
            action = -1
            for e in action_values_s:
                if e[0] in unselected:
                    action = e[0]
                    break
        else:
            #action = np.random.randint(0, self.n_actions)
            action_sc = np.random.randint(0, len(unselected))
            action = unselected[action_sc]
        return action
    '''

    def predictCorrelation(self, observation, nextAction):
        observation = observation[np.newaxis, :]
        actions_value = self.sess.run(self.q_eval, feed_dict={self.s : observation})
        if np.hstack(actions_value)[int(nextAction)] > 0.0:
            return 1
        else:
            return 0

    def predictAction(self, observation, legal_nextP):
        observation = observation[np.newaxis, :]
        actions_value = self.sess.run(self.q_eval, feed_dict={self.s : observation})
        index = np.argsort(np.hstack(actions_value))[::-1] # the index of ranked values
        observation = np.hstack(observation)
        for idx in index:
            if observation[idx] != 1 and idx in legal_nextP:
                return idx
        return -1


    def learn(self):
        # check to replace target parameters
        if self.learn_step_counter % self.replace_target_iter == 0:
            self.sess.run(self.replace_target_op)
            print('\ntarget_params_replaced\n')

        # sample batch memory from all memory
        if self.memory_counter > self.memory_size:
            sample_index = np.random.choice(self.memory_size, size=self.batch_size)
        else:
            sample_index = np.random.choice(self.memory_counter, size=self.batch_size)
        batch_memory = self.memory[sample_index, :]


        '''
        q_next, q_eval = self.sess.run(
            [self.q_next, self.q_eval],
            feed_dict={
                self.s_: batch_memory[:, -self.n_features:],  # fixed params
                self.s: batch_memory[:, :self.n_features],  # newest params
            })

        # change q_target w.r.t q_eval's action
        q_target = q_eval.copy()

        batch_index = np.arange(self.batch_size, dtype=np.int32)
        eval_act_index = batch_memory[:, self.n_features].astype(int)
        reward = batch_memory[:, self.n_features + 1]

        if self.double_q:
            max_act4next = np.argmax()

        q_target[batch_index, eval_act_index] = reward + self.gamma * np.max(q_next, axis=1)
        '''

        q_next, q_eval4next = self.sess.run(
            [self.q_next, self.q_eval],
            feed_dict={self.s_: batch_memory[:, -self.n_features:],    # next observation
                       self.s: batch_memory[:, -self.n_features:]})    # next observation
        q_eval = self.sess.run(self.q_eval, {self.s: batch_memory[:, :self.n_features]})

        q_target = q_eval.copy()

        batch_index = np.arange(self.batch_size, dtype=np.int32)
        eval_act_index = batch_memory[:, self.n_features].astype(int)
        reward = batch_memory[:, self.n_features + 1]

        if self.double_q:
            max_act4next = np.argmax(q_eval4next, axis=1)        # the action that brings the highest value is evaluated by q_eval
            selected_q_next = q_next[batch_index, max_act4next]  # Double DQN, select q_next depending on above actions
        else:
            selected_q_next = np.max(q_next, axis=1)    # the natural DQN

        q_target[batch_index, eval_act_index] = reward + self.gamma * selected_q_next


        """
        For example in this batch I have 2 samples and 3 actions:
        q_eval =
        [[1, 2, 3],
         [4, 5, 6]]

        q_target = q_eval =
        [[1, 2, 3],
         [4, 5, 6]]

        Then change q_target with the real q_target value w.r.t the q_eval's action.
        For example in:
            sample 0, I took action 0, and the max q_target value is -1;
            sample 1, I took action 2, and the max q_target value is -2:
        q_target =
        [[-1, 2, 3],
         [4, 5, -2]]

        So the (q_target - q_eval) becomes:
        [[(-1)-(1), 0, 0],
         [0, 0, (-2)-(6)]]

        We then backpropagate this error w.r.t the corresponding action to network,
        leave other action as error=0 cause we didn't choose it.
        """

        # train eval network
        _, self.cost = self.sess.run([self._train_op, self.loss],
                                     feed_dict={self.s: batch_memory[:, :self.n_features],
                                                self.q_target: q_target})
        self.cost_his.append(self.cost)

        # increasing epsilon
        self.epsilon = self.epsilon + self.epsilon_increment if self.epsilon < self.epsilon_max else self.epsilon_max
        self.learn_step_counter += 1

    def evaluate(self, observation_lrhs):
        # to have batch dimension when feed into tf placeholder
        observation_new = observation_lrhs[np.newaxis, :]
        # forward feed the observation and get q value for every actions
        actions_value = self.sess.run(self.q_eval, feed_dict={self.s: observation_new})
        #action = np.argmax(actions_value)
        actions_value = np.hstack(actions_value)
        action_values_s = [[i, e] for i, e in enumerate(actions_value)]
        return action_values_s


    def plot_cost(self):
        import matplotlib.pyplot as plt
        plt.plot(np.arange(len(self.cost_his)), self.cost_his)
        plt.ylabel('Cost')
        plt.xlabel('training steps')
        plt.show()

