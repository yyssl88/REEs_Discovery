import numpy as np
import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()
import sys
import os
#sys.path.append("../")
from REEs_model.utils import convert_to_onehot
from REEs_model.parameters import __eval__, __evalR__
from sklearn.model_selection import train_test_split
import time


class FilterClassifier(object):
    def __init__(self,
                 n_features,
                 learning_rate,
                 hidden_dim,
                 epochs,
                 batch_size):
        self.n_features = n_features
        self.learning_rate = learning_rate
        self.hidden_dim = hidden_dim
        self.epochs = epochs
        self.batch_size = batch_size

    def construct(self):
        self.features = tf.placeholder(tf.float32, [None, self.n_features], name='predicates_features')
        self.labels = tf.placeholder(tf.float32, [None, 2], name='label')

        with tf.variable_scope("evaluation"):
            c_names, n_l1, w_initializer, b_initializer = \
                ["evaluation_params", tf.GraphKeys.GLOBAL_VARIABLES], self.hidden_dim, \
                tf.random_normal_initializer(0., 0.03), tf.constant_initializer(0.01)

            # first layer
            with tf.variable_scope('l1'):
                self.w1 = tf.get_variable('w1', [self.n_features, n_l1], initializer=w_initializer, collections=c_names)
                self.b1 = tf.get_variable('b1', [1, n_l1], initializer=b_initializer, collections=c_names)
                l1 = tf.nn.relu(tf.matmul(self.features, self.w1) + self.b1)
            # second layer
            with tf.variable_scope('l2'):
                self.w2 = tf.get_variable('w2', [n_l1, n_l1], initializer=w_initializer, collections=c_names)
                self.b2 = tf.get_variable('b2', [1, n_l1], initializer=b_initializer, collections=c_names)
                l2 = tf.nn.relu(tf.matmul(l1, self.w2) + self.b2)
            # final layer
            with tf.variable_scope('l3'):
                self.w3 = tf.get_variable('w3', [n_l1, 2], initializer=w_initializer, collections=c_names)
                self.b3 = tf.get_variable('b3', [1, 2], initializer=b_initializer, collections=c_names)
                logit = tf.matmul(l2, self.w3) + self.b3

        self.predictions = tf.nn.softmax(logit)
        # loss function
        self.cross_entropy = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=self.predictions, labels=self.labels))
        self.optimizer = tf.train.AdamOptimizer(learning_rate=self.learning_rate)
        self.train_op = self.optimizer.minimize(self.cross_entropy)
        # accuracy
        correct_predictions = tf.equal(tf.argmax(self.predictions, 1), tf.argmax(self.labels, 1))
        self.accuracy_tf = tf.reduce_mean(tf.cast(correct_predictions, 'float'))

    def generateAllTrainingInstances(self, pComb, DQN, validator, max_lhs_predicates, N=200):
        rawTrainData = pComb.selectMultiPathsRandom(validator, N)
        allData, allLabels = pComb.generateTrainingInstances(rawTrainData, DQN, validator, max_lhs_predicates)

        # transfer label to one-hot
        allLabels = convert_to_onehot(allLabels)
        trainData, validData, trainLabels, validLabels = train_test_split(allData, allLabels, train_size=0.9, random_state=42)
        return trainData, validData, trainLabels, validLabels

    def generate_batch(self, batch_id, batch_size, trainData, trainLabels):
        start_id = batch_size * batch_id
        end_id = batch_size * (batch_id + 1)
        batchTrainData = trainData[start_id: end_id]
        batchTrainLabels = trainLabels[start_id: end_id]
        return batchTrainData, batchTrainLabels

    def train(self, trainData, trainLabels, validData, validLabels):
        print('start training...')
        # construct the model
        self.construct()
        # initialize all variables
        init_op = tf.global_variables_initializer()
        saver = tf.train.Saver()
        # open a session and run the training graph
        session_config = tf.ConfigProto(log_device_placement=True)
        session_config.gpu_options.allow_growth = True
        self.sess = tf.Session(config=session_config)
        self.sess.run(init_op)

        for epoch in range(self.epochs):
            num_batch = len(trainData) // self.batch_size + 1
            for batch_id in range(num_batch):
                start_train = time.time()
                batchTrainData, batchTrainLabels = self.generate_batch(batch_id, self.batch_size, trainData, trainLabels)
                feed_dict_train = {self.features: batchTrainData,
                                   self.labels: batchTrainLabels}

                _, batchTrainPredictions, loss_batch = self.sess.run([self.train_op, self.predictions, self.cross_entropy], feed_dict=feed_dict_train)
                end_train = time.time()
                train_measurements = __eval__(np.argmax(batchTrainLabels, 1), np.argmax(batchTrainPredictions, 1))
                log = f'epoch {epoch}, train_loss: {loss_batch}, train_acc: {train_measurements[0]}, train_recall: {train_measurements[1]}, ' \
                      f'train_precision: {train_measurements[2]}, train_f1: {train_measurements[3]}, time: {end_train - start_train} '
                print(log)
            # validation
            feed_dict_valid = {self.features: validData, self.labels: validLabels}
            validPredictions = self.sess.run(self.predictions, feed_dict=feed_dict_valid)
            valid_measurements = __eval__(np.argmax(validPredictions, 1), np.argmax(validLabels, 1))
            valid_log = f'epoch {epoch}, valid_acc: {valid_measurements[0]}, valid_recall: {valid_measurements[1]}, ' \
                    f'valid_precision: {valid_measurements[2]}, valid_f1: {valid_measurements[3]} '
            print("Histogram of validation is : ", np.histogram(np.argmax(validPredictions, 1)))
            print(validPredictions)
            print(valid_log)

        # test random data
        XX = self.generateRandomData(self.n_features)
        feed_dict_test = {self.features: XX}
        testPredictions = self.sess.run(self.predictions, feed_dict=feed_dict_test)
        print(testPredictions)
        print("Histogram of Test is : ", np.histogram(np.argmax(testPredictions, 1)))

    def generateRandomData(self, feature_num, N=200):
        X = np.zeros((N, feature_num))
        predicates_num = feature_num // 2
        for i in range(N):
            # select lhs
            k = np.random.randint(1, 6)
            sc = np.random.choice(predicates_num, k, replace=False)
            X[i][sc] = 1.0
            # select rhs
            k = np.random.randint(0, predicates_num)
            X[i][predicates_num + k] = 1.0
        return X


    def saveFilterData(self, filter_dir, trainData, validData, trainLabels, validLabels):
        trainData = np.array(trainData)
        validData = np.array(validData)
        trainLabels = np.array(trainLabels)
        validLabels = np.array(validLabels)
        np.save(os.path.join(filter_dir, 'trainData.npy'), trainData)
        np.save(os.path.join(filter_dir, 'validData.npy'), validData)
        np.save(os.path.join(filter_dir, 'trainLabels.npy'), trainLabels)
        np.save(os.path.join(filter_dir, 'validLabels.npy'), validLabels)

    def loadFilterData(self, filter_dir):
        trainData = np.load(os.path.join(filter_dir, 'trainData.npy'))
        validData = np.load(os.path.join(filter_dir, 'validData.npy'))
        trainLabels = np.load(os.path.join(filter_dir, 'trainLabels.npy'))
        validLabels = np.load(os.path.join(filter_dir, 'validLabels.npy'))
        return trainData, validData, trainLabels, validLabels

    def saveOneMatrix(self, fout, matrix):
        fout.write(str(matrix.shape[0]) + " " + str(matrix.shape[1]))
        fout.write("\n")
        for i in range(matrix.shape[0]):
            fout.write(' '.join([str(e) for e in matrix[i]]))
            fout.write("\n")
        fout.write("\n")

    def saveModelToText(self, output_file):
        w1_m, b1_m, w2_m, b2_m, w3_m, b3_m = self.sess.run([self.w1, self.b1, self.w2, self.b2, self.w3, self.b3])
        f = open(output_file, 'w')
        self.saveOneMatrix(f, w1_m)
        self.saveOneMatrix(f, b1_m)
        self.saveOneMatrix(f, w2_m)
        self.saveOneMatrix(f, b2_m)
        self.saveOneMatrix(f, w3_m)
        self.saveOneMatrix(f, b3_m)
        f.close()



class FilterRegressor(object):
    def __init__(self,
                 n_features,
                 learning_rate,
                 hidden_dim,
                 epochs,
                 batch_size):
        self.n_features = n_features
        self.learning_rate = learning_rate
        self.hidden_dim = hidden_dim
        self.epochs = epochs
        self.batch_size = batch_size

    def construct(self):
        self.features = tf.placeholder(tf.float32, [None, self.n_features], name='predicates_features')
        self.labels = tf.placeholder(tf.float32, [None, 1], name='label')

        with tf.variable_scope("evaluation"):
            c_names, n_l1, w_initializer, b_initializer = \
                ["evaluation_params", tf.GraphKeys.GLOBAL_VARIABLES], self.hidden_dim, \
                tf.random_normal_initializer(0., 0.03), tf.constant_initializer(0.01)

            # first layer
            with tf.variable_scope('l1'):
                self.w1 = tf.get_variable('w1', [self.n_features, n_l1], initializer=w_initializer, collections=c_names)
                self.b1 = tf.get_variable('b1', [1, n_l1], initializer=b_initializer, collections=c_names)
                l1 = tf.nn.relu(tf.matmul(self.features, self.w1) + self.b1)
            # second layer
            with tf.variable_scope('l2'):
                self.w2 = tf.get_variable('w2', [n_l1, n_l1], initializer=w_initializer, collections=c_names)
                self.b2 = tf.get_variable('b2', [1, n_l1], initializer=b_initializer, collections=c_names)
                l2 = tf.nn.relu(tf.matmul(l1, self.w2) + self.b2)
            # final layer
            with tf.variable_scope('l3'):
                self.w3 = tf.get_variable('w3', [n_l1, 1], initializer=w_initializer, collections=c_names)
                self.b3 = tf.get_variable('b3', [1, 1], initializer=b_initializer, collections=c_names)
                self.predictions = tf.matmul(l2, self.w3) + self.b3

        # loss function
        self.loss = tf.reduce_mean(tf.losses.mean_squared_error(predictions=self.predictions, labels=self.labels))
        self.optimizer = tf.train.AdamOptimizer(learning_rate=self.learning_rate)
        self.train_op = self.optimizer.minimize(self.loss)

    def generateAllTrainingInstances(self, pComb, DQN, validator, max_lhs_predicates, tokenVobs, N):
        rawTrainData = pComb.selectMultiPathsRandom(validator, tokenVobs, N)
        allData, allLabels = pComb.generateTrainingInstances(rawTrainData, DQN, validator, max_lhs_predicates, tokenVobs)

        trainData, validData, trainLabels, validLabels = train_test_split(allData, allLabels, train_size=0.9, random_state=42)
        return trainData, validData, trainLabels, validLabels

    def saveFilterData(self, filter_dir, trainData, validData, trainLabels, validLabels):
        trainData = np.array(trainData)
        validData = np.array(validData)
        trainLabels = np.array(trainLabels)
        validLabels = np.array(validLabels)
        np.save(os.path.join(filter_dir, 'trainData.npy'), trainData)
        np.save(os.path.join(filter_dir, 'validData.npy'), validData)
        np.save(os.path.join(filter_dir, 'trainLabels.npy'), trainLabels)
        np.save(os.path.join(filter_dir, 'validLabels.npy'), validLabels)

    def loadFilterData(self, filter_dir):
        trainData = np.load(os.path.join(filter_dir, 'trainData.npy'))
        validData = np.load(os.path.join(filter_dir, 'validData.npy'))
        trainLabels = np.load(os.path.join(filter_dir, 'trainLabels.npy'))
        validLabels = np.load(os.path.join(filter_dir, 'validLabels.npy'))
        return trainData, validData, trainLabels, validLabels

    def generate_batch(self, batch_id, batch_size, trainData, trainLabels):
        start_id = batch_size * batch_id
        end_id = batch_size * (batch_id + 1)
        batchTrainData = trainData[start_id: end_id]
        batchTrainLabels = trainLabels[start_id: end_id]
        return batchTrainData, batchTrainLabels

    def train(self, trainData, trainLabels, validData, validLabels):
        print('start training...')
        # construct the model
        self.construct()
        # initialize all variables
        init_op = tf.global_variables_initializer()
        saver = tf.train.Saver()
        # open a session and run the training graph
        session_config = tf.ConfigProto(log_device_placement=True)
        session_config.gpu_options.allow_growth = True
        self.sess = tf.Session(config=session_config)
        self.sess.run(init_op)

        for epoch in range(self.epochs):
            num_batch = len(trainData) // self.batch_size + 1
            for batch_id in range(num_batch):
                start_train = time.time()
                batchTrainData, batchTrainLabels = self.generate_batch(batch_id, self.batch_size, trainData, trainLabels)
                feed_dict_train = {self.features: batchTrainData,
                                   self.labels: batchTrainLabels}

                _, batchTrainPredictions, loss_batch = self.sess.run([self.train_op, self.predictions, self.loss], feed_dict=feed_dict_train)
                end_train = time.time()
                train_measurements = __evalR__(batchTrainPredictions, batchTrainLabels)
                print("Training Loss : {} with time : {}".format(train_measurements, end_train - start_train))
            # validation
            feed_dict_valid = {self.features: validData, self.labels: validLabels}
            validPredictions = self.sess.run(self.predictions, feed_dict=feed_dict_valid)
            valid_measurements = __evalR__(validPredictions, validLabels)
            print("Validation Loss : {}".format(valid_measurements))

    def saveOneMatrix(self, fout, matrix):
        fout.write(str(matrix.shape[0]) + " " + str(matrix.shape[1]))
        fout.write("\n")
        for i in range(matrix.shape[0]):
            fout.write(' '.join([str(e) for e in matrix[i]]))
            fout.write("\n")
        fout.write("\n")

    def saveModelToText(self, output_file):
        w1_m, b1_m, w2_m, b2_m, w3_m, b3_m = self.sess.run([self.w1, self.b1, self.w2, self.b2, self.w3, self.b3])
        f = open(output_file, 'w')
        self.saveOneMatrix(f, w1_m)
        self.saveOneMatrix(f, b1_m)
        self.saveOneMatrix(f, w2_m)
        self.saveOneMatrix(f, b2_m)
        self.saveOneMatrix(f, w3_m)
        self.saveOneMatrix(f, b3_m)
        f.close()


