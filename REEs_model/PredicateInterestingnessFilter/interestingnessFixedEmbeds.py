import numpy as np
import pandas as pd
import sys
sys.path.append('../../REEs_model')
from collections import defaultdict
import time
from REEs_model.parameters import *
from REEs_model.parameters import __eval__, MAX_LHS_PREDICATES, MAX_RHS_PREDICATES
from REEs_model.REEs_repr import REEsRepr
import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()


class InterestingnessEmbeds(object):
    def __init__(self,
                 vob_size,
                 token_embedding_size,
                 hidden_size,
                 rees_embedding_size,
                 max_predicates_lhs,
                 max_predicates_rhs,
                 lr,
                 epochs,
                 batch_size):
        # setup rule representation
        self.reesRepr = REEsRepr(vob_size,
                                 token_embedding_size,
                                 hidden_size,
                                 rees_embedding_size,
                                 max_predicates_lhs,
                                 max_predicates_rhs)

        # interestingness weights
        self.weight_interest = tf.Variable(tf.random_normal([rees_embedding_size, 1]), trainable=True)
        self.learning_rate = lr
        self.epochs = epochs
        self.batch_size = batch_size
        self.vob_size = vob_size

        # construct the model
        self.construct()
        # initialize all variables
        init_op = tf.global_variables_initializer()
        self.saver = tf.train.Saver()
        # open a session and run the training graph
        session_config = tf.ConfigProto(log_device_placement=True)
        session_config.gpu_options.allow_growth = True

        self.sess = tf.Session(config=session_config)

        self.saver = tf.train.Saver()
        self.sess.run(init_op)


    def saveOneMatrix(self, fout, matrix):
        if len(matrix.shape) <= 1:
            fout.write(str(matrix.shape[0]))
        else:
            fout.write(str(matrix.shape[0]) + " " + str(matrix.shape[1]))
        fout.write("\n")
        for i in range(matrix.shape[0]):
            fout.write(' '.join([str(e) for e in matrix[i]]))
            fout.write("\n")
        fout.write("\n")

    def saveModelToText(self, model_txt_path):
        token_ph = tf.placeholder(dtype=tf.int32, shape=[1, self.vob_size], name='all_tokens_input')
        tEmbeddings = self.reesRepr.getTokensEmbeddings(token_ph)[0]
        w2, w3 = self.reesRepr.extractParameters(self.sess)
        dummy_input = np.array([[e for e in range(self.vob_size)]])
        w1, w4 = self.sess.run([tEmbeddings, self.weight_interest], feed_dict={token_ph: dummy_input})
        w1 = np.array(w1)
        w2 = np.array(w2)
        w3 = np.array(w3)
        w4 = np.array(w4)
        print(w1.shape, w2.shape, w3.shape, w4.shape)
        f = open(model_txt_path, 'w')
        self.saveOneMatrix(f, np.array(w1))        
        self.saveOneMatrix(f, np.array(w2))        
        self.saveOneMatrix(f, np.array(w3))        
        self.saveOneMatrix(f, np.array(w4))        
        f.close()

    def saveModel(self, model_path):
        self.saver.save(self.sess, model_path)

    def loadModel(self, model_path):
        self.saver.restore(self.sess, model_path)

    def inference_classification(self, interestingness_1, interestingness_2):
        interestingness_logits = tf.concat([interestingness_1, interestingness_2], 1)
        predictions = tf.nn.softmax(interestingness_logits)
        return interestingness_logits, predictions

    def loss_compute(self, predictions, GT):
        # loss = tf.reduce_sum(tf.losses.mean_squared_error(labels=GT,predictions=prediction))
        # loss
        # cross_entropy = tf.reduce_mean(-tf.reduce_sum(GT * tf.log(predictions), reduction_indices=[1]))
        cross_entropy = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=predictions, labels=GT))
        return cross_entropy

    def construct(self):
        # token ids of tokeVob
        self.lhs_vec_ph_left = tf.placeholder(dtype=tf.int32,
                                                       shape=[None, MAX_LHS_PREDICATES * TOKENS_OF_PREDICATE],
                                                       name='LHS_vec_ph_left')
        self.rhs_vec_ph_left = tf.placeholder(dtype=tf.int32,
                                                       shape=[None, MAX_RHS_PREDICATES * TOKENS_OF_PREDICATE],
                                                       name='RHS_vec_ph_left')
        self.lhs_vec_ph_right = tf.placeholder(dtype=tf.int32,
                                                        shape=[None, MAX_LHS_PREDICATES * TOKENS_OF_PREDICATE],
                                                        name='LHS_vec_ph_right')
        self.rhs_vec_ph_right = tf.placeholder(dtype=tf.int32,
                                                        shape=[None, MAX_RHS_PREDICATES * TOKENS_OF_PREDICATE],
                                                        name='RHS_vec_ph_right')
        self.label_ph = tf.placeholder(dtype=tf.float32, shape=[None, 2],
                                                name='label')

        # construct the rule interestingness model
        ree_embed_left = self.reesRepr.encode(self.lhs_vec_ph_left, self.rhs_vec_ph_left)
        ree_embed_right = self.reesRepr.encode(self.lhs_vec_ph_right, self.rhs_vec_ph_right)
        self.interestingness_left = tf.matmul(ree_embed_left, self.weight_interest)
        self.interestingness_right = tf.matmul(ree_embed_right, self.weight_interest)

        # predictions
        self.logits, self.predictions = self.inference_classification(self.interestingness_left, self.interestingness_right)
        self.loss = self.loss_compute(self.predictions, self.label_ph)
        self.optimizer = tf.train.AdamOptimizer(learning_rate=self.learning_rate)
        #self.optimizer = tf.keras.optimizers.Adam(learning_rate=self.learning_rate)
        self.train_op = self.optimizer.minimize(self.loss)
        # accuracy
        correct_predictions = tf.equal(tf.argmax(self.predictions, 1), tf.argmax(self.label_ph, 1))
        self.accuracy_tf = tf.reduce_mean(tf.cast(correct_predictions, 'float'))


    def generate_batch(self, batch_id, batch_size, rees_lhs, rees_rhs, train_pair_ids, train_labels):
        train_num = len(train_pair_ids)
        start_id = batch_size * batch_id
        end_id = batch_size * (batch_id + 1)
        batch_train_pair_ids = train_pair_ids[start_id: end_id]
        batch_train_labels = train_labels[start_id: end_id]
        # generate real training data
        batch_lhs_left = [rees_lhs[e[0]] for e in batch_train_pair_ids]
        batch_rhs_left = [rees_rhs[e[0]] for e in batch_train_pair_ids]
        batch_lhs_right = [rees_lhs[e[1]] for e in batch_train_pair_ids]
        batch_rhs_right = [rees_rhs[e[1]] for e in batch_train_pair_ids]
        return np.array(batch_lhs_left, 'int'), np.array(batch_rhs_left, 'int'), \
                np.array(batch_lhs_right, 'int'), np.array(batch_rhs_right, 'int'), batch_train_labels

    def compute_interestingness(self, rees_lhs, rees_rhs):
        num_batch = len(rees_lhs) // self.batch_size + 1
        interestingness_values = []
        for batch_id in range(num_batch):
            # fetch data
            start_id, end_id = self.batch_size * batch_id, self.batch_size * (batch_id + 1)
            batch_rees_lhs, batch_rees_rhs = rees_lhs[start_id: end_id], rees_rhs[start_id: end_id]
            feed_dict_interest = {self.lhs_vec_ph_left: batch_rees_lhs,
                                  self.rhs_vec_ph_left: batch_rees_rhs}
            batch_interestingness = self.sess.run(self.interestingness_left, feed_dict_interest)
            if len(batch_interestingness) > 0:
                interestingness_values += list(np.hstack(batch_interestingness))
        return interestingness_values[:len(rees_lhs)]

    def evaluate(self, rees_lhs, rees_rhs, test_pair_ids, test_labels):
        start_time = time.time()
        test_lhs_left, test_rhs_left, test_lhs_right, test_rhs_right, test_labels_ = self.generate_batch(
            0, len(rees_lhs), rees_lhs, rees_rhs, test_pair_ids, test_labels
        )
        feed_dict_test = {self.lhs_vec_ph_left: test_lhs_left,
                          self.rhs_vec_ph_left: test_rhs_left,
                          self.lhs_vec_ph_right: test_lhs_right,
                          self.rhs_vec_ph_right: test_rhs_right
        }
        test_predictions = self.sess.run(self.predictions, feed_dict=feed_dict_test)
        predict_time = time.time() - start_time
        test_measurements = __eval__(np.argmax(test_predictions, 1), np.argmax(test_labels_, 1))
        test_log = f'test_acc: {test_measurements[0]}, test_recall: {test_measurements[1]}, ' \
                    f'test_precision: {test_measurements[2]}, test_f1: {test_measurements[3]}, test_time: {predict_time} '
        print(test_log)


    def train(self, rees_lhs, rees_rhs, train_pair_ids, train_labels, valid_pair_ids, valid_labels):
        print('start training...')

        start_total = 0  # time.time()
        for epoch in range(self.epochs):
            start_train = time.time()
            ## Generate Training Batch
            num_batch = len(train_pair_ids) // self.batch_size + 1
            for batch_id in range(num_batch):
                batch_lhs_left, batch_rhs_left, batch_lhs_right, batch_rhs_right, train_batch_labels = self.generate_batch(
                    batch_id,
                    self.batch_size, rees_lhs, rees_rhs, train_pair_ids, train_labels)
                feed_dict_train = {self.lhs_vec_ph_left: batch_lhs_left,
                                   self.rhs_vec_ph_left: batch_rhs_left,
                                   self.lhs_vec_ph_right: batch_lhs_right,
                                   self.rhs_vec_ph_right: batch_rhs_right,
                                   self.label_ph: train_batch_labels}

                _, batch_train_predictions, loss_epoch = self.sess.run([self.train_op, self.predictions, self.loss], feed_dict=feed_dict_train)
                end_train = time.time()
                start_total += end_train - start_train
                # print(f'epoch {epoch}, mean batch loss: {loss_epoch}, acc: {measurements[0]}, recall: {measurements[1]}, precision: {measurements[2]}, f1: {measurements[3]}, time cost: {end_train - start_train}')
                train_measurements = __eval__(np.argmax(batch_train_predictions, 1), np.argmax(train_batch_labels, 1))
                log = f'epoch {epoch}, train_loss: {loss_epoch}, train_acc: {train_measurements[0]}, train_recall: {train_measurements[1]}, ' \
                      f'train_precision: {train_measurements[2]}, train_f1: {train_measurements[3]}, time: {end_train - start_train} '
                print(log)

            valid_batch_lhs_left, valid_batch_rhs_left, valid_batch_lhs_right, valid_batch_rhs_right, valid_batch_labels = self.generate_batch(
                    0, len(valid_pair_ids), rees_lhs, rees_rhs, valid_pair_ids, valid_labels)
            feed_dict_valid = {self.lhs_vec_ph_left: valid_batch_lhs_left,
                                   self.rhs_vec_ph_left: valid_batch_rhs_left,
                                   self.lhs_vec_ph_right: valid_batch_lhs_right,
                                   self.rhs_vec_ph_right: valid_batch_rhs_right,
                                   self.label_ph: valid_batch_labels}
            valid_predictions = self.sess.run(self.predictions, feed_dict=feed_dict_valid)
            valid_measurements = __eval__(np.argmax(valid_predictions, 1), np.argmax(valid_batch_labels, 1))
            valid_log = f'epoch {epoch}, valid_acc: {valid_measurements[0]}, valid_recall: {valid_measurements[1]}, ' \
                    f'valid_precision: {valid_measurements[2]}, valid_f1: {valid_measurements[3]} '
            print(valid_log)




