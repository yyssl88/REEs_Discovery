import numpy as np
import tensorflow as tf
from official.nlp import optimization

from REEs_reprBert import *

class Interestingness(object):
    def __init__(self, bert_model_name,
                 embed_dim,
                 epochs,
                 learning_rate,
                 steps_per_epochs,
                 batch_size,
                 model_file_path
                 ):
        # here first only consider subjective measures
        self.reesRepr = REEsRepr(bert_model_name, embed_dim)
        #self.rees_input = tf.keras.layers.Input(shape=(None, 2 * (MAX_LHS_PREDICATES + 1)), dtype=tf.string, name='pairs_of_rees')
        self.rees_input = tf.keras.layers.Input(shape=(None, ), dtype=tf.string, name='pairs_of_rees')
        self.epochs = epochs
        self.batch_size = batch_size
        self.steps_per_epochs = steps_per_epochs
        self.learning_rate = learning_rate
        self.interestingness_model = None
        self.model_file_path = model_file_path

    def processREE(self, ree):
        f'''
        :param ree:  str: "p_1 ^ p_2 ... ^ p_m -> p_0"
        :return: [[p_1, p_2, ..., p_{MAX_LHS_PREDICATES}, p_0]
        '''
        print('REE is ', ree)
        [lhs, rhs] = ree.split(LHS_TO_RHS_SYMBOL)
        lhs, rhs = lhs.strip(), rhs.strip()
        lhs_arr = [e.strip() for e in lhs.split(LHS_DELIMITOR_SYMBOL)]
        lhs_arr += [PADDING_VALUE] * (MAX_LHS_PREDICATES - len(lhs_arr))
        lhs_arr.append(rhs)
        return np.array(lhs_arr, 'str')

    def processREEMultiRHS(self, ree):
        [lhs, rhs] = ree.split(LHS_TO_RHS_SYMBOL)
        lhs, rhs = lhs.strip(), rhs.strip()
        # lhs
        lhs_arr = [e.strip() for e in lhs.split(LHS_DELIMITOR_SYMBOL)]
        lhs_arr += [PADDING_VALUE] * (MAX_LHS_PREDICATES - len(lhs_arr))
        # rhs
        rhs_arr = [e.strip() for e in rhs.split(LHS_DELIMITOR_SYMBOL)]
        rhs_arr += [PADDING_VALUE] * (MAX_RHS_PREDICATES - len(rhs_arr))
        ree_arr = lhs_arr + rhs_arr
        return np.array(ree_arr, 'str')

    def processREEs(self, rees):
        '''
        :param rees: an array of string
        :return: rees_lhs [['p_1', ...], ['p_2', ...], ...], rees_rhs: ['p_0', ...]
        '''
        rees_set = []
        for ree in rees:
            #ree_set = self.processREE(ree)
            ree_set = self.processREEMultiRHS(ree)
            rees_set.append(ree_set)
        return np.array(rees_set, 'str')

    def processREEsPairs(self, rees_df):
        rees_1_raw = rees_df['ree_1'].values
        rees_2_raw = rees_df['ree_2'].values
        rees_1 = self.processREEs(rees_1_raw)
        rees_2 = self.processREEs(rees_2_raw)
        return np.concatenate([rees_1, rees_2], axis=1)

    def computeSubjectiveMeasures(self, rees_input_one, final_layer, dropout_layer):
        #bert_embeds = self.reesRepr.encoder(rees_input_one)
        bert_embeds = self.reesRepr.encoder_multRHS(rees_input_one)
        bert_embeds = dropout_layer(bert_embeds)
        interestingness_values = final_layer(bert_embeds) # tf.keras.layers.Dense(1, activation=None, name='regression')(bert_embeds)
        return interestingness_values

    def construct_model(self):
        rees_input_1 = self.rees_input[:, :MAX_LHS_PREDICATES + MAX_RHS_PREDICATES]
        rees_input_2 = self.rees_input[:, MAX_LHS_PREDICATES + MAX_RHS_PREDICATES:]
        print("The shape of rees : ", self.rees_input.shape)
        print("The shape of 1st ree : ", rees_input_1.shape)
        print("The shape of 2nd ree : ", rees_input_2.shape)
        dropout_layer = tf.keras.layers.Dropout(0.1)
        final_layer = tf.keras.layers.Dense(1, activation=None, name='regression')
        interesting_values_1 = self.computeSubjectiveMeasures(rees_input_1, final_layer, dropout_layer)
        interesting_values_2 = self.computeSubjectiveMeasures(rees_input_2, final_layer, dropout_layer)
        predictions = tf.sigmoid(interesting_values_1 - interesting_values_2)
        return tf.keras.Model(self.rees_input, predictions)

    def train(self, rees_data_train, rees_data_valid):
        self.interestingness_model = self.construct_model()
        rees_train = rees_data_train[['ree_1', 'ree_2']]
        rees_train_labels = np.array(rees_data_train['label'].values, 'int')[:,np.newaxis]
        rees_valid = rees_data_valid[['ree_1', 'ree_2']]
        rees_valid_labels = np.array(rees_data_valid['label'].values, 'int')
        rees_train_set = self.processREEsPairs(rees_train)
        rees_valid_set = self.processREEsPairs(rees_valid)
        rees_valid_sets_labels = (rees_valid_set, rees_valid_labels)

        print("The shape of the training instances is {} and {}".format(rees_train_set.shape, rees_train_labels.shape))
        loss = tf.keras.losses.BinaryCrossentropy(from_logits=True)
        metrics = tf.metrics.BinaryAccuracy()
        num_train_steps = self.steps_per_epochs * self.epochs
        num_warmup_steps = int(0.1 * num_train_steps)
        init_lr = self.learning_rate

        optimizer = optimization.create_optimizer(init_lr=init_lr,
                                                  num_train_steps=num_train_steps,
                                                  num_warmup_steps=num_warmup_steps,
                                                  optimizer_type='adamw')
        self.interestingness_model.compile(optimizer=optimizer,
                                      loss=loss,
                                      metrics=metrics)
        print('Train the interestingness model for BERT')
        history = self.interestingness_model.fit(x=rees_train_set,
                                                 y=rees_train_labels,
                                                validation_data=rees_valid_sets_labels,
                                                epochs=self.epochs,
                                                batch_size=self.batch_size)

        # save the model
        self.interestingness_model.save(self.model_file_path, include_optimizer=False)


    def test(self, rees_data_test):
        rees_test = rees_data_test[['ree_1', 'ree_2']]
        rees_test_labels = np.array(rees_data_test['label'].values, 'int')[:, np.newaxis]
        rees_test_set = self.processREEsPairs(rees_test)
        rees_test_sets_labels = (rees_test_set, rees_test_labels)
        #loss, accuracy = self.interestingness_model.evaluate(rees_test_sets_labels)
        loss, accuracy = self.interestingness_model.evaluate(x=rees_test_set, y=rees_test_labels)
        return loss, accuracy


