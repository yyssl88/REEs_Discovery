import sys
import numpy as np
from REEs_model.parameters import *
import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()


class REEsRepr(object):
    def __init__(self,
                 vob_size,
                 token_embedding_size,
                 hidden_size,
                 rees_embedding_size,
                 max_predicates_lhs,
                 max_predicates_rhs,
                 pretrained_matrix=None):
        self.vob_size = vob_size
        self.token_embedding_size = token_embedding_size
        self.hidden_size = hidden_size
        self.rees_embedding_size = rees_embedding_size
        self.max_predicates_lhs = max_predicates_lhs
        self.max_predicates_rhs = max_predicates_rhs

        # self.tokensEmbeddMatrix = tf.Variable(tf.random_normal([self.vob_size, self.token_embedding_size]), trainable=True)
        self.tokensEmbedMatrix = tf.keras.layers.Embedding(self.vob_size,
                                                           self.token_embedding_size, weights=[pretrained_matrix], trainable=True)
                                                           #input_size=self.max_predicates_lhs * TOKENS_OF_PREDICATE)
        #self.tokensEmbedMatrix.set_weights(pretrained_matrix)
        self.weightPredicates = tf.Variable(tf.random_normal([3, self.token_embedding_size]), trainable=True)

        self.weightREEsEmbeds = tf.Variable(tf.random_normal([self.token_embedding_size * 2, self.rees_embedding_size]), trainable=True)

    # extract used parameters
    def extractParameters(self, sess):
        #dummy_input = np.array([[e] for e in range(self.vob_size)])
        #embeds = self.tokensEmbedMatrix(dummy_input)
        [w2, w3] = sess.run([self.weightPredicates, self.weightREEsEmbeds])
        return w2, w3

    def clauseEmbed_2(self, embeddingsC, predicates_num, actfunc=tf.nn.relu):
        # 1. encode LHS
        # embeddings_lrhs = embeddingsC.reshape(
        embeddings_lrhs = tf.reshape(embeddingsC,
            [-1, predicates_num, TOKENS_OF_PREDICATE, self.token_embedding_size])
        # 1.1 get "t0" and "A"
        embeddings_lrhs_operands1 = embeddings_lrhs[:, :, 0:1, :] + embeddings_lrhs[:, :, 1:2, :]
        # 1.2 get "t1" and "B"
        embeddings_lrhs_operands2 = embeddings_lrhs[:, :, 3:4, :] + embeddings_lrhs[:, :, 4:5, :]
        # 1.3 get operator
        embeddings_lrhs_operator = embeddings_lrhs[:, :, 2:3, :]
        embeddings_lrhs = tf.concat([embeddings_lrhs_operands1, embeddings_lrhs_operands2], axis=2)
        embeddings_lrhs = tf.concat([embeddings_lrhs, embeddings_lrhs_operator], axis=2)
        # embeddings_lrhs:           shape [batch_size, maxPredicatesLHS, 3, tokenEmbeddingSize]
        # predicate_embeddings:     shape [batch_size, maxPredicateLHS, tokenEmbeddingSize]
        predicate_embeddings = actfunc(tf.reduce_mean(tf.multiply(embeddings_lrhs, self.weightPredicates), 2))
        # shape [batch_size, tokenEmbeddingSize]
        embeddings_lrhs = tf.reduce_mean(predicate_embeddings, 1)
        return embeddings_lrhs


    def clauseEmbed(self, embeddingsC, predicates_num, actfunc=tf.nn.relu):
        # 1. encode LHS
        # embeddings_lrhs = embeddingsC.reshape(
        embeddings_lrhs = tf.reshape(embeddingsC,
            [-1, predicates_num, TOKENS_OF_PREDICATE, self.token_embedding_size])
        # 1.1 get "t0" and "A"
        embeddings_lrhs_operands1 = embeddings_lrhs[:, :, 0:1, :] + embeddings_lrhs[:, :, 1:2, :]
        # 1.2 get "t1" and "B"
        embeddings_lrhs_operands2 = embeddings_lrhs[:, :, 3:4, :] + embeddings_lrhs[:, :, 4:5, :]
        # 1.3 get operator
        embeddings_lrhs_operator = embeddings_lrhs[:, :, 2:3, :]
        embeddings_lrhs = tf.concat([embeddings_lrhs_operands1, embeddings_lrhs_operands2], axis=2)
        embeddings_lrhs = tf.concat([embeddings_lrhs, embeddings_lrhs_operator], axis=2)
        # embeddings_lrhs:           shape [batch_size, maxPredicatesLHS, 3, tokenEmbeddingSize]
        # predicate_embeddings:     shape [batch_size, maxPredicateLHS, tokenEmbeddingSize]
        predicate_embeddings = actfunc(tf.reduce_mean(tf.multiply(embeddings_lrhs, self.weightPredicates), 2))
        # shape [batch_size, tokenEmbeddingSize]
        embeddings_lrhs = tf.reduce_mean(predicate_embeddings, 1)
        return embeddings_lrhs

    ''' t0.A = t1.B     =>  [t0, A, =, t1, B]
        ML(t0.A, t1.B)  =>  [t0, A, ML, t1, B]
        t0.A = c        =>  [t0, A, =, t0, c]
    '''
    def encode(self, rees_lhs, rees_rhs, actfunc=tf.nn.relu):
        # shape: [batch_size, maxPredicatesLHS * TOKENS_OF_PREDICATE, tokenEmbeddingSize]
        embeddings_lhs = self.tokensEmbedMatrix(rees_lhs)
        # shape: [batch_size, maxPredicatesRHS * TOKENS_OF_PREDICATE, tokenEmbeddingSize]
        embeddings_rhs = self.tokensEmbedMatrix(rees_rhs)

        # 1. encode LHS
        embeddings_X = self.clauseEmbed(embeddings_lhs, self.max_predicates_lhs)
        # 2. encode RHS
        embeddings_Y = self.clauseEmbed(embeddings_rhs, self.max_predicates_rhs)
        # 3. embedding of rees
        embeddings_rees = tf.concat([embeddings_X, embeddings_Y], axis=1)

        # final REEs embeddings
        embeddings_rees = actfunc(tf.matmul(embeddings_rees, self.weightREEsEmbeds))

        return embeddings_rees

    ''' get token embeddings of all tokens
    '''
    def getTokensEmbeddings(self, tokens):
        tEmbeddings = self.tokensEmbedMatrix(tokens)
        return tEmbeddings 



