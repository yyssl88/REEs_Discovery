import logging
import os
import re
import string
import time
from collections import defaultdict

import numpy as np
import tensorflow as tf

# sys.path.append('dagmm/dagmm.py')
from unsupervised_learning.dagmm import DAGMM
from unsupervised_learning.strsimpy.cosine import Cosine
from unsupervised_learning.strsimpy.damerau import Damerau
from unsupervised_learning.strsimpy.jaccard import Jaccard
from unsupervised_learning.strsimpy.jaro_winkler import JaroWinkler
from unsupervised_learning.strsimpy.levenshtein import Levenshtein
from unsupervised_learning.strsimpy.longest_common_subsequence import LongestCommonSubsequence
from unsupervised_learning.strsimpy.metric_lcs import MetricLCS
from unsupervised_learning.strsimpy.ngram import NGram
from unsupervised_learning.strsimpy.normalized_levenshtein import NormalizedLevenshtein
from unsupervised_learning.strsimpy.optimal_string_alignment import OptimalStringAlignment
from unsupervised_learning.strsimpy.overlap_coefficient import OverlapCoefficient
from unsupervised_learning.strsimpy.qgram import QGram
from unsupervised_learning.strsimpy.sorensen_dice import SorensenDice
from unsupervised_learning.strsimpy.weighted_levenshtein import WeightedLevenshtein

class Matcher:
    # qiu 1: 定义全局变量

    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '0'
    logging.getLogger("tensorflow").setLevel(logging.WARNING)

    useless_words = {'深圳', '深圳市', '福田', '福田区', '街道', '社区', '广东', '广东省'}

    CN_NUM = {u'〇': 0, u'一': 1, u'二': 2, u'三': 3, u'四': 4, u'五': 5, u'六': 6, u'七': 7, u'八': 8, u'九': 9,
              u'零': 0, u'壹': 1, u'贰': 2, u'叁': 3, u'肆': 4, u'伍': 5, u'陆': 6, u'柒': 7, u'捌': 8, u'玖': 9, u'貮': 2, u'两': 2}

    lower_letters = string.ascii_lowercase

    sim_vectors = []

    sentence_pairs = []
    original_pairs = []

    first_labels = []

    model_file_prefix = './models/'
    output_file = './result/result.txt'

    # qiu 2: 读入配置，是否使用数字匹配
    TrueLabelStepOne = 1
    TrueLabelStepTwo = 1

    idx_refined = []
    sentence_pairs_refined = []

    model_dagmm = None


    def clean_sent(self, sentence):
        sentence = str(sentence).lower()  # convert to lower case
        sentence = sentence.replace("", "")  # remove space
        sentence = sentence.replace("\n", "")  # remove '\n'
        sentence = sentence.replace("\t", "")  # remove '\t'
        sentence = re.sub(r'\t ', '', sentence)
        sentence = re.sub(' - ', '-', sentence)  # refit dashes (single words)
        for p in '/+-^*÷#!"(),.:;<=>?@[\]_`{|}~\'¿€$%&£±':  # clean punctuation
            sentence = sentence.replace(p, ' ' + p + ' ')  # good/bad to good / bad
        sentence = sentence.strip()  # strip leading and trailing white space
        # tokenized_sentence = nltk.tokenize.word_tokenize(sentence) # nltk tokenizer
        # tokenized_sentence = [w for w in tokenized_sentence if w in printable] # remove non ascii characters
        return sentence

    """ remove all all non alphanumeric and non chinese """
    def removeSym(self, sentence):
        pat_block = u'[^\u4e00-\u9fff0-9a-zA-Z]+';
        pattern = u'([0-9]+{0}[0-9]+)|{0}'.format(pat_block)
        res = re.sub(pattern, lambda x: x.group(1) if x.group(1) else u"", sentence)
        return res


    def clean(self, sentence):
        for w in self.useless_words:
            sentence = sentence.replace(w, '')
        return sentence


    def removeUseless(self, sentence):
        # check whether contain '( ... )'
        sentence = re.sub(r'\((.)+\)', '', sentence)
        sentence = re.sub(r'（(.)+）', '', sentence)
        # check whether contain telephone number
        sentence = re.sub(r'\d{11}', '', sentence)
        return sentence


    def replaceHanDigitsToDigits(self, sentence):
        s = ''
        for e in sentence:
            if e in self.CN_NUM:
                s += str(self.CN_NUM[e])
            else:
                s += e
        return s


    def keepOnlyLettersDigits(self, sentence):
        s = ''
        for e in sentence:
            if e.isdigit() or e in self.lower_letters:
                s += e
        return s


    def keepOnlyLettersDigitsImproved(self, sentence, largeseqnlen=8):
        s = ''
        hip = '-'
        for i, e in enumerate(list(sentence)):
            if e.isdigit() or e in self.lower_letters:
                s += e
                if i + 1 < len(sentence) and sentence[i + 1].isdigit() == False and sentence[i + 1] not in self.lower_letters:
                    s += hip

        # remove large sequence of
        ss = s.split(hip)
        s = ''
        for e in ss:
            if len(e) >= largeseqnlen or len(e) == 0:
                continue
            else:
                s += e + hip

        if len(s) == 0:
            return s

        if s[-1] == hip:
            s = s[:-1]
        # print(sentence, s)
        return s


    ''' load dataset
    '''


    def load_dataset(self, data_file):
        sentence_pairs = []
        line_pairs = []
        for line in open(data_file, 'r', encoding='utf-8'):
            pair_1, pair_2 = line.split("\t")[0], line.split("\t")[1]
            pair_1, pair_2 = pair_1.replace("\n", ""), pair_2.replace("\n", "")
            _pair_1, _pair_2 = self.clean_sent(pair_1), self.clean_sent(pair_2)
            _pair_1, _pair_2 = self.clean(_pair_1), self.clean(_pair_2)
            _pair_1, _pair_2 = self.removeUseless(_pair_1), self.removeUseless(_pair_2)
            _pair_1, _pair_2 = self.removeSym(_pair_1), self.removeSym(_pair_2)
            _pair_1, _pair_2 = self.replaceHanDigitsToDigits(_pair_1), self.replaceHanDigitsToDigits(_pair_2)
            line_pairs.append([pair_1, pair_2])
            sentence_pairs.append([_pair_1, _pair_2])
        return sentence_pairs, line_pairs
        # return sentence_pairs[:1000], line_pairs[:1000]

    def load_dataset(self, data_file, block_id, block_num):
        sentence_pairs = []
        line_pairs = []
        for line in open(data_file, 'r', encoding='utf-8'):
            pair_1, pair_2 = line.split("\t")[0], line.split("\t")[1]
            pair_1, pair_2 = pair_1.replace("\n", ""), pair_2.replace("\n", "")
            line_pairs.append([pair_1, pair_2])
        numP = len(line_pairs)
        block_len = int(numP * 1.0 / block_num)
        block_start, block_end = block_id * block_len, (block_id + 1) * block_len
        line_pairs = line_pairs[block_start: block_end]
        for pairs in line_pairs:
            pair_1, pair_2 = pairs[0], pairs[1]
            _pair_1, _pair_2 = self.clean_sent(pair_1), self.clean_sent(pair_2)
            _pair_1, _pair_2 = self.clean(_pair_1), self.clean(_pair_2)
            _pair_1, _pair_2 = self.removeUseless(_pair_1), self.removeUseless(_pair_2)
            _pair_1, _pair_2 = self.removeSym(_pair_1), self.removeSym(_pair_2)
            _pair_1, _pair_2 = self.replaceHanDigitsToDigits(_pair_1), self.replaceHanDigitsToDigits(_pair_2)
            sentence_pairs.append([_pair_1, _pair_2])
        return sentence_pairs, line_pairs
        # return sentence_pairs[:1000], line_pairs[:1000]


    def extractFreqWords(self, data, top=100):
        hist = defaultdict(int)
        for sent in data:
            for w in set(sent):
                if w.isdigit() or w in self.lower_letters:
                    continue
                hist[w] += 1
        freq = [[w, f] for w, f in hist.items()]
        freq = sorted(freq, key=lambda x: x[1], reverse=1)
        return freq[:top]


    def keepUsefulFreqWords(self, data, freq_, ratio=0.5):
        freq = defaultdict(int)
        hist = defaultdict(int)
        for w in freq_:
            hist[w[0]] = 0
            freq[w[0]] = w[1]
        for sent in data:
            for w in freq.keys():
                wpos = sent.find(w)
                if wpos > 0:
                    if sent[wpos - 1:wpos].isdigit() or sent[wpos - 1:wpos] in self.lower_letters:
                        hist[w] += 1
        res = []
        for w, f in hist.items():
            if f >= ratio * freq[w]:
                res.append([w, f])
        return res


    def extractDigitsORLettersBeforeToken(self, sentence, kw):
        wpos = sentence.find(kw)
        res = ""
        if wpos > 0:
            sc = wpos - 1
            while sc >= 0 and (sentence[sc:sc + 1].isdigit() or sentence[sc:sc + 1] in self.lower_letters):
                res = sentence[sc:sc + 1] + res
                sc -= 1
        return res


    def extractLastDigits(self, sentence):
        res = ""
        if len(sentence) >= 0:
            sc = len(sentence) - 1
            while sc >= 0 and (sentence[sc:sc + 1].isdigit() or sentence[sc:sc + 1] in self.lower_letters):
                res = sentence[sc:sc + 1] + res
                sc -= 1
        return res


    def generateFeatures(self, sentence, keywords):
        feas = []
        f_1 = self.keepOnlyLettersDigits(sentence)
        feas.append(f_1)
        feas.append(self.extractLastDigits(sentence))
        for kw in keywords:
            feas.append(self.extractDigitsORLettersBeforeToken(sentence, kw))
        return feas


    def calSimRefine(self, sentence1, sentence2, sim_num=2):
        if len(sentence1) == 0 or len(sentence2) == 0:
            return [0] * sim_num

        sim_vector = []
        # 6, levenshtein.distance
        sim = Levenshtein()
        sim6 = - sim.distance(sentence1, sentence2)
        sim_vector.append(sim6)

        # 7, longestCommonSubsequence.distance
        sim = LongestCommonSubsequence()
        sim7 = - sim.distance(sentence1, sentence2)
        sim_vector.append(sim7)

        return sim_vector


    def calSimRefine_(self, sentence_vec1, sentence_vec2):
        sim_vector = []

        simED = Levenshtein()
        simLCS = LongestCommonSubsequence()
        simJW = JaroWinkler()
        simSA = OptimalStringAlignment()
        for token1, token2 in zip(sentence_vec1, sentence_vec2):
            if len(token1) == 0 and len(token2) == 0:
                sim_vector += [1.0, 1.0, 1.0, 1.0]
                continue
            if len(token1) == 0 or len(token2) == 0:
                sim_vector += [0.0, 0.0, 0.0, 0.0]
                # sim_vector += [0.0]
                continue
            sim6 = 1.0 - (simED.distance(token1, token2)) / max(len(token1), len(token2))
            # sim6 = simED.distance(token1, token2)
            sim_vector.append(sim6)

            sim4 = simJW.similarity(token1, token2)
            sim_vector.append(sim4)

            # sim7 = simLCS.distance(token1, token2)
            sim7 = 1.0 - (simLCS.distance(token1, token2) * 1.0 / (len(token1) + len(token2)))
            sim_vector.append(sim7)

            # sim11 = simSA.distance(token1, token2)
            sim11 = 1.0 - (simSA.distance(token1, token2) * 1.0 / max(len(token1), len(token2)))
            sim_vector.append(sim11)
        return sim_vector


    def calSim(self, sentence1, sentence2, sim_num=16):
        if len(sentence1) == 0 or len(sentence2) == 0:
            return [0] * sim_num
        sim_vector = []
        # 1, cos.similarity
        sim = Cosine(1)
        sim1 = sim.similarity(sentence1, sentence2)
        sim_vector.append(sim1)

        # 2, damerau.distance
        sim = Damerau()
        sim2 = 1.0 - (sim.distance(sentence1, sentence2) / max(len(sentence1), len(sentence2)))
        # sim2 = sim.distance(sentence1, sentence2)
        sim_vector.append(sim2)

        # 3, jaccard.distance
        sim = Jaccard(2)
        sim3 = sim.similarity(sentence1, sentence2)
        sim_vector.append(sim3)

        # 4, OverlapCoefficient
        sim = OverlapCoefficient(3)
        sim4 = sim.similarity(sentence1, sentence2)
        sim_vector.append(sim4)

        # 5, jaroWinkler.similarity
        # from .jaro_winkler import JaroWinkler
        sim = JaroWinkler()
        sim5 = sim.similarity(sentence1, sentence2)
        sim_vector.append(sim5)

        # 6, levenshtein.distance
        sim = Levenshtein()
        # sim6 = sim.distance(sentence1, sentence2)
        sim6 = 1.0 - (sim.distance(sentence1, sentence2) * 1.0 / max(len(sentence1), len(sentence2)))
        sim_vector.append(sim6)

        # 7, longestCommonSubsequence.distance
        sim = LongestCommonSubsequence()
        sim7 = 1.0 - (sim.distance(sentence1, sentence2) * 1.0 / (len(sentence1) + len(sentence2)))
        # sim7 = sim.distance(sentence1, sentence2)
        sim_vector.append(sim7)

        # 8, MetricLCS.distance
        sim = MetricLCS()
        # sim8 = sim.distance(sentence1, sentence2)
        sim8 = 1.0 - sim.distance(sentence1, sentence2)
        sim_vector.append(sim8)

        # 9, NGram distance
        sim = NGram(2)
        # sim9 = sim.distance(sentence1, sentence2)
        sim9 = 1.0 - sim.distance(sentence1, sentence2)
        sim_vector.append(sim9)

        # 10, NormalizedLevenshtein
        sim = NormalizedLevenshtein()
        sim10 = sim.similarity(sentence1, sentence2)
        sim_vector.append(sim10)

        # 11, NormalizedLevenshtein
        sim = OptimalStringAlignment()
        # sim11 = sim.distance(sentence1, sentence2)
        sim11 = 1.0 - (sim.distance(sentence1, sentence2) * 1.0 / max(len(sentence1), len(sentence2)))
        sim_vector.append(sim11)

        # 12, OverlapCoefficient
        sim = OverlapCoefficient(2)
        # print(sentence1, sentence2)
        sim12 = sim.similarity(sentence1, sentence2)
        # sim12 = 1.0 -  ( sim.distance(sentence1, sentence2) * 1.0 / max(len(sentence1), len(sentence2)) )
        sim_vector.append(sim12)

        # 13, QGram
        sim = QGram(2)
        # sim13 = sim.distance(sentence1, sentence2)
        sim13 = 1.0 - (sim.distance(sentence1, sentence2) * 1.0 / max(len(sentence1), len(sentence2)))
        sim_vector.append(sim13)

        # 14, SorensenDice
        sim = SorensenDice(2)
        sim14 = sim.similarity(sentence1, sentence2)
        sim_vector.append(sim14)

        # 15, NormalizedLevenshtein
        sim = WeightedLevenshtein()
        # sim15 = sim.distance(sentence1, sentence2)
        sim15 = 1.0 - (sim.distance(sentence1, sentence2) * 1.0 / max(len(sentence1), len(sentence2)))
        sim_vector.append(sim15)

        # 16, OverlapCoefficient
        sim = OverlapCoefficient(1)
        sim16 = sim.similarity(sentence1, sentence2)
        sim_vector.append(sim16)

        return sim_vector


    def generate_simvectors(self, corpus_sentences):
        sim_vector_list = []
        id1 = []
        id2 = []
        for i in range(len(corpus_sentences)):
            sentence1 = corpus_sentences[i]
            if sentence1.strip() == "":
                continue

            for j in range(i + 1, len(corpus_sentences)):
                sim_vector = []

                sentence2 = corpus_sentences[j]
                if sentence2.strip() == "":
                    continue

                sim_vector_list.append(self.calSim(sentence1, sentence2))
                id1.append(i)
                id2.append(j)
                # print(j)
                # if j == 1874:
                #     print(" ")

            print(i)
        return sim_vector_list, id1, id2


    # for train
    def decideTrueLabels(self, X, labels):
        sc_0 = [i for i, e in enumerate(labels) if e == 0]
        sc_1 = [i for i, e in enumerate(labels) if e == 1]
        avg_0 = np.mean(X[sc_0])
        avg_1 = np.mean(X[sc_1])
        # print(avg_0, avg_1)
        if avg_0 > avg_1:
            _labels = labels * -1 + 1
            return _labels, 0
        return labels, 1


    # for inference
    def selectTrueLabels(self, labels, TrueLabel):
        if TrueLabel == 1:
            return labels
        _labels = labels * -1 + 1
        return _labels

    # qiu: 这里model_dagmm.fit(X)和model_dagmm.predict_clusters(X)需要分开，训练和预测的数据可以是不同数据
    # qiu: 这里是批量输入进行预测；需要支持单个句子对进行预测，并且不需要重新加载模型
    def DeepGMM(self, sim_vectors, model_file, iftrain=False, tau=0):
        X = sim_vectors

        if iftrain:
            tf.reset_default_graph()
            self.model_dagmm = DAGMM(
                comp_hiddens=[64, 32, 4], comp_activation=tf.nn.tanh,
                est_hiddens=[64, 32, 2], est_activation=tf.nn.tanh, est_dropout_ratio=0.25,
                epoch_size=2000, minibatch_size=256
            )
            self.model_dagmm.fit(X)
            # save model
            self.model_dagmm.save(model_file)
        else:
            if self.model_dagmm is None:
                self.model_dagmm.restore(model_file)

        # predict clusters
        labels = self.model_dagmm.predict_clusters(X)

        '''
        # inference
        energy = model_dagmm.predict(X)
        labels = []
        for e in energy:
            if e >= tau:
                labels.append(1)
            else:
                labels.append(0)
        '''
        return labels


    def printHigh(self, sim_vecs, labels):
        print('Similarity vectors of label 1 are ...')
        for vec, label in zip(sim_vecs, labels):
            if label == 1:
                print(vec)


# m = Matcher()
#
# m.fit()
# m.fit_digits()
# m.save_result()