import os
from unsupervised_learning.matcher import Matcher
import numpy as np


class ER_Matcher:
    matcher = Matcher()
    TrueLabel = 1

    def set_stopwords(self, stopwords):
        self.matcher.useless_words = stopwords

    def fit_and_predict(self, sentence_pairs):
        clean_pairs = []
        for line in sentence_pairs:
            pair_1, pair_2 = line[0], line[1]
            _pair_1, _pair_2 = self.matcher.clean_sent(pair_1), self.matcher.clean_sent(pair_2)
            _pair_1, _pair_2 = self.matcher.clean(_pair_1), self.matcher.clean(_pair_2)
            _pair_1, _pair_2 = self.matcher.removeUseless(_pair_1), self.matcher.removeUseless(_pair_2)
            _pair_1, _pair_2 = self.matcher.removeSym(_pair_1), self.matcher.removeSym(_pair_2)
            _pair_1, _pair_2 = self.matcher.replaceHanDigitsToDigits(_pair_1), self.matcher.replaceHanDigitsToDigits(_pair_2)
            clean_pairs.append([_pair_1, _pair_2])

        self.matcher.sentence_pairs = clean_pairs
        self.matcher.original_pairs = sentence_pairs

        sim_array = []
        for pair in clean_pairs:
            sim_array.append(self.matcher.calSim(pair[0], pair[1]))
        sim_vectors = np.array(sim_array)

        # qiu 4: 把相似矩阵输入dagmm，得到预测结果，并写入文件
        iftrain = True
        labels = self.matcher.DeepGMM(sim_vectors, os.path.join(self.matcher.model_file_prefix, 'model_step_1'), iftrain=iftrain)

        print('First decision is ', labels)
        # decide which cluster is 1
        if iftrain == True:
            labels, self.TrueLabel = self.matcher.decideTrueLabels(sim_vectors, labels)
            if self.TrueLabel == 0:
                for i in range(len(labels)):
                    labels[i] = 1 - labels[i]

        return labels

    def fit(self, sentence_pairs):
        clean_pairs = []
        for line in sentence_pairs:
            pair_1, pair_2 = line[0], line[1]
            _pair_1, _pair_2 = self.matcher.clean_sent(pair_1), self.matcher.clean_sent(pair_2)
            _pair_1, _pair_2 = self.matcher.clean(_pair_1), self.matcher.clean(_pair_2)
            _pair_1, _pair_2 = self.matcher.removeUseless(_pair_1), self.matcher.removeUseless(_pair_2)
            _pair_1, _pair_2 = self.matcher.removeSym(_pair_1), self.matcher.removeSym(_pair_2)
            _pair_1, _pair_2 = self.matcher.replaceHanDigitsToDigits(_pair_1), self.matcher.replaceHanDigitsToDigits(_pair_2)
            clean_pairs.append([_pair_1, _pair_2])

        self.matcher.sentence_pairs = clean_pairs
        self.matcher.original_pairs = sentence_pairs

        sim_array = []
        for pair in clean_pairs:
            sim_array.append(self.matcher.calSim(pair[0], pair[1]))
        sim_vectors = np.array(sim_array)

        # qiu 4: 把相似矩阵输入dagmm，得到预测结果，并写入文件
        iftrain = True
        labels = self.matcher.DeepGMM(sim_vectors, os.path.join(self.matcher.model_file_prefix, 'model_step_1'), iftrain=iftrain)

        print('First decision is ', labels)
        # decide which cluster is 1

        if iftrain == True:
            labels, self.TrueLabel = self.matcher.decideTrueLabels(sim_vectors, labels)
            if self.TrueLabel == 0:
                for i in range(len(labels)):
                    labels[i] = 1 - labels[i]

        return labels

    def predict(self, sentence_pairs):
        clean_pairs = []
        for line in sentence_pairs:
            pair_1, pair_2 = line[0], line[1]
            _pair_1, _pair_2 = self.matcher.clean_sent(pair_1), self.matcher.clean_sent(pair_2)
            _pair_1, _pair_2 = self.matcher.clean(_pair_1), self.matcher.clean(_pair_2)
            _pair_1, _pair_2 = self.matcher.removeUseless(_pair_1), self.matcher.removeUseless(_pair_2)
            _pair_1, _pair_2 = self.matcher.removeSym(_pair_1), self.matcher.removeSym(_pair_2)
            _pair_1, _pair_2 = self.matcher.replaceHanDigitsToDigits(_pair_1), self.matcher.replaceHanDigitsToDigits(_pair_2)
            clean_pairs.append([_pair_1, _pair_2])

        self.matcher.sentence_pairs = clean_pairs
        self.matcher.original_pairs = sentence_pairs

        sim_array = []
        for pair in clean_pairs:
            sim_array.append(self.matcher.calSim(pair[0], pair[1]))
        sim_vectors = np.array(sim_array)

        labels = self.matcher.model_dagmm.predict_clusters(sim_vectors)

        if self.TrueLabel == 0:
            for i in range(len(labels)):
                labels[i] = 1 - labels[i]
        return labels