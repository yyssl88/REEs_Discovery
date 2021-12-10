from strsimpy.cosine import Cosine
from strsimpy.damerau import Damerau
from strsimpy.jaccard import Jaccard
from strsimpy.jaro_winkler import JaroWinkler
from strsimpy.levenshtein import Levenshtein
from strsimpy.longest_common_subsequence import LongestCommonSubsequence
from strsimpy.metric_lcs import MetricLCS
from strsimpy.ngram import NGram
from strsimpy.normalized_levenshtein import NormalizedLevenshtein
from strsimpy.optimal_string_alignment import OptimalStringAlignment
from strsimpy.qgram import QGram
from strsimpy.sorensen_dice import SorensenDice
from strsimpy.weighted_levenshtein import WeightedLevenshtein
from strsimpy.overlap_coefficient import OverlapCoefficient

import sys
from itertools import combinations

""" Preprocess text """
def clean_sent(sentence):
    sentence = str(sentence).lower() # convert to lower case
    sentence = re.sub(' - ','-',sentence) # refit dashes (single words)
    for p in '/+-^*÷#!"(),.:;<=>?@[\]_`{|}~\'¿€$%&£±': # clean punctuation
        sentence = sentence.replace(p,' '+p+' ') # good/bad to good / bad
    sentence = sentence.strip() # strip leading and trailing white space
    #tokenized_sentence = nltk.tokenize.word_tokenize(sentence) # nltk tokenizer
    #tokenized_sentence = [w for w in tokenized_sentence if w in printable] # remove non ascii characters
    return sentence


useless_words = {'深圳', '深圳市', '福田', '福田区', '街道', '社区', '广东', '广东省'}


def clean(sentence):
    for w in useless_words:
        sentence.replace(w, '')
    return sentence

data_file = sys.argv[0]
output_file = sys.argv[1]

lines = []
corpus_sentences = []

totalnum = 0

'''
# 全集数据
# for line in open("./data/fillresult.txt", 'r', encoding='utf-8'):
for line in open("../source_small.txt", 'r', encoding='utf-8'):
    if(len(line) > 0):
        line = line.replace("\n", "")
        lines.append(line)
        corpus_sentences.append(line.split("\t")[0])
'''

for line in open(data_file, 'r', encoding='utf-8'):
    line = line.replace("\n", "")
    line = clean_sent(line)
    line = clean(line)
    lines.append(line)
    corpus_sentences.append(line)

sim_vector_list = []
id1 = []
id2 = []
for i in range(len(corpus_sentences)):
    sentence1 = corpus_sentences[i]

    for j in range(i, len(corpus_sentences)):
        sim_vector = []

        sentence2 = corpus_sentences[j]

        # 1, cos.similarity
        sim = Cosine(1)
        sim1 = sim.similarity(sentence1, sentence2)
        sim_vector.append(sim1)

        # 2, damerau.distance
        sim = Damerau()
        sim2 = - sim.distance(sentence1, sentence2)
        sim_vector.append(sim2)

        # 3, jaccard.distance
        sim = Jaccard(1)
        sim3 = - sim.distance(sentence1, sentence2)
        sim_vector.append(sim3)

        # 4, jaccard.distance
        sim = JaroWinkler()
        sim4 = sim.similarity(sentence1, sentence2)
        sim_vector.append(sim4)

        # 5, jaroWinkler.similarity
        from .jaro_winkler import JaroWinkler
        sim = JaroWinkler()
        sim5 = sim.similarity(sentence1, sentence2)
        sim_vector.append(sim5)

        # 6, levenshtein.distance
        sim = Levenshtein()
        sim6 = - sim.distance(sentence1, sentence2)
        sim_vector.append(sim6)

        # 7, longestCommonSubsequence.distance
        sim = LongestCommonSubsequence()
        sim7 = - sim.distance(sentence1, sentence2)
        sim_vector.append(sim7)

        # 8, MetricLCS.distance
        sim = MetricLCS()
        sim8 = - sim.distance(sentence1, sentence2)
        sim_vector.append(sim8)

        # 9, NGram distance
        sim = NGram(2)
        sim9 = - sim.distance(sentence1, sentence2)
        sim_vector.append(sim9)

        # 10, NormalizedLevenshtein
        sim = NormalizedLevenshtein()
        sim10 = sim.similarity(sentence1, sentence2)
        sim_vector.append(sim10)

        # 11, NormalizedLevenshtein
        sim = OptimalStringAlignment()
        sim11 = - sim.distance(sentence1, sentence2)
        sim_vector.append(sim11)

        # 12, OverlapCoefficient
        sim = OverlapCoefficient(3)
        sim12 = - sim.distance(sentence1, sentence2)
        sim_vector.append(sim12)

        # 13, QGram
        sim = QGram(1)
        sim13 = - sim.distance(sentence1, sentence2)
        sim_vector.append(sim13)

        # 14, SorensenDice
        sim = SorensenDice(2)
        sim14 = sim.similarity(sentence1, sentence2)
        sim_vector.append(sim14)

        # 15, NormalizedLevenshtein
        sim = WeightedLevenshtein()
        sim15 = - sim.distance(sentence1, sentence2)
        sim_vector.append(sim15)

        sim_vector_list.append(sim_vector)
        id1.append(i)
        id2.append(j)
        # print(j)
        # if j == 1874:
        #     print(" ")


    print(i)


# min-max normalization
from sklearn import preprocessing
sim_vector_list = np.array(sim_vector_list)
min_max_scaler = preprocessing.MinMaxScaler()
sim_vector_list = min_max_scaler.fit_transform(sim_vector_list)

'''
sim_vector_file = open("z.txt", "w", encoding='utf-8')
for sim_vector in sim_vector_list:
    sim_vector_file.write(" ".join(str(x) for x in sim_vector) + "\n")
sim_vector_file.close()
'''


import numpy as np
from sklearn.mixture import GaussianMixture
X = np.loadtxt("z.txt")
gmm = GaussianMixture(n_components=2)
gmm.fit(X)
labels = gmm.predict(X)
# print(labels)

res = []

for i, label in enumerate(labels):
    x, y = id1[i], id2[y]
    res.append([corpus_sentences[x], corpus_sentences[y], str(label)])

f = open(output_file, 'w')
for line in res:
    s = '\t'.join(line)
    f.write(s)
    w.write('\n')


'''
# 确定哪类是相等类
num_0 = 0
num_1 = 0
true_label = 0
for label in labels:
    if label == 0:
        num_0 = num_0 + 1
    else:
        num_1 = num_1 + 1
if num_0 < num_1:
    true_label = 0
else:
    true_label = 1
'''


