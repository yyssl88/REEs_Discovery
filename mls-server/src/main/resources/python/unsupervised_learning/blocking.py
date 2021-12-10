import numpy as np
import csv
from pyhanlp import *
from collections import defaultdict
import jieba
import re

csv.field_size_limit(sys.maxsize)

# read csv
def read_csv(filename):
    data = []
    f = open(filename, 'r')
    with f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            data.append(row)
    f.close()
    return data[0], data[1:]


""" Preprocess text """
def clean_sent(sentence):
    sentence = str(sentence).lower() # convert to lower case
    sentence = re.sub(r'\t ', '', sentence)
    sentence = re.sub(' - ','-',sentence) # refit dashes (single words)
    for p in '/+-^*÷#!"(),.:;<=>?@[\]_`{|}~\'¿€$%&£±': # clean punctuation
        sentence = sentence.replace(p,' '+p+' ') # good/bad to good / bad
    sentence = sentence.strip() # strip leading and trailing white space
    #tokenized_sentence = nltk.tokenize.word_tokenize(sentence) # nltk tokenizer
    #tokenized_sentence = [w for w in tokenized_sentence if w in printable] # remove non ascii characters
    return sentence

""" remove all all non alphanumeric and non chinese """
def removeSym(sentence):
    pat_block = u'[^\u4e00-\u9fff0-9a-zA-Z]+';
    pattern = u'([0-9]+{0}[0-9]+)|{0}'.format(pat_block)
    res = re.sub(pattern, lambda x: x.group(1) if x.group(1) else u"", sentence)
    return res

useless_words = {'广东省深圳市福田区', '广东深圳市福田区', '深圳', '深圳市', '福田', '福田区', '街道', '社区', '广东', '广东省'}

useless_words = sorted(useless_words, key=lambda x : len(x), reverse=1)
print(useless_words)

# load JIEBA dictionary
jieba.set_dictionary("../data/address.dict")


def clean(sentence):
    for w in useless_words:
        sentence.replace(w, '')
    return sentence

def gram(sentence, gram_len=3):
    token = []
    for i in range(len(sentence) - gram_len + 1):
        token.append(sentence[i: i + gram_len])
    return token

def word(sentence):
    return sentence.split()

def tokenizer(sentence):
    token = []
    for term in HanLP.segment(sentence):
        if term.word not in useless_words:
            token.append(term.word)
    return token

def jieba_token(sentence):
    token = []
    for term in list(jieba.cut(sentence)):
        if term not in useless_words:
            token.append(term)
    return token

def tokenizers(sentences):
    tokens = []
    for sent in sentences:
        tokens.append(jieba_token(sent))
        #tokens.append(tokenizer(sent))
        #tokens.append(gram(sent))
    return tokens


def blocking(tokens, min_token_len=3, useless_ratio=0.1):
    index = defaultdict(list)
    record_num = len(tokens)
    for rid, token in enumerate(tokens):
        for t in token:
            if len(t) <= min_token_len:
                continue
            index[t].append(rid)
    for k, v in index.items():
        if len(v) > useless_ratio * record_num:
            index[k] = []
    return index


def collectCandidates(index, address):
    pass


data_file = sys.argv[1] #'../data/addresss.csv'
output_dir = sys.argv[2]
schema, data = read_csv(data_file)
data = [removeSym(clean(e[0])) for e in data]
print(data[:10])
#address = address[:100000]
# tokenized
print('Start tokenize ...')
tokens = tokenizers(data)
print(tokens[:100])
print('Start blocking ...')
index = blocking(tokens)

print("The number of blocks is ", len(index))

# collect candidate pair
count = 1
for k, v in index.items():
    # output_file = os.path.join('../data/index/',  str(count) + '.index')
    output_file = os.path.join(output_dir,  str(count)  + '-' + k + '.index')
    count += 1
    f = open(output_file, 'w')
    for e in v:
        # f.write(address[e])
        f.write(str(e))
        f.write('\n')
    f.close()


