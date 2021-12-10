import sys
import re
from itertools import combinations
from collections import defaultdict
import numpy as np
from .basic import getListOfFiles, read_csv
from pyhanlp import *
import jieba

useless_words = {'广东省深圳市福田区', '广东深圳市福田区', '深圳', '深圳市', '福田', '福田区', '街道', '社区', '广东', '广东省'}

useless_words = sorted(useless_words, key=lambda x : len(x), reverse=1)
print(useless_words)

# load JIEBA dictionary
jieba.set_dictionary("../data/address.dict")

""" remove all all non alphanumeric and non chinese """
def removeSym(sentence):
    pat_block = u'[^\u4e00-\u9fff0-9a-zA-Z]+';
    pattern = u'([0-9]+{0}[0-9]+)|{0}'.format(pat_block)
    res = re.sub(pattern, lambda x: x.group(1) if x.group(1) else u"", sentence)
    return res

def clean(sentence):
    for w in useless_words:
        sentence.replace(w, '')
    return sentence

def readTrain(data_file):
	schema, data = read_csv(data_file)
	res = []
	for record in data:
		x = record[0].replace('\n', '')
		y = record[1].replace('\n', '')
		res.append(x + '\t' + y)
	return res

def gram(sentence, gram_len=3):
    token = []
    for i in range(len(sentence) - gram_len + 1):
        token.append(sentence[i: i + gram_len])
    return token

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
        # tokens.append(jieba_token(sent))
        tokens.append(tokenizer(sent))
        #tokens.append(gram(sent))
    return tokens


def blocking(tokens, key_t, min_token_len=2, useless_ratio=0.1):
    index = defaultdict(list)
    record_num = len(tokens)
    for rid, token in enumerate(tokens):
        for t in token:
            if (key_t in t) or (t in key_t):
        	    continue
            if len(t) <= min_token_len:
                continue
            index[t].append(rid)
    for k, v in index.items():
        if len(v) > useless_ratio * record_num:
            index[k] = []
    return index

def is_Chinese(ch):
    # only solve chinese text
    isCN = True
    for e in ch:
        #print(e)
        if not (e >= "\u4e00" and e <= "\u9fff"):
            isCN = False
    return isCN

BLOCK_SEP = '-'
maxlines=20

data_file = sys.argv[1]             # the original dataset
block_dir = sys.argv[2]             # blocking files directory
block_start_id = int(sys.argv[3]) 	# block ID start
block_end_id = int(sys.argv[4])		# block ID end
minIntersection = float(sys.argv[5]) #int(sys.argv[5])  # the minimal intersection size
output_file = sys.argv[6]			# candidate pairs

# block_dir = '../data/index/'
# 1. generate all blocks needed to read

all_paths, all_files = getListOfFiles(block_dir, suffix='.index')

block_paths = defaultdict()
for p, f in zip(all_paths, all_files):
	block_id = int(f.split(BLOCK_SEP)[0])
	block_name = f.split(BLOCK_SEP)[1]
	if not is_Chinese(block_name):
		continue
	if block_id >= block_start_id and block_id <= block_end_id:
		block_paths[block_name] = p

print('The number of blocks is ', len(block_paths))

# read data
schema, data = read_csv(data_file)
data = [e[0] for e in data]
data_clean = [removeSym(clean(e[0])) for e in data]
tokens = tokenizers(data_clean)
print('The number of data is ', len(data))
print(data[:10])
# 2. read all data from each path

block_datas = defaultdict(list)
for block_name, path in block_paths.items():
	_data = []
	for line in open(path, 'r', encoding='utf-8'):
		_line = line.replace("\n", "")
		_data.append(_line.strip())
	block_datas[block_name] = _data #[:maxlines]

# 3. prepare data
block_pairs = []
for i in range(len(data)):
    ss = defaultdict(int)
    block_pairs.append(ss)

print('The number of entries is ', len(block_pairs))
for block_name, ids in block_datas.items():
    ids_array = [int(e) for e in ids]
    for id_pair in list(combinations(ids_array, 2)):
        id1, id2 = id_pair[0], id_pair[1]
        # print(id1, id2)
        block_pairs[id1][id2] += 1

    '''
    # reconstruct blocks
    tokens = []
    for record in data:
        tokens.append(tokenizer(record))
    blockers = blocking(tokens, block_name)
    for k, _list in blockers.items():
        list_ = _list[:maxlines]
        ylen = len(list_)
        for id_pair in list(combinations(np.arange(ylen), 2)):
            block_pairs.append([data[id_pair[0]], data[id_pair[1]]])
    '''

final_pairs = []
for rid_1, _dict in enumerate(block_pairs):
    for rid_2, c in _dict.items():
        len1, len2 = len(tokens[rid_1]), len(tokens[rid_2])
        if (len1 + len2 - c) <= 0:
            continue
        jacc = c * 1.0 / (len1 + len2 - c)
        # if c >= minIntersection:
        if jacc >= minIntersection:
            final_pairs.append([rid_1, rid_2])

'''
# 4. read original file
original_pairs = readTrain('../data/train/addrs.csv')
'''

# 4. save
f = open(output_file, 'w')
for data_pair in final_pairs:
	s = data[data_pair[0]] + '\t' + data[data_pair[1]]
	f.write(s)
	f.write('\n')
'''
for s in original_pairs:
	f.write(s)
	f.write('\n')
'''
f.close()

