import numpy as np
import sys
from scipy.spatial.distance import cosine
from itertools import combinations
from basic import *
import os

cluster_ids_file = sys.argv[1]
embedding_file = sys.argv[2]
embedding_ids_file = sys.argv[3]
sample_ratio = float(sys.argv[4])
original_data_dir = sys.argv[5]
sample_data_dir = sys.argv[6]

# load clusters
clusters = []
for line in open(cluster_ids_file):
	ids = [e for e in line.split()]
	clusters.append(ids)

cluster_num = len(clusters)

#print(clusters)

# record scores
scores_all = []

# load embeddings
embeddings = np.load(embedding_file)
tids = np.load(embedding_ids_file)

tids_hash = {}
for i, tid in enumerate(tids):
	tids_hash[tid] = i


def rank(cluster, tids_hash, embeddings):
	embeds = [embeddings[tids_hash[e]] for e in cluster]
	_idx = np.arange(len(cluster))
	coms = combinations(_idx, 2)
	scores = [0] * len(cluster)
	for com in coms:
		if com[0] == com[1]:
			continue
		cos = cosine(embeds[com[0]], embeds[com[1]])
		scores[com[0]] += cos
		scores[com[1]] += cos
	rank = [[i, j] for i, j in zip(cluster, scores)]
	rank = sorted(rank, key = lambda x : x[1], reverse=1)
	return [e[0] for e in rank]

''' rank and sample
'''
def sample_IDs(rank, sample_ratio):
	sample_num = np.round(sample_ratio * len(rank))
	return rank[:sample_num]

def extractSampleIDs(sample_ids, relation_name):
	s = []
	for e in sample_ids:
		k = e.split('_')
		if k[0] == relation_name:
			s.append(int(k[1]))
	return s

def extractSamples(dirName, outputDirName, sample_ids):
	allFiles, relations = getListOfFiles(dirName)
	for path, rname in zip(allFiles, relations):
		schema, tuples = read_csv(path)
		sample_ids = extractSampleIDs(sample_ids, rname)
		sample_data = [tuples[e] for e in sample_ids]
		output_file = os.path.join(outputDirName, rname)
		write_csv(output_file, schema, sample_data)


# process clusters
for cluster in clusters:
	ranks = rank(cluster, tids_hash, embeddings)
	sample_ids = sample_IDs(ranks, sample_ratio)
	extractSamples(original_data_dir, sample_data_dir, sample_ratio)


