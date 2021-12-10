import os
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--model_hdfs_path", type=str, default="/sentencebert")
parser.add_argument("--input_hdfs_file", type=str, default="/input.txt")
parser.add_argument("--block_num", type=int, default=1)
parser.add_argument("--block_id", type=int, default=0)
hp = parser.parse_args()

model_hdfs_path_1 = hp.model_hdfs_path
input_hdfs_file = hp.input_hdfs_file
block_num = hp.block_num
block_id = hp.block_id

# spark RDD无法传参的原因，模型文件需放置在固定位置model_hdfs_path
model_hdfs_path = "/sentencebert"
model_local_path = "/home/mlsserver_py/data/sentencebert"
if model_hdfs_path != model_hdfs_path_1:
    print("hdfs dfs -rm -r " + model_hdfs_path)
    print(os.system("hdfs dfs -rm -r " + model_hdfs_path))
    print("hdfs dfs -cp " + model_hdfs_path_1 + " " + model_hdfs_path)
    print(os.system("hdfs dfs -cp " + model_hdfs_path_1 + " " + model_hdfs_path))

sentences =[]
block_sentences =[]
print(os.system("hdfs dfs -get " + input_hdfs_file + " "))
paths = input_hdfs_file.split(os.sep)
input_file = paths[len(paths)-1]
for line in open(input_file, 'r', encoding='utf-8'):
    sentences.append(line)

block_len = (len(sentences) + 0.0) / block_num

bid = 0
for i in range(len(sentences)):
    if (bid == block_id):
        block_sentences.append(sentences[i])
    if i > (bid + 1) * block_len:
        bid = bid + 1
        if bid > block_num - 1:
            bid = block_num - 1

from sentence_transformers import SentenceTransformer

sys.path.append("/home/mlsserver_py/src/main/resources/python/")
sys.path.append("/home/mlsserver_py/src/main/resources/python/" + "mlpredicate/lsh/")

from mlpredicate.lsh.lshash import LSHash
from mlpredicate.lsh.lsh_config import Config

if not os.path.exists(model_local_path):
    print(os.system("hdfs dfs -get " + model_hdfs_path + " " + model_local_path))

model = SentenceTransformer(model_local_path)

corpus_embeddings = model.encode(list(block_sentences), show_progress_bar=True, convert_to_numpy=True)

lsh = LSHash(Config.LSH_hash_size, Config.LSH_input_dim, Config.LSH_num_hashtables)
hashes = lsh.get_hash_for_pointlist_multiprocessing(corpus_embeddings, seed=88888888)

result_str = ""
result_str = result_str + Config.Begin_Sign
for hash in hashes:
    # print(hash)
    sh = str(hash)
    sh = sh.replace("[", "")
    sh = sh.replace("]", "")
    result_str = result_str + sh + "\n"

result_str = result_str + Config.End_Sign
print(result_str)