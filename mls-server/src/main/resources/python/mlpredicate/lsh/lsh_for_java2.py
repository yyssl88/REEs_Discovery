import os
import sys

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from sentence_transformers import SentenceTransformer
from multiprocessing import Pool as ThreadPool

sys.path.append("/home/mlsserver_py/src/main/resources/python/")
sys.path.append("/home/mlsserver_py/src/main/resources/python/" + "mlpredicate/lsh/")

from mlpredicate.lsh.lshash import LSHash
from mlpredicate.lsh.lsh_config import Config

os.environ["PYSPARK_PYTHON"] = Config.Python_Home

model_hdfs_path = "/sentencebert"
model_local_path = "/home/mlsserver_py/data/sentencebert"

def get_hashes(sentences):
    sys.path.append("/home/mlsserver_py/src/main/resources/python/")
    sys.path.append("/home/mlsserver_py/src/main/resources/python/" + "mlpredicate/lsh/")
    from mlpredicate.lsh.lshash import LSHash
    from mlpredicate.lsh.lsh_config import Config
    os.environ["PYSPARK_PYTHON"] = Config.Python_Home

    if not os.path.exists(model_local_path):
        print(os.system("hdfs dfs -get " + model_hdfs_path + " " + model_local_path))
    model = SentenceTransformer(model_local_path)
    corpus_embeddings = model.encode(list(sentences), show_progress_bar=True, convert_to_numpy=True)
    lsh = LSHash(Config.LSH_hash_size, Config.LSH_input_dim, Config.LSH_num_hashtables)
    # hashes = []
    # for corpus_embedding in corpus_embeddings:
    #     hash = lsh.get_hash(corpus_embedding, seed=Config.LSH_SEED)
    #     hashes.append(hash)

    # 多进程并行
    def process(corpus_embedding):
        return lsh.get_hash(corpus_embedding, seed=Config.LSH_SEED)

    pool = ThreadPool()
    hashes = pool.map(process, corpus_embeddings)
    pool.close()
    pool.join()

    return hashes

def get_lsh(spark=None, sentences=[], model_hdfs_path="/sentencebert", input_hdfs_file="/input.txt"):
    if spark == None:
        spark = SparkSession.builder.config(conf=SparkConf().setAppName("lsh_test")).getOrCreate()
        sc = spark.sparkContext

    # 第1步，把测试句子分桶，并转成向量
    if len(sentences) == 0:
        print(os.system("hdfs dfs -get " + input_hdfs_file + " "))
        paths = input_hdfs_file.split(os.sep)
        input_file = paths[len(paths)-1]
        for line in open(input_file, 'r', encoding='utf-8'):
            sentences.append(line)

    i = 0
    sentence_bucket = []
    sentence_buckets = []
    for line in sentences:
        sentence_bucket.append(line)
        i = i + 1
        if i == Config.Bucket_size:
            sentence_buckets.append(sentence_bucket)
            sentence_bucket = []
            i = 0
    if i > 0:
        sentence_buckets.append(sentence_bucket)

    sentencesRDD = sc.parallelize(sentence_buckets)
    appliedRDD = sentencesRDD.flatMap(get_hashes)
    hashes = appliedRDD.collect()

    return hashes

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_hdfs_path", type=str, default="/sentencebert")
    parser.add_argument("--input_hdfs_file", type=str, default="/input.txt")
    hp = parser.parse_args()

    model_hdfs_path_1 = hp.model_hdfs_path
    input_hdfs_file = hp.input_hdfs_file

    # spark RDD无法传参的原因，模型文件需放置在固定位置model_hdfs_path
    if model_hdfs_path != model_hdfs_path_1:
        print("hdfs dfs -rm -r " + model_hdfs_path)
        print(os.system("hdfs dfs -rm -r " + model_hdfs_path))
        print("hdfs dfs -cp " + model_hdfs_path_1 + " " + model_hdfs_path)
        print(os.system("hdfs dfs -cp " + model_hdfs_path_1 + " " + model_hdfs_path))

    hashes = get_lsh(model_hdfs_path=model_hdfs_path, input_hdfs_file=input_hdfs_file)

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