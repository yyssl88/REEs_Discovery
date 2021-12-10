import os
import sys
import time
import platform
import argparse

if (platform.system() == 'Windows'):
    sys.path.append("F:/work/javaproject/mls-server_feature-er-condig/src/main/resources/python")
    sys.path.append("F:/work/javaproject/mls-server_feature-er-condig/src/main/resources/python/mlpredicate/ditto")
    sys.path.append("F:/work/javaproject/mls-server_feature-er-condig/src/main/resources/python/mlpredicate/ditto/Snippext_public")
else:
    sys.path.append("/home/mlsserver_py/src/main/resources/python/")
    sys.path.append("/home/mlsserver_py/src/main/resources/python/" + "mlpredicate/ditto/")
    sys.path.append("/home/mlsserver_py/src/main/resources/python/" + "mlpredicate/ditto/Snippext_public/")
from mlpredicate.ditto.rml.config import Config

block_num = 1
block_id = 0

parser = argparse.ArgumentParser()
parser.add_argument("--table1_hdfs_path", type=str, default=Config.Sentences_File_1)
parser.add_argument("--table2_hdfs_path", type=str, default=Config.Sentences_File_2)
parser.add_argument("--blocking_result_hdfs_path", type=str, default="pairs.txt")
parser.add_argument("--model_hdfs_path", type=str, default="ditto")
parser.add_argument("--block_num", type=int, default=1)
parser.add_argument("--block_id", type=int, default=0)
hp = parser.parse_args()

sentences_file_1 = hp.table1_hdfs_path
sentences_file_2 = hp.table2_hdfs_path
blocking_result_file = hp.blocking_result_hdfs_path
model = hp.model_hdfs_path
block_num = hp.block_num
block_id = hp.block_id

# 第一步，下载数据文件到本地
# 删掉本地缓存数据
print(os.system("rm -rf " + Config.Local_Data_Path))
if not os.path.exists(Config.Local_Data_Path):
    os.makedirs(Config.Local_Data_Path)
print(os.system("hdfs dfs -get " + sentences_file_1 + " " + Config.Local_Data_Path))
print(os.system("hdfs dfs -get " + sentences_file_2 + " " + Config.Local_Data_Path))

# 模型文件包含pt文件和config文件
print(os.system("hdfs dfs -get " + model + " " + Config.Local_Data_Path))
print(os.system("hdfs dfs -get /configs.json " + Config.Local_Data_Path))

print(os.system("hdfs dfs -get " + blocking_result_file + " " + Config.Local_Data_Path))
if not os.path.exists("~/.cache/huggingface"):
    print(os.system("hdfs dfs -get /huggingface ~/.cache/"))
if not os.path.exists("/usr/lib/nltk_data"):
    print(os.system("hdfs dfs -get /nltk_data /usr/lib/"))

# strs = str(sentences_file_1).split("/")
# sentences_file_1 = strs[len(strs)-1]
#
# strs = str(sentences_file_2).split("/")
# sentences_file_2 = strs[len(strs)-1]
#
# strs = str(blocking_result_file).split("/")
# blocking_result_file = strs[len(strs)-1]
#
# strs = str(model).split("/")
# model = strs[len(strs)-1]

# 第二步，读入表，构造{id -> 行}词典
def sentences2dict(sentences_file):
    sentences_dict = {}
    titles = []

    with open(Config.Local_Data_Path + sentences_file, 'r', encoding='utf-8') as fin:
        i = 0
        for line in fin:
            line = line.replace("\n", "")
            if i == 0:
                titles = line.split(",")
            else:
                sentences_dict[str(i - 1)] = line.split(",")
            i = i + 1

    return titles, sentences_dict

from matcher import *
def get_ditto_predictions(pairs, parser):
    # parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, default='ditto')
    parser.add_argument("--input_path", type=str, default='input/test.txt')
    parser.add_argument("--output_path", type=str, default='output/test_out.txt')
    # parser.add_argument("--lm", type=str, default='distilbert')
    parser.add_argument("--lm", type=str, default='roberta')
    parser.add_argument("--use_gpu", dest="use_gpu", action="store_true")
    parser.add_argument("--fp16", dest="fp16", action="store_true")
    parser.add_argument("--checkpoint_path", type=str, default=Config.Local_Data_Path)
    parser.add_argument("--dk", type=str, default=None)
    parser.add_argument("--summarize", dest="summarize", action="store_true")
    parser.add_argument("--max_len", type=int, default=256)
    hp = parser.parse_args()

    # load the models
    config, model = load_model(hp.model, hp.checkpoint_path,
                               hp.lm, hp.use_gpu, hp.fp16)

    summarizer = dk_injector = None
    if hp.summarize:
        summarizer = Summarizer(config, hp.lm)

    if hp.dk is not None:
        if 'product' in hp.dk:
            dk_injector = ProductDKInjector(config, hp.dk)
        else:
            dk_injector = GeneralDKInjector(config, hp.dk)

    predictions = []
    logits = []

    lm=hp.lm
    max_len=hp.max_len
    try:
        predictions, logits = classify(pairs, config, model, lm=lm, max_len=max_len)
    except:
        # ignore the whole batch
        return predictions

    return predictions

# 第三步，读入blocking后的id pair
import os
paths = blocking_result_file.split(os.sep)
blocking_result_file = paths[len(paths)-1]
print(blocking_result_file)
with open(Config.Local_Data_Path + blocking_result_file, 'r', encoding='utf-8') as fin:
    for line in fin:
        # 每行的格式为：
        # ML;tableA->title,author,women;tableA->title,author,women:2|3;4|5;
        time1 = time.time()

        line = line.replace("\n", "")
        ids = line.split(";")

        # 第3.1步，读入table和column信息
        table_1 = ids[1].split("->")[0]
        columns_1 = ids[1].split("->")[1].split(",")
        columns_index_1 = set()

        titles_1, sentences_dict_1 = sentences2dict(table_1 + ".csv")
        # 分桶，保证每个元素有且只有分到一个桶
        i = 0
        for title in titles_1:
            if title in columns_1:
                columns_index_1.add(i)
            i = i + 1

        table_2 = ids[2].split("->")[0]
        columns_2 = ids[2].split("->")[1].split(",")
        columns_index_2 = set()

        titles_2, sentences_dict_2 = sentences2dict(table_2 + ".csv")
        i = 0
        for title in titles_2:
            if title in columns_2:
                columns_index_2.add(i)
            i = i + 1

        id_pairs = []
        sentence_pairs = []
        i = 3

        while i < len(ids) - 1:
            # print(ids[i])
            id_1 = ids[i].split("|")[0]
            id_2 = ids[i].split("|")[1]
            id_pairs.append([id_1, id_2])

            # sentence_1 = ""
            # for index in columns_index_1:
            #     if len(sentence_1) > 0:
            #         sentence_1 = sentence_1 + " "
            #     sentence_1 = sentence_1 + str(sentences_dict_1[id_1][index])
            #
            # sentence_2 = ""
            # for index in columns_index_2:
            #     if len(sentence_2) > 0:
            #         sentence_2 = sentence_2 + " "
            #     sentence_2 = sentence_2 + str(sentences_dict_2[id_2][index])

            sentence_1 = ""
            for index in columns_index_1:
                sentence_1 = sentence_1 + " [COL] " + str(titles_1[index]) + " [VAL] " + str(sentences_dict_1[id_1][index]) + " "

            sentence_2 = ""
            for index in columns_index_2:
                sentence_2 = sentence_2 + " [COL] " + str(titles_2[index]) + " [VAL] " + str(sentences_dict_2[id_2][index]) + " "

            sentence_pairs.append([str(sentence_1), str(sentence_2)])

            i = i + 1

        time2 = time.time()
        print("读入数据时间: " + str(int(time2) - int(time1)))

        # 补充模型下载判断代码

        predict_sentence_pairs = []
        predict_id_pairs = []
        pairs_num = len(sentence_pairs)
        block_len = (pairs_num + 0.0) / block_num

        bid = 0
        for i in range(len(sentence_pairs)):
            if (bid == block_id):
                predict_sentence_pairs.append(sentence_pairs[i])
                predict_id_pairs.append(id_pairs[i])
                # print("block_id: " + str(block_id) + " " + str(i))
            if i > (bid + 1) * block_len:
                bid = bid + 1
                if bid > block_num - 1:
                    bid = block_num - 1

        labels = get_ditto_predictions(predict_sentence_pairs, parser)

        time3 = time.time()
        print("预测时间: " + str(int(time3) - int(time2)))
        print("labels: " + str(labels))

        i = 0
        result_str = ""
        for label in labels:
            if label == 1 or label == "1":
                id_1 = predict_id_pairs[i][0]
                id_2 = predict_id_pairs[i][1]
                result_str = result_str + (str(id_1) + "|" + str(id_2) + ";")
            i = i + 1

        print(Config.Begin_Sign + result_str + Config.End_Sign)
