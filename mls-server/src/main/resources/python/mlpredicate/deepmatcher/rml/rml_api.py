import os
import sys
import time
import platform
import argparse
from .config import Config
import deepmatcher as dm
import random
import re


sys.path.append(Config.Py_Path)
sys.path.append(Config.Py_Path + Config.Py_DeepMatcher_Path)

block_num = 1
block_id = 0

parser = argparse.ArgumentParser()
parser.add_argument("--hdfs_path", type=str, default=Config.Hdfs_Path)
parser.add_argument("--table1", type=str, default=Config.Sentences_File_1)
parser.add_argument("--table2", type=str, default=Config.Sentences_File_2)
parser.add_argument("--pair_file", type=str, default="pairs.txt")
parser.add_argument("--block_num", type=int, default=1)
parser.add_argument("--block_id", type=int, default=0)
parser.add_argument("--model", type=str, default="sentencebert")
parser.add_argument("--threshold", type=float, default=0.9)

hp = parser.parse_args()

hdfs_path = hp.hdfs_path
pairs_file = hp.pair_file
sentences_file_1 = hp.table1
sentences_file_2 = hp.table2
model = hp.model
threshold = hp.threshold
block_num = hp.block_num
block_id = hp.block_id

# 第一步，下载数据文件到本地
# 删掉本地缓存数据
print(os.system("rm -rf " + Config.Local_Data_Path))
if not os.path.exists(Config.Local_Data_Path):
    os.makedirs(Config.Local_Data_Path)
print(os.system("hdfs dfs -get " + sentences_file_1 + " " + Config.Local_Data_Path))
print(os.system("hdfs dfs -get " + sentences_file_2 + " " + Config.Local_Data_Path))
print(os.system("hdfs dfs -get " + hdfs_path + "/" + model + " " + Config.Local_Data_Path))
print(os.system("hdfs dfs -get " + hdfs_path + "/" + pairs_file + " " + Config.Local_Data_Path))


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

def get_deepmatcher_predictions(model_path, pairs, threshold):
    # 把测试数据写入临时文件中
    tmp_csv_file = "tmp_csv_" + str(random.randint(0,1000000)) + ".csv"
    id = 0
    with open(tmp_csv_file, 'w') as fout:
        fout.write("id,left_value,right_value\n")
        for pair in pairs:
            fout.write(str(id) + "," + pair[0] + "," + pair[1] + "\n")
            id = id + 1
        fout.flush()
        fout.close()

    model = dm.MatchingModel()
    model.load_state(model_path)

    unlabeled = dm.data.process_unlabeled(path=tmp_csv_file, trained_model=model)
    prediction = str(model.run_prediction(unlabeled))

    prediction = re.sub(' +', '\t', prediction)
    print(prediction)

    lines = prediction.split("\n")

    labels = []
    for line_num in range(2, len(lines)):
        line = lines[line_num]
        str_list = line.split('\t')
        if(len(str_list) > 1):
            match_score = float(str_list[1])
            if(match_score > threshold):
                labels.append(1)
            else:
                labels.append(0)

    return list(labels)

# 第三步，读入blocking后的id pair
with open(Config.Local_Data_Path + pairs_file, 'r', encoding='utf-8') as fin:
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
            print(ids[i])
            id_1 = ids[i].split("|")[0]
            id_2 = ids[i].split("|")[1]
            id_pairs.append([id_1, id_2])

            sentence_1 = ""
            for index in columns_index_1:
                if len(sentence_1) > 0:
                    sentence_1 = sentence_1 + " "
                sentence_1 = sentence_1 + str(sentences_dict_1[id_1][index])

            sentence_2 = ""
            for index in columns_index_2:
                if len(sentence_2) > 0:
                    sentence_2 = sentence_2 + " "
                sentence_2 = sentence_2 + str(sentences_dict_2[id_2][index])

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

        labels = get_deepmatcher_predictions(Config.Local_Data_Path + model, predict_sentence_pairs, threshold)

        time3 = time.time()
        print("预测时间: " + str(int(time3) - int(time2)))
        print("labels: " + str(labels))

        i = 0
        result_str = ""
        for label in labels:
            if label == 1 or label == "1":
                id_1 = predict_id_pairs[i][0]
                id_2 = predict_id_pairs[i][1]
                result_str = result_str + (str(id_1) + ";" + str(id_2) + ";")
            i = i + 1

        print(Config.Begin_Sign + result_str + Config.End_Sign)
