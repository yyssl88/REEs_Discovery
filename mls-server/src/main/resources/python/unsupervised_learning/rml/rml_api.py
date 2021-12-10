import sys
import time
import os

local_path = "/unsupervised/"

import platform
if(platform.system()=='Windows'):
    sys.path.append("f:/work/javaproject/spark_test/src/main/python/")
else:
    sys.path.append("/home/unsupervised_learning/src/main/python/")
    local_path = "/home/unsupervised_learning/data/"



from unsupervised_learning.er_matcher import ER_Matcher

hdfs_path = sys.argv[1]
taskId = sys.argv[2]
print(hdfs_path)
#hdfs_path = ""
table_hdfs_path = "/tmp/wanjia/"
sentences_file_1 = "tableA.csv"
sentences_file_2 = "tableB.csv"
pairs_file = taskId

result_file = open("ml_result.txt", "w", encoding='utf-8')

block_num = 1
block_id = 0

if (len(sys.argv) >= 5):
    block_num = int(sys.argv[3])
    block_id = int(sys.argv[4])

# 第一步，下载数据文件到本地
print(os.system("hdfs dfs -get " + table_hdfs_path + "/" + sentences_file_1 + " " + local_path))
print(os.system("hdfs dfs -get " + table_hdfs_path + "/" + sentences_file_2 + " " + local_path))
print(os.system("hdfs dfs -get " + hdfs_path +" " + local_path))


# 第二步，读入表，构造{id -> 行}词典
def sentences2dict(sentences_file):
    sentences_dict = {}
    titles = []

    with open(local_path + sentences_file, 'r', encoding='utf-8') as fin:
        i = 0
        for line in fin:
            line = line.replace("\n", "")
            if i == 0:
                titles = line.split(",")
            else:
                sentences_dict[str(i-1)] = line.split(",")
            i = i + 1

    return titles, sentences_dict

    # 第三步，读入blocking后的id pair
with open(local_path + pairs_file, 'r', encoding='utf-8') as fin:
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


        max_fix_pairs = 5000
        fit_pairs = []
        if len(sentence_pairs) > max_fix_pairs:
            for i in range(max_fix_pairs):
                fit_pairs.append(sentence_pairs[i])
        else:
            fit_pairs = sentence_pairs
        er = ER_Matcher()
        er.fit(fit_pairs)

        time3 = time.time()
        print("训练时间: " + str(int(time3) - int(time2)))


        predict_sentence_pairs = []
        predict_id_pairs = []
        pairs_num = len(sentence_pairs)
        block_len = (pairs_num + 0.0) / block_num

        bid = 0
        for i in range(len(sentence_pairs)):
            if(bid == block_id):
                predict_sentence_pairs.append(sentence_pairs[i])
                predict_id_pairs.append(id_pairs[i])
                # print("block_id: " + str(block_id) + " " + str(i))
            if i > (bid + 1) * block_len:
                bid = bid + 1
                if bid > block_num - 1:
                    bid = block_num - 1

        labels = er.predict(predict_sentence_pairs)

        time4 = time.time()
        print("预测时间: " + str(int(time4) - int(time3)))

        i = 0
        result_str = ""
        for label in labels:
            if label == 1 or label == "1":
                id_1 = predict_id_pairs[i][0]
                id_2 = predict_id_pairs[i][1]
                result_file.write(str(id_1) + ";" + str(id_2) + ";")
                result_str = result_str + (str(id_1) + ";" + str(id_2) + ";")
            i = i + 1

        result_file.write("\n")
        print("\n")
        print("unsupervised_learning result is:" + result_str + "unsupervised_learning result end")

        time5 = time.time()
        print("预测时间: " + str(int(time5) - int(time4)))


result_file.flush()
result_file.close()
