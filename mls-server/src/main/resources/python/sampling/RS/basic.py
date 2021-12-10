import numpy as np
import sys
import csv
import copy
import os
from collections import defaultdict
from clean import *
from subprocess import Popen, PIPE
from hdfs import *
import pandas as pd

# from calCCM import *

# func to read a csv file, output[0] is a schema
def read_csv(filename):
    data = []
    f = open(filename, 'r')
    with f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            data.append(row)
    f.close()
    return data[0], data[1:]

def write_csv(filename, schema, data):
    with open(filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(schema)
        writer.writerows(data)


''' combine csv to one string
'''
def combineCSV2one(relation_name, input_csv):
    # load data
    # id,title,authors,venue,year
    schema, tuples = read_csv(input_csv)
    #selected_schema = ['title', 'venue']
    #selected_schema = ['id', 'title', 'authors', 'venues', 'year', 'eid']
    scripts = [schema.index(e) for e in schema if "id" not in e]

    print(tuples[:5])
    #data = combine_csv(tuples, scripts)
    data = combine_csv_(tuples, scripts)

    data_ = []
    for i, d in enumerate(data):
        data_.append([relation_name + '_' + str(i), d])
    # write(data, output_file)
    return data_


''' list all .csv files in the directory
'''
def getListOfFiles(dirName):
    # create a list of all '*.csv' files
    # names in the given directory
    listOfFile = os.listdir(dirName)
    names = [r for r in listOfFile if r[-4:] == '.csv']
    allFiles = list()
    # Iterate over all the entries
    for entry in names:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory
        if os.path.isdir(fullPath):
            allFiles = allFiles + getListOfFiles(fullPath)
        else:
            allFiles.append(fullPath)

    return allFiles, [r[:-4] for r in listOfFile if r[-4:] == '.csv']


''' list all .csv files in the directory from HDFS
'''
def getListOfFilesHDFS(dirName):
    process = Popen(f'hdfs dfs -ls -h ' + dirName, shell=True, stdout=PIPE, stderr=PIPE)
    std_out, _ = process.communicate()
    allFiles = [fn.split(' ')[-1] for fn in std_out.decode().split('\n')[1:]][:-1]
    files = [fn.split(' ')[-1].split('/')[-1] for fn in std_out.decode().split('\n')[1:]][:-1]
    return allFiles, [r[:-4] for r in files if r[-4:] == '.csv']

''' sample 
'''
def sample(data, sample_num):
    np.random.seed(20)
    sc = np.random.choice(len(data), sample_num, replace=False)
    sample = []
    for e in sc:
        sample.append(data[e][1])
    return sample


def generateAllString(dirName, train_file, all_file, sample_num=5000):
    allFiles, relations = getListOfFiles(dirName)
    data = []
    for path, rname in zip(allFiles, relations):
        data += combineCSV2one(rname, path)

    # generate training data
    sample_data = sample(data, sample_num)
    write(sample_data, train_file)
    # write all data
    schema = ['tid', 'data']
    write_csv(data, schema, all_file)

def uniform_sample(original_data_dir, sample_data_dir, sample_ratio, sample_seed):
    allFiles, relations = getListOfFiles(original_data_dir)
    sample_datasets = []
    schemas = []
    for path, rname in zip(allFiles, relations):
        schema, data = read_csv(path)
        sample_num = int(sample_ratio * len(data))
        np.random.seed(sample_seed)
        sc = np.random.choice(len(data), sample_num, replace=False)
        samples = [data[e] for e in sc]
        # add sample data
        sample_datasets.append(samples)
        schemas.append(schema)

    # write to files
    for rid, rname in enumerate(relations):
        output_file = os.path.join(sample_data_dir, rname + '.csv')
        write_csv(output_file, schemas[rid], sample_datasets[rid])


def read_csv_hdfs(client_hdfs, path):
    # client_hdfs = InsecureClient('http://vm16' + ':50070')
    with client_hdfs.read(path) as reader:
        df = pd.read_csv(reader, index_col=0)
    return df


def uniform_sampleHDFS(original_data_dir, sample_data_dir, sample_ratio, sample_seed):
    allFiles, relations = getListOfFilesHDFS(original_data_dir)
    sample_datasets = []
    schemas = []
    client_hdfs = InsecureClient('http://vm16' + ':50070')
    for path, rname in zip(allFiles, relations):
        with client_hdfs.read(path) as reader:
            df = pd.read_csv(reader, index_col=0)
        schema = list(df.column)
        samples = df.sample(frac=sample_ratio, replace=True, random_state=sample_seed)
        # add sample data
        sample_datasets.append(samples)
        schemas.append(schema)

    # write to files
    for samples in sample_datasets:
        output_file = os.path.join(sample_data_dir, rname + '.csv')
        with client_hdfs.write(output_file) as writer:
            samples.to_csv(writer)





'''
import sys

original_data_dir = sys.argv[1]
train_original_file = sys.argv[2]
infer_original_file = sys.argv[3]
sample_num = int(sys.argv[4])

generateAllString(original_data_dir, train_original_file, infer_original_file, sample_num)
'''


