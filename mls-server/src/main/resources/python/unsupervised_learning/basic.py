import numpy as np
import sys
import csv
import copy
import os
from collections import defaultdict
from subprocess import Popen, PIPE, call
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


def clean_sents(sents):
    sents_clean = []
    for sent in sents:
        sents_clean.append(clean_sent(sent))
    return sents_clean


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
def getListOfFiles(dirName, suffix='.csv'):
    # create a list of all '*.csv' files
    # names in the given directory
    suffix_len = len(suffix)
    listOfFile = os.listdir(dirName)
    names = [r for r in listOfFile if r[-suffix_len:] == suffix]
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

    return allFiles, [r[:-suffix_len] for r in listOfFile if r[-suffix_len:] == suffix]


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

def write_csv_hdfs(client_hdfs, data, path):
    with client_hdfs.write(path) as writer:
        data.to_csv(writer)


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

def generate_trainingdataHDFS(original_data_dir, sample_data_file, sample_ratio, sample_seed):
    # allFiles, relations = getListOfFilesHDFS(original_data_dir)
    process = Popen('mkdir ./temp_dir', shell=True, cwd='./')
    process = Popen('rm -r ./temp_dir/*',shell=True, cwd='./')
    process = Popen(f'hdfs dfs -get ' + original_data_dir + '/* ./temp_dir/', shell=True, stdout=PIPE, stderr=PIPE)
    process.communicate()
    training_data = []
    allFiles, relations = getListOfFiles("./temp_dir/")
    for path, rname in zip(allFiles, relations):
        print(path, rname)
        schema, data = read_csv(path)
        sample_num = int(sample_ratio * len(data))
        np.random.seed(sample_seed)
        sc = np.random.choice(len(data), sample_num, replace=False)
        samples = [data[e] for e in sc]
        # add sample data
        print(samples[:5])
        training_data += [' '.join([e.lower().strip() for e in ss]) for ss in samples]

    f = open(sample_data_file, 'w')
    for data in training_data:
        f.write(data)
        f.write('\n')
    f.close()

def samplesWithoutOneAttr(samples):
    _samples = []
    num = len(samples[0])
    for i, ss in enumerate(samples):
        np.random.seed(i)
        sc = np.random.choice(num, 2, replace=False)
        for e in sc:
            attr = ' '.join([_e.lower().strip() for i, _e in enumerate(ss) if i != e])
            _samples.append(attr)
    return _samples

def generate_trainingData(original_data_dir, sample_data_file, sample_ratio, sample_seed):
    allFiles, relations = getListOfFiles(original_data_dir)
    sample_datasets = []
    training_data = []
    for path, rname in zip(allFiles, relations):
        schema, data = read_csv(path)
        sample_num = int(sample_ratio * len(data))
        np.random.seed(sample_seed)
        sc = np.random.choice(len(data), sample_num, replace=False)
        samples = [data[e] for e in sc]
        training_data += [' '.join([e.lower().strip() for e in ss]) for ss in samples]
        _samples = samplesWithoutOneAttr(samples)
        training_data += _samples

    training_data = clean_sents(training_data)

    f = open(sample_data_file, 'w')
    for data in training_data:
        f.write(data)
        f.write('\n')
    f.close()

def write(data, output_file):
    f = open(output_file, 'w')
    for r in data:
        f.write(r)
        f.write('\n')
    f.close()

def generate_testingData(original_data_dir, original_data_vae_dir):
    allFiles, relations = getListOfFiles(original_data_dir)
    for path, rname in zip(allFiles, relations):
        schema, data = read_csv(path)
        attr_num = len(schema)
        # generate the full data
        data_full = []
        for record in data:
            attr = ' '.join([_e.lower().strip() for i, _e in enumerate(record)])
            data_full.append(attr)
        # save
        data_full = clean_sents(data_full)
        output_file = os.path.join(original_data_vae_dir, rname + '.txt')
        write(data_full, output_file)
        # generate data by taking out one specific attribute
        for aid in range(attr_num):
            data_sub = []
            for record in data:
                attr = ' '.join([_e.lower().strip() for i, _e in enumerate(record) if i != aid])
                data_sub.append(attr)
            # save
            data_sub = clean_sents(data_sub)
            output_file = os.path.join(original_data_vae_dir, rname + '___' + str(aid) + '.txt')
            write(data_sub, output_file)





'''
import sys

original_data_dir = sys.argv[1]
train_original_file = sys.argv[2]
infer_original_file = sys.argv[3]
sample_num = int(sys.argv[4])

generateAllString(original_data_dir, train_original_file, infer_original_file, sample_num)
'''


