import numpy as np
import csv
import os
import random
from collections import defaultdict

''' read directory of files
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

''' write a csv data to a file
'''
def write_csv(filename, schema, data):
    with open(filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(schema)
        writer.writerows(data)

''' read datasets from a directory
    return:
    (1) a list of datasets
    (2) a list of corresponding schemas
    (3) a list of relational names
'''
def loadDataFromDir(dirName):
    filenames, relations = getListOfFiles(dirName)
    datasets, schemas, tidStarts = [], [], []
    # for filename in filenames:
    for fid in range(len(filenames)):
        filename = filenames[fid]
        s, d = read_csv(filename)
        datasets.append(d)
        schemas.append(s)
    tidStarts = [0] * len(datasets)
    tidStarts[0] = 0
    for fid in range(len(filenames)-1):
        tidStarts[fid + 1] = tidStarts[fid] + len(datasets[fid])

    return datasets, schemas, relations, tidStarts

''' read results of a ML predicate
    return:
    a list of <t_0, t_1> tuple pair
'''
def loadOneMLPredicate(mlpredicate_file):
    pass

''' construct PLI for ONE column 
'''
def constructPLIOneColumn(dataset, cid, tidStart):
    pli = defaultdict(list)
    for line_id, record in enumerate(dataset):
        pli[record[cid]].append(line_id + tidStart)
    return pli

''' construct PLIS for all datasets
'''
def constructPLIs(datasets, schemas, relations, tidStarts):
    plis = []
    for rid, rname in enumerate(relations):
        pli_onedata = []
        dataset, schema, tidStart = datasets[rid], schemas[rid], tidStarts[rid]
        for cid in range(len(schema)):
            pli = constructPLIOneColumn(dataset, cid, tidStart)
            pli_onedata.append(pli)
        plis.append(pli_onedata)
    return plis

''' random walk starting from one tuple and sample the next tuple
'''
def randomWalkSamplingOneEdge(tid, rid_of_tid, datasets, tidStarts, plis, seed):
    #resultsTupleID = []
    #selected = {}
    pli_one = plis[rid_of_tid]
    record = datasets[rid_of_tid][tid - tidStarts[rid_of_tid]]
    colNum = len(record)
    countsCols = [0] * colNum
    for cid, d in enumerate(record):
        pli_t = pli_one[cid]
        if d in pli_t:
            countsCols[cid] = len(pli_t[d])
    probabilities = [e * 1.0 / sum(countsCols) for e in countsCols]
    # 1. randomly select one attribute by weights
    np.random.seed(tid * seed)
    cid_s = np.random.choice(colNum, 1, replace=False, p=probabilities)[0]
    # 2. retrieve the value of selected cid and extract the corresponding list
    value = record[cid_s]
    tupleList = pli_one[cid_s][value]
    # 3. randomly select one
    np.random.seed(2 * tid * seed)
    sc = np.random.choice(len(tupleList), 1, replace=False)[0]
    return tupleList[sc]

''' random walk
'''
def randomWalkSampling(datasets, tidStarts, plis, seed, sampleRatio, maxTuplePerRule):
    selectedTupleIDs = {}
    numTuples = tidStarts[-1] + len(datasets[-1])
    sample_num = int(numTuples * sampleRatio)
    iter = 0
    while iter <= sample_num:
        # 1. initialize one sample
        random.seed(iter * seed)
        tid_init = random.randint(0, numTuples)
        selectedTupleIDs.add(tid_init)
        iter += 1
        # 2. random walk
        tid = tid_init
        for i in range(maxTuplePerRule - 1):
            tid = randomWalkSamplingOneEdge(tid, retrieveRID(tid, tidStarts), datasets, tidStarts, plis, seed)
            selectedTupleIDs.add(tid)
            iter += 1
    return selectedTupleIDs


''' return the true relation id GIVEN one tuple id
'''
def retrieveRID(tid, tidStarts):
    rid = 0
    for rid in range(len(tidStarts) - 1):
        s = tidStarts[rid]
        if tid >= s and tid < tidStarts[rid + 1]:
            return rid
    return len(tidStarts)

''' generate the sample data from 'selectedTupleIDs'
'''
def genSample(selectedTupleIDs, datasets, tidStarts):
    samples = [[] for i in range(len(datasets))]
    for tid in selectedTupleIDs:
        rid = retrieveRID(tid, tidStarts)
        samples[rid].append(datasets[rid][tid - tidStarts[rid]])
    return samples

''' save sample data
'''
def saveSample(filedir, datasets, schemas, relations):
    # save csv files
    for rid in range(len(datasets)):
        schema = schemas[rid]
        filename = os.path.join(filedir, relations[rid] + '.csv')
        data = datasets[rid]
        write_csv(filename, schema, data)
