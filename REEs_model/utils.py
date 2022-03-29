import numpy as np
import os
import csv
from collections import defaultdict
from REEs_model.parameters import *
from REEs_model.PredicateComsFilter.predicateAgent import Predicate


# func to read a csv file, output[0] is a schema
def read_csv(filename):
    data = []
    f = open(filename, 'r', encoding='gb18030', errors='ignore')
    with f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            data.append(row)
    f.close()
    return data[0], data[1:]

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

def readDataFile(dirName):
    allFiles, _ = getListOfFiles(dirName)
    datasets = []
    for filename in allFiles:
        s, d = read_csv(filename)
        datasets.append(d)
    return datasets


def tokenize(rule, tokenVobs):
    lhs, rhs = rule.split(LHS_TO_RHS_SYMBOL)[0].strip(), rule.split(LHS_TO_RHS_SYMBOL)[1].strip()
    tokens_lhs = [e.strip() for e in lhs.split(LHS_DELIMITOR_SYMBOL)]
    #print(tokens_lhs)
    token_rhs = rhs
    unit_lhs = [parsePredicate(e, tokenVobs) for e in tokens_lhs]
    unit_lhs = [e for e in unit_lhs if e != None]
    unit_rhs = parsePredicate(token_rhs, tokenVobs)
    unit_rule = [e for e in unit_lhs]
    unit_rule.append(unit_rhs)
    return unit_rule

def processAllRules(rees, tokenVobs):
    #tokenVobs = defaultdict(int)
    insertVobsHash(PADDING_VALUE, tokenVobs)
    rees_transform = []
    for rule in rees:
        rees_transform.append(tokenize(rule, tokenVobs))
    # transform tokens to IDs in tokenVobs
    rees_lhs_final, rees_rhs_final = [], []
    for rule in rees_transform:
        #print(rule)
        lhs, rhs = [], []
        # deal with lhs
        lhs_len = len(rule) - 1
        for e in rule[:-1]:
            lhs += [tokenVobs[z] for z in e]
        lhs = lhs + (MAX_LHS_PREDICATES * TOKENS_OF_PREDICATE - len(lhs)) * [tokenVobs[PADDING_VALUE]]
        rees_lhs_final.append(lhs)
        # deal with rhs
        rhs += [tokenVobs[z] for z in rule[-1]]
        rhs = rhs + (MAX_RHS_PREDICATES * TOKENS_OF_PREDICATE - len(rhs)) * [tokenVobs[PADDING_VALUE]]
        rees_rhs_final.append(rhs)
    return rees_lhs_final, rees_rhs_final #, tokenVobs


def splitPredicate(predicate, tokenVobs):
    res = []
    # operand1
    operand1 = predicate[0]
    tid1 = operand1.split('.')[0]
    attr1 = operand1[len(tid1) + 1:]
    res += [tid1, attr1]
    # add operator
    res += [predicate[1]]
    # operand2
    # check whether the operand is a constant or not
    operand2 = predicate[-1]
    if len(operand2) >= 3 and operand2[0] == 't' and operand2[1].isdigit() and operand2[2] == '.':
        # is an operand
        tid2 = operand2.split('.')[0]
        attr2 = operand2[len(tid2) + 1:]
        res += [tid2, attr2]
    else:
        res += [tid1, operand2]
    
    # add to tokenVobs
    for token in res:
        insertVobsHash(token, tokenVobs)
    return res

def parsePredicate(predicate, tokenVobs):
    """
    提取规则 核心数据
    :param predicate:
    :return:
    """
    #print('PREDICATE  : ', predicate)
    # check Relation(t0)  ---- 提取规则中的 t0 t1这样的 比如： rule=casorgcn(t0) rule=order(t1)  提取的都是括号里面的 t0 t1
    if predicate.find("(") != -1 and predicate.find(")") != -1 and predicate[:2] != 'ML' and predicate[:len('similar')] != 'similar':
        ss = predicate[predicate.find("(") + 1:predicate.find(")")]
        for i in range(1, len(ss), 1):
            if ss[:i].isalpha and ss[i:].isdigit:
                return None

    res = None
    # check ML(t0.A, t1.B)
    if predicate[:2] == "ML":
        t = predicate.split('(')
        operator = t[0]
        # find substring in '( ... )'
        op = predicate[predicate.find("(") + 1:predicate.find(")")]
        operand1, operand2 = op.split(',')[0].strip(), op.split(',')[1].strip()
        res = [operand1, operator, operand2]
    elif len(predicate) >= len('similar') and predicate[:len('similar')] == "similar":
        ss = [e.strip() for e in predicate[len('similar') + 1:-1].split(',')]
        # print(ss)
        op = ss[0] + ' ' + ss[-1]
        res = [ss[1], op, ss[2]]
    else: # check other predicates
        t = predicate.split()
        operand1 = t[0]
        operator = t[1]
        operand2 = ' '.join(t[2:])
        res = [operand1, operator, operand2]
    return splitPredicate(res, tokenVobs)

def convert_to_onehot(data):
    data_onehot = np.zeros((len(data), 2))
    for rid, e in enumerate(data):
        # 这里有可能出现Nan  需要特殊处理
        if e == e:
            e = int(e)
        else:
            e = 0
        data_onehot[rid, int(e)] = 1
        # data_onehot[rid, ]
    return data_onehot


'''  parse predicates
'''
def generateVobs(all_predicates_file):
    vobsHash = defaultdict(int)
    vobsHash[PADDING_VALUE] = 0
    # add predicate strings
    allPredicatesStr = []
    for line in open(all_predicates_file):
        allPredicatesStr.append(line.strip())
    for pStr in allPredicatesStr:
        p = Predicate(pStr)
        if p.index1 != None:
            insertVobsHash(p.index1, vobsHash)
        if p.index2 != None:
            insertVobsHash(p.index2, vobsHash)
        if p.operator != None:
            insertVobsHash(p.operator, vobsHash)
        if p.operand1 != None:
            insertVobsHash(p.operand1['relation'], vobsHash)
            insertVobsHash(p.operand1['attribute'], vobsHash)
        if p.operand2 != None:
            insertVobsHash(p.operand2['relation'], vobsHash)
            insertVobsHash(p.operand2['attribute'], vobsHash)
        if p.constant != None:
            insertVobsHash(p.constant, vobsHash)
    return vobsHash

def insertVobsHash(element, vobsHash):
    if element not in vobsHash:
        vobsHash[element] = len(vobsHash.keys())


