import numpy as np
import logging
import copy

'''
type 1: t.A op t'.B
type 2: t.A op c

op = {=, !=, <, <=, >=, >, ML}
'''


class Predicates(object):
    def __init__(self, es_file, delimiter=';', **kwargs):
        self.delimiter = delimiter
        self.predicates = {}
        self.es = self.loadESFile(es_file)
        self.genAllPredicates()
        return

    def loadESFile(self, es_file):
        '''
        :param es_file: store all evidences and their supports
                        with the format "PS\t\tsupport"
        :return:
        '''
        es = {}
        for line in open(es_file, encoding='utf-8'):
            if len(line.strip())==0:continue
            ps = line.split('SUPP')[0]
            support = int(line.split('SUPP')[1])
            es[ps] = support
        return es

    def filterPredicates(self, p):
        if p[:2] == 'ML':
            return False
        op = p.split()[1].strip()
        if (op == "<>"):
            return True
        return False

    def genAllPredicates(self):
        count = 0
        for e, support in self.es.items():
            predicates = e.split(self.delimiter)
            for p in predicates:
                if p not in self.predicates and self.filterPredicates(p) == False:
                     self.predicates[p] = count
                     count += 1

    def EStoVec(self):
        num = sum(list(self.es.values()))
        logging.info(self.predicates)
        X = np.zeros((num,len(list(self.predicates.keys()))), dtype=np.uint8)
        c = 0
        for i, e in enumerate(list(self.es.keys())):
            n = self.es[e]
            for k in range(n):
                for p in e.split(self.delimiter):
                    if self.filterPredicates(p) == False:
                        X[c][self.predicates[p]] = 1
                c += 1
        return X

    def EStoVecU(self):
        num = len(list(self.es.keys()))
        X = np.zeros((num, len(list(self.predicates.keys()))), dtype=np.uint8)
        supps, _ES = [], []
        c = 0
        for i, e in enumerate(list(self.es.keys())):
            for p in e.split(self.delimiter):
                if self.filterPredicates(p) == False:
                    X[c][self.predicates[p]] = 1
            supps.append(self.es[e])
            _ES.append(e)
            c += 1
        return X, supps, _ES

    def EStoVecUG(self, scale):
        num = len(list(self.es.keys()))
        E = np.zeros(len(list(self.predicates.keys())), dtype=np.uint8)
        X = []
        for i, e in enumerate(list(self.es.keys())):
            for p in e.split(self.delimiter):
                if self.filterPredicates(p) == False:
                    E[self.predicates[p]] = 1
            # dup
            n = self.es[e]
            n_sc = int(np.ceil(n / scale))
            for i in range(n_sc):
                X.append(copy.deepcopy(E))
        return np.array(X, dtype=np.uint8)

    def estimate_covariance(self, X, columns):
        pass

    def getPredicates(self):
        return [p for p in self.predicates.keys()]
