import numpy as np
import argparse
import logging
import json
import os
from predicates import *
from utility import *
from collections import defaultdict
from sklearn.cluster import KMeans

def containZero(supp_allo):
    for e in supp_allo:
        if e <= 0:
            return True
    return False

def main():
    parser = argparse.ArgumentParser(description='evidence set partition')
    parser.add_argument('-n', '--cluster_num', type=int, default=5)
    parser.add_argument('-d', '--data_path')
    parser.add_argument('-s', '--support_ratio', type=float, default=0.01)
    parser.add_argument('-o', '--output_dir')
    args = parser.parse_args()
    arg_dict = args.__dict__
    for k, v in sorted(arg_dict.items()):
        logging.info('[Argument] %s=%r' % (k, v))

    try:
        ps = Predicates(arg_dict['data_path'])
        X, supps, _ES = ps.EStoVecU()
        print('supps: ', supps, np.sum(supps))
        kmeans = KMeans(n_clusters=arg_dict['cluster_num'], random_state=0).fit(X)
        supp_tau = np.ceil(np.sum(supps) * arg_dict['support_ratio'])
        supp_all = np.sum(supps)

        # collect results
        es_parts = [[] for i in range(arg_dict['cluster_num'])]
        for eid, label in enumerate(list(kmeans.labels_)):
            es_parts[label].append([X[eid], supps[eid], _ES[eid]])

        # allocate thresholds
        supp_allo = []
        for i in range(arg_dict['cluster_num']):
            local_supp = np.sum([es_parts[i][j][1] for j in range(len(es_parts[i]))])
            #print(local_supp, supp_tau)
            supp_allo.append(int(np.ceil(local_supp * 1.0 / supp_all * supp_tau)))

        print('supps allocation : ', supp_allo)
        # prevent zero support
        while containZero(supp_allo):
            min_sc = np.argmin(supp_allo)
            max_sc = np.argmax(supp_allo)
            supp_allo[min_sc] += 1
            supp_allo[max_sc] -= 1

        # save results
        es_parts_file = []
        for cid in range(arg_dict['cluster_num']):
            es_file = os.path.join(arg_dict['output_dir'], 'evidenceset_part' + str(cid) + '.txt')
            f = open(es_file, 'w')
            for info in es_parts[cid]:
                _es_ = info[-1] + 'SUPP' + str(info[1])
                f.write(_es_)
                f.write('\n')
            f.close()
            es_parts_file.append(es_file)

        es_parts_path = os.path.join(arg_dict['output_dir'], 'evidenceset.txt')
        f = open(es_parts_path, 'w')
        for _file_, _supp_ in zip(es_parts_file, supp_allo):
            f.write(_file_ + 'SUPP' + str(_supp_))
            f.write('\n')
        f.close()

    except:
        logging.exception('Exception occured')
    finally:
        logging.info('[log]')

if __name__ == '__main__':
    main()

