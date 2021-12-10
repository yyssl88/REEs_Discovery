import numpy as np
import argparse
import logging
import json
from predicates import *
from utility import *
from collections import defaultdict
from sklearn.covariance import GraphicalLasso, graphical_lasso
from sklearn import covariance, preprocessing

def main():
    parser = argparse.ArgumentParser(description='covariance computation')
    parser.add_argument('-d', '--data_path')
    parser.add_argument('-t', '--threshold', type=float, default=0.0)
    parser.add_argument('-k', '--topK', type=int, default=90)
    parser.add_argument('-o', '--index_file')
    parser.add_argument('-u', '--not_target_columns')
    parser.add_argument('-s', '--scale', type=float, default=100.0)
    parser.add_argument('-a', '--alpha', type=float, default=0.001)
    parser.add_argument('-m', '--max_iter', type=int, default=2000)
    args = parser.parse_args()
    arg_dict = args.__dict__
    for k, v in sorted(arg_dict.items()):
        logging.info('[Argument] %s=%r' % (k, v))

    try:
        ps = Predicates(arg_dict['data_path'])
        X = ps.EStoVec()
        #X = ps.EStoVecUG(arg_dict['scale'])
        columns = ps.getPredicates()
        X = np.array(X, dtype=np.float32)
        logging.info(X.shape)
        myScaler = preprocessing.StandardScaler()
        X = myScaler.fit_transform(X)
        logging.info(X)
        emp_cov = covariance.empirical_covariance(X)
        logging.info('emp : ', emp_cov, emp_cov.shape)
        shrunk_cov = covariance.shrunk_covariance(emp_cov, shrinkage=0.1)

        #print(cov, cov.shape, X.shape)
        #model = GraphicalLasso(alpha=0.001, mode='cd', tol=1e-4, max_iter=5000)
        #cov = model.fit(X)
        est_cov, inv_cov = graphical_lasso(shrunk_cov, alpha=arg_dict['alpha'], mode='cd', max_iter=arg_dict['max_iter'])
        prec = np.abs(inv_cov) # model.get_precision()

        logging.info('The precision matrix is : ')
        logging.info(prec)

        # inverted index
        index = defaultdict(list)
        for c, p in enumerate(columns):
            for i in range(len(prec)):
                if prec[c][i] > arg_dict['threshold']:
                    index[p].append([columns[i], prec[c][i]])

        # sort
        index_ = defaultdict(list)
        for k, v in index.items():
            v_ = sorted(v, key=lambda x: x[1], reverse=1)
            index_[k] = [e[0] for e in v_[:arg_dict['topK']]]
        index = index_

        # save heatmap to file
        #np.save(arg_dict['heatmap_file'], np.array(prec))
        #np.save(arg_dict['columns_file'], np.array(columns))

        # save
        #f = open(arg_dict['index_file'] + '.json', 'w')
        #json.dump(index, f)
        write_predicates(arg_dict['index_file'], index, arg_dict['not_target_columns'])
        logging.info('success')

    except:
        logging.exception('Exception occured')
    finally:
        logging.info('[log]')

if __name__ == '__main__':
    main()
