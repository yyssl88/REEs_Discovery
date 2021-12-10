import numpy as np
import argparse
import logging
import os

from func import *


def main():
    parser = argparse.ArgumentParser(description="online random walk sampling")
    parser.add_argument('-d', '--data_dir')
    parser.add_argument('-r', '--sample_ratio', type=float, default=0.1)
    parser.add_argument('-m', '--max_tuple_ree', type=int, default=2)
    parser.add_argument('-s', '--seed', type=int, default=1234567)
    parser.add_argument('-o', '--output_dir')

    args = parser.parse_args()
    arg_dict = args.__dict__
    for k, v in sorted(arg_dict.items()):
        logging.info('[Argument] %s=%r' % (k, v))

    try:
        # 1. load datasets
        datasets, schemas, relations, tidStarts = loadDataFromDir(args['data_dir'])
        # 2. construct PLIs and ML results
        plis = constructPLIs(datasets, schemas, relations, tidStarts)
        # 3. random walk
        selectedTupleIDs = randomWalkSampling(datasets, tidStarts, plis, args['sample_ratio'], args['max_tuple_ree'])
        # 4. generate the sample data
        samples = genSample(selectedTupleIDs, datasets, tidStarts)
        # 5. save to dir
        saveSample(args['output_dir'], samples, schemas, relations)

    except:
        logging.exception('Exception occurred')
    finally:
        logging.info('[log]')

if __name__ == '__main__':
    main()
