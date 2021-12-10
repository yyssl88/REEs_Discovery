import numpy as np
import pandas as pd

delimiter = ":"

def read_csv(data_file):
    return pd.read_csv(data_file)

def write_predicates(data_file, index, not_target_columns):
    f = open(data_file, 'w', encoding='utf-8')
    if(not_target_columns is not None and not_target_columns):
        columns = not_target_columns.split(',');
    for k, v in index.items():
        if(not (not_target_columns is not None and not_target_columns and is_not_target_predicate(k, columns))):
            s = k + delimiter
            for e in v:
                s += e + delimiter
            f.write(s + "\n")
            print(s)

def is_not_target_predicate(key, columns):
    for item in columns:
        if(item in key):
            return True
