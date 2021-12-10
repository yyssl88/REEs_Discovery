import numpy as np
import sys
from basic import *


# sample from HDFS files

original_data_dir = sys.argv[1]
sample_data_dir = sys.argv[2]
sample_ratio = float(sys.argv[3])
sample_seed = int(sys.argv[4])


# uniform sample
uniform_sample(original_data_dir, sample_data_dir, sample_ratio, sample_seed)


