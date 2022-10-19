import numpy as np
import pandas as pd
import sys

data_file = sys.argv[1]
training_ratio = float(sys.argv[2])

#rule_pair_set_path = os.path.join(data_prefix, datasetName, 'labeled_data.txt')
rule_pair_set_path = data_file

rules_pair_ids_set = pd.read_csv(rule_pair_set_path, delimiter=" ")

schemas = list(rules_pair_ids_set.columns)
#rules_pair_ids_set
rules_pair_ids_set = rules_pair_ids_set.values

pairs_num = len(rules_pair_ids_set)

scripts = np.arange(pairs_num) 
np.random.seed(20)
np.random.shuffle(scripts)
validation_ratio = (1 - training_ratio) / 2
training_num, validation_num = int(training_ratio * pairs_num), int(validation_ratio * pairs_num)
scripts_train = scripts[:training_num]
scripts_valid = scripts[training_num: training_num + validation_num]
scripts_test = scripts[training_num + validation_num:]

train, valid, test = rules_pair_ids_set[scripts_train], rules_pair_ids_set[scripts_valid], rules_pair_ids_set[scripts_test]
train_df = pd.DataFrame(train, columns=schemas)
valid_df = pd.DataFrame(valid, columns=schemas)
test_df = pd.DataFrame(test, columns=schemas)

train_df.to_csv(data_file + '_train.csv', index=False)
valid_df.to_csv(data_file + '_valid.csv', index=False)
test_df.to_csv(data_file + '_test.csv', index=False)
