import pandas as pd
import argparse
import logging
import sys
sys.path.append('../../REEs_model/')
from sklearn.model_selection import train_test_split
from interestingnessFixedEmbeds import *
from predicateAgentInterestingness import Predicate
from REEs_model.utils import *

def main():
    parser = argparse.ArgumentParser(description="Learn the rule interestingness with fixed embeddings")
    parser.add_argument('-rules_file', '--rules_file', type=str, default='../REEs_model_data/Pod_test_data/rule2000/rules.csv')
    parser.add_argument('-train_file', '--train_file', type=str, default='../REEs_model_data/Pod_test_data/rule2000/train.csv')
    parser.add_argument('-test_file', '--test_file', type=str,
                    default='../REEs_model_data/Pod_test_data/rule2000/test.csv')

    parser.add_argument('-lr', '--lr', type=float, default=0.001)
    parser.add_argument('-token_embed_dim', '--token_embed_dim', type=int, default=100)
    parser.add_argument('-hidden_size', '--hidden_size', type=int, default=100)
    parser.add_argument('-rees_embed_dim', '--rees_embed_dim', type=int, default=50)
    parser.add_argument('-epochs', '--epochs', type=int, default=300)
    parser.add_argument('-model_file', '--model_file', type=str, default='../REEs_model_data/Pod_test_data/rule2000/interestingness_model')
    parser.add_argument('-model_txt_file', '--model_txt_file', type=str, default='../REEs_model_data/Pod_test_data/rule2000/interestingness_model')
    parser.add_argument('-vobs_file', '--vobs_file', type=str, default='../REEs_model_data/Pod_test_data/rule2000/vobs.txt')
    parser.add_argument('-batch_size', '--batch_size', type=int, default=128)
    parser.add_argument('-all_predicates_file', '--all_predicates_file', type=str, default='../REEs_model_data/all_predicates.txt')

    args = parser.parse_args()
    arg_dict = args.__dict__
    for k, v in sorted(arg_dict.items()):
        logging.info('[Argument] %s=%r' % (k, v))
        print("k:", k, ", v:", v)

    # data
    train_data = pd.read_csv(arg_dict['train_file'])
    test_data = pd.read_csv(arg_dict['test_file'])
    rees_data = pd.read_csv(arg_dict['rules_file'])['rule'].values
    tokenVobs = generateVobs(arg_dict['all_predicates_file'])
    rees_lhs, rees_rhs = processAllRules(rees_data, tokenVobs)
    print("The token vobs is ", tokenVobs)
    print('The first 10 data')
    print(rees_lhs[:5])
    print(rees_rhs[:5])
    # split train and valid
    train_data, valid_data = train_test_split(train_data, train_size=0.9, random_state=42)
    train_pair_ids, train_labels = train_data[['left_id', 'right_id']].values, train_data['label'].values
    valid_pair_ids, valid_labels = valid_data[['left_id', 'right_id']].values, valid_data['label'].values
    test_pair_ids, test_labels = test_data[['left_id', 'right_id']].values, test_data['label'].values
    train_labels = convert_to_onehot(train_labels)
    valid_labels = convert_to_onehot(valid_labels)
    test_labels = convert_to_onehot(test_labels)

    token_embed_size = arg_dict['token_embed_dim'] #len(tokenVobs[PADDING_VALUE])

    vob_size = len(tokenVobs)
    print("The number of Tokens is ", vob_size)

    # save the token vobs
    f = open(arg_dict['vobs_file'], 'w')
    for k, v in tokenVobs.items():
        f.write(str(k) + " " + str(v))
        f.write("\n")
    f.close()

    
    # model
    model = InterestingnessEmbeds(vob_size, token_embed_size, arg_dict['hidden_size'],
                                  arg_dict['rees_embed_dim'], MAX_LHS_PREDICATES,
                                  MAX_RHS_PREDICATES, arg_dict['lr'], arg_dict['epochs'], arg_dict['batch_size'])


    # train
    model.train(rees_lhs, rees_rhs, train_pair_ids, train_labels, valid_pair_ids, valid_labels)
    # evaluate
    model.evaluate(rees_lhs,rees_rhs, test_pair_ids, test_labels)
    # save the rule interestingness model in TensorFlow type
    model.saveModel(arg_dict['model_file'])
    # save the rule interestingness model in TXT type
    model.saveModelToText(arg_dict['model_txt_file'])
    
 
if __name__ == '__main__':
    main()



