import pandas as pd
import argparse
import logging
import sys
import pickle as plk
sys.path.append('../../REEs_model/')
sys.path.append('../../')
sys.path.append('../../../')
from sklearn.model_selection import train_test_split
from interestingnessFixedEmbedsWithObj import *
from REEs_model.utils import *

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

def main():
    parser = argparse.ArgumentParser(description="Learn the rule interestingness with fixed embeddings")
    parser.add_argument('-rules_file', '--rules_file', type=str, default='../REEs_model_data/Pod_test_data/rule2000/rules.csv')

    parser.add_argument('-lr', '--lr', type=float, default=0.001)
    parser.add_argument('-optionIfObj', '--optionIfObj', type=bool, default=True)
    parser.add_argument('-token_embed_dim', '--token_embed_dim', type=int, default=768)
    parser.add_argument('-hidden_size', '--hidden_size', type=int, default=300)
    parser.add_argument('-rees_embed_dim', '--rees_embed_dim', type=int, default=200)
    parser.add_argument('-epochs', '--epochs', type=int, default=300)
    parser.add_argument('-model_file', '--model_file', type=str, default='../REEs_model_data/Pod_test_data/rule2000/interestingness_model')
    parser.add_argument('-vobs_file', '--vobs_file', type=str, default='../REEs_model_data/Pod_test_data/rule2000/vobs.txt')
    parser.add_argument('-batch_size', '--batch_size', type=int, default=128)
    parser.add_argument('-predicates_path', '--predicates_path', type=str, default='../REEs_model_data/all_predicates.txt')
    # pretrained matrix
    parser.add_argument('-pretrained_matrix_file', '--pretrained_matrix_file', type=str, default='../REEs_model_data/all_predicates.txt')
    # score file
    parser.add_argument('-rule_interestingness_file', '--rule_interestingness_file', type=str, default='../REEs_model_data/all_predicates.txt')


    args = parser.parse_args()
    arg_dict = args.__dict__
    for k, v in sorted(arg_dict.items()):
        logging.info('[Argument] %s=%r' % (k, v))
        print("k:", k, ", v:", v)

    # data
    rules_data = pd.read_csv(arg_dict['rules_file'])
    rees_data = rules_data['rule'].values
    rees_obj = np.array(rules_data[['support_ratio', 'confidence', 'conciseness']].values)
    rees_obj[:, 0] = 1.0 / (1 + np.exp(-rees_obj[:, 0])) #/= 10000
    #rees_obj[:, 0] /= 100000.0
    tokenVobs = generateVobs(arg_dict['predicates_path'])
    rees_lhs, rees_rhs = processAllRules(rees_data, tokenVobs)
    print("The token vobs is ", tokenVobs)
    print('The first 10 data')
    print(rees_lhs[:5])
    print(rees_rhs[:5])

    token_embed_size = arg_dict['token_embed_dim'] #len(tokenVobs[PADDING_VALUE])

    vob_size = len(tokenVobs)
    print("The number of Tokens is ", vob_size)

    # save the token vobs
    f = open(arg_dict['vobs_file'], 'w')
    for k, v in tokenVobs.items():
        f.write(str(k) + " " + str(v))
        f.write("\n")
    f.close()

    # load pretrained matrix
    with open(arg_dict['pretrained_matrix_file'], 'rb') as f:
        pretrained_matrix_dict = plk.load(f)
    pretrained_matrix = np.zeros((vob_size, token_embed_size))
    for k, v in pretrained_matrix_dict.items():
        c = int(tokenVobs[k])
        print(c, v.shape)
        pretrained_matrix[c, :] = np.array(v)
    
    print(pretrained_matrix.shape)

    
    # model
    model = InterestingnessEmbedsWithObj(vob_size, token_embed_size, arg_dict['hidden_size'],
                                  arg_dict['rees_embed_dim'], MAX_LHS_PREDICATES,
                                  MAX_RHS_PREDICATES, rees_obj.shape[1], arg_dict['optionIfObj'], 
                                  arg_dict['lr'], arg_dict['epochs'], arg_dict['batch_size'], pretrained_matrix)

    model.loadModel(arg_dict['model_file'])
    scores = model.compute_interestingness(rees_lhs, rees_rhs, rees_obj)
    scores = np.array(scores)
    np.save(arg_dict['rule_interestingness_file'], scores)
 
if __name__ == '__main__':
    main()



