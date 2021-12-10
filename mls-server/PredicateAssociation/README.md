# PredicateAssociation

```
# Given {Psel, next_p, reward}, train the DQN model
python train.py -init_train 1 -predicate_num 18 -sequence "9 5,3,1230;2 4,15,410;1 13,5,1604" -legal_nextP_indices "" 
```

```
# Given Psel, predict next_p with maximum predicted reward
python predict.py -init_train 0 -predicate_num 18 -sequence "9 5;2 4;1 13" -legal_nextP_indices "1 3 6 14;6 14 17;1 3 5 8 9"

# Given Psel and next_p, predict whether Psel can be expanded with next_p
python predict.py -init_train 0 -predicate_num 18 -sequence "9 5,3;2 4,15;1 13,5" -legal_nextP_indices "" 
```


```
# Given sequence file, train the RL model with sequences of size N
python train_from_file.py -data_name "airports" -N 300
```

-init_train : whether training model for the first time; 1/0

-predicate_num : the size of allPredicates, i.e., the dimension of state

-sequence : input string, of which the format is "Psel, p, reward; Psel, p, reward; ..."
- Use ';' to split different triples of input data;
- When all {Psel, p, reward} are not empty, it means to use the data to train DQN model, using *train.py*;
- When only all {p, reward} are empty, it means to predict next {p}, subject to 'legal_nextP_indices', using *predict.py*;
- When only all {reward} are empty, it means to predict whether {Psel} can be expanded with {p}, using *predict.py*.

-legal_nextP_indices : given {Psel}, predict next_p with maximum predicted reward to expand Psel. Meantime, next_p should appear in 'legal_nextP_indices'

-learning_rate : the parameter of DQN model

-reward_decay : the parameter of DQN model

-e_greedy : the parameter of DQN model

-replace_target_iter : the parameter of DQN model

-memory_size : the parameter of DQN model

-batch_size : the parameter of DQN model

-model_path : the path where trained model saved