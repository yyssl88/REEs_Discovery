# REE Discovery
This project studies two questions about rule discovery. Can we characterize the usefulness of rules using quantitative criteria? How can we discover rules using those criteria? As a testbed, we consider entity enhancing rules (REEs), which subsume common association rules and data quality rules as special cases. We characterize REEs using a bi-criteria model, with both objective measures such as support and confidence, and subjective measures for the user’s needs; we learn the subjective measure and the weight vectors via active learning. Based on the bi-criteria model, we develop a top-𝑘 algorithm to discover top-ranked REEs, and an any-time algorithm for successive discovery via lazy evaluation. 

For more details, see the **Discovering Top-k Rules using Subjective and Objective Criteria** paper. 

The codes mainly include two parts:
1. mls-server: rule discovery;  
2. REEs_model: interestingness model;  

## Installation
Before building the projects, the following prerequisites need to be installed:
* Java JDK 1.8
* Maven
* transformers
* tensorflow 2.6.2
* pytorch 1.10.2 
* huggingface

## REEs_model
--- The source code of dynamic predicate filtering and rule interestingness

#### 1. Rules and data used in the paper.
e.g., Airports dataset
```
ls ./REEs_model_data/revision/labeled_data_400/airports/

airports_all_predicates.txt  airports_rules.txt  train/  train_0/  train_1/  train_2/  train_3/  train_4/

ls ./REEs_model_data/revision/labeled_data_400/airports/train/

all_predicates.txt  rules.txt  tokenVobs.txt
```
Folders train_0 to train_4 contains 5 training, validation and testing data. The final accuracy is the average of the 5 testing data files.

The file 'rules.txt' is the set of rules used in the paper for interestingness model training.

For other datasets, simply replace airports with aminer, hospital and ncvoter.

#### 2. Run the interestingness model

##### 1) Our model
```
cd ./REEs_model/shell/revision
./interestingness_ours.sh ${tid} ${cid} ${dir_path}
```
Here the arguments are described as follows.

- **tid** is 0-4, representing Airports, Hospital, NCVoter, Inspection, and DBLP separately.
- **cid** is 0-4, representing train_0, train_1, train_2, train_3, train_4.
- **dir_path** is the absolute path of REEs_model in the local settings.

The results will be saved in './REEs_model_data/revision/results/'

##### 2) Other baselines
```
cd ./REEs_model/shell/revision
./bert_interestingness.sh ${tid} ${cid} ${dir_path} ${cuda}             # for bert
./bert_mlm_interestingness.sh ${tid} ${cid} ${dir_path} ${cuda}         # for bert mlm
./interestingness_transformer.sh ${tid} ${cid} ${dir_path} ${cuda}      # transformer
./interestingness_objML.sh                                              # NoSub
```
where **cuda** refers to the gpu id

#### 3. Active learning (AL)
```
cd ./REEs_model/shell/revision
./interestingness_ours_active_learning.sh ${tid} ${dir_path}
```
The AL procedure involved user interaction, i.e., labeling 40 pairs of rules recommended by our model in each round.

The user interaction is as follows.
```
###################################################################################
Rule 0 LHS : hospital(t0) ⋀ hospital(t1) ⋀  t1.Condition = 'Surgical Infection Prevention' ⋀  t0.State = t1.State ⋀  t0.County_Name = t1.County_Name ⋀  t0.Hospital_Owner = t1.Hospital_Owner ⋀  t0.Emergency_Service = t1.Emergency_Service ⋀  t0.Sample = t1.Sample  ->  t0.Provider_Number = t1.Provider_Number
, RHS : [1, 6, 3, 2, 6]
 OBJ : [9.38637479e-07 8.50089151e-01 1.66666667e-01]

-----------------------------------------------------------------------------------
Rule 1 LHS : hospital(t0) ⋀ hospital(t1) ⋀  t0.Hospital_Type = 'Critical Access Hospitals' ⋀  t1.Emergency_Service = 'Yes' ⋀  t0.State = t1.State ⋀  t0.County_Name = t1.County_Name ⋀  t0.Hospital_Type = t1.Hospital_Type ⋀  t0.Sample = t1.Sample  ->  t1.Hospital_Owner = 'Voluntary non-profit - Church'
, RHS : [2, 19, 3, 2, 28]
 OBJ : [1.07978142e-07 8.50327967e-01 1.66666667e-01]


Which one is more interesting ? 
```
The user inputs 0 or 1 to select the most interesting rule from the above two.

After 4 rounds interaction, our model could learn better users' preference.

## mls-server    
This code is for REEs discovery.
Below we give a toy example.

1. Put the datasets into HDFS:
```
hdfs dfs -mkdir /tmp/datasets_discovery/
hdfs dfs -put airports.csv /tmp/datasets_discovery/
```

2. Put the files related to interestingness model, from datasets/, into HDFS:
```
hdfs dfs mkdir -p /tmp/rulefind/interestingness/airports_topk/
hdfs dfs -put tokenVobs.txt /tmp/rulefind/interestingness/airports_topk/
hdfs dfs -put interestingnessModel.txt /tmp/rulefind/interestingness/airports_topk/
hdfs dfs -put filterRegressionModel.txt /tmp/rulefind/interestingness/airports_topk/
hdfs dfs -put airports_predicates.txt /tmp/rulefind/interestingness/airports_topk/
```
3. Download all the dependencies (https://drive.google.com/drive/folders/1Gviqt7zcaRGQho4x5i6hPnuwPmWonWFR?usp=sharing), then move the directory lib/ into mls-server/example/:
```
cd mls-server/
mv lib/ example/
```
4. Compile and build the project:
```
mvn package
```
Then move and replace the **mls-server-0.1.1.jar** from mls-server/target/ to example/lib/:
```
mv target/mls_server-0.1.1.jar example/lib/
```
5. After all these preparation, run the toy example:
```
cd example/scripts/
./discovery.sh
```
The results will be shown in discoveryResults/, as 'resRootFile' in run_unit.sh shows.

## Datasets
Only contain a small dataset Airport.

The others are released in https://drive.google.com/drive/folders/1Gviqt7zcaRGQho4x5i6hPnuwPmWonWFR?usp=sharing

