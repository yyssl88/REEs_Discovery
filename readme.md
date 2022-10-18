# REE Discovery
This project studies **Parallel Rule Discovery from Large Datasets by Sampling**.
To scale with large datasets, this
paper proposes a multi-round sampling strategy for rule discovery.
We consider entity enhancing rules (REEs) for collective entity resolution
and conflict resolution, which may carry constant patterns
and machine learning predicates. We sample large datasets with
accuracy bounds 𝛼 and 𝛽 such that at least 𝛼% of rules discovered
from samples are guaranteed to hold on the entire dataset (i.e., precision),
and at least 𝛽% of rules on the entire dataset can be mined
from the samples (i.e., recall). We also quantify the connection between
support and confidence of the rules on samples and their
counterparts on the entire dataset. To scale with the number of tuple
variables in collective rules, we adopt deep Q-learning to select
semantically relevant predicates. To improve the recall, we develop
a tableau method to recover constant patterns from the dataset. We
parallelize the algorithm such that it guarantees to reduce runtime
when more processors are used. Using real-life and synthetic data,
we empirically verify that the method speeds up REE discovery by
12.2 times with sample ratio 10% and recall 82%.

The codes mainly include two parts:
1. mls-server: rule discovery;  
2. REEs_model: DQN model;  

## Installation
Before building the projects, the following prerequisites need to be installed:
* Java JDK 1.8
* Maven ---
* 

## REEs_model
--- The source code of dynamic predicate filtering and rule interestingness

## mls-server    
This code is for REEs discovery.
Below we give a toy example.

1. Put the datasets into HDFS:
```
hdfs dfs -put airports.csv /data_path/
```
2. Then revise the data path in code:
```
set the path of line 2004 in src/main/java/sics/seiois/mlsserver/biz/mock/RuleFindRequestMock.java to be hdfs:///data_path/airports.csv
```
3. put the files related to DQN model into HDFS:
```
hdfs dfs mkdir -p /tmp/rulefind/DQNairports/
hdfs dfs -put airports_model.txt /tmp/rulefind/DQNairports/

hdfs dfs mkdir -p /tmp/rulefind/allPredicates/
hdfs dfs -put airports_predicates.txt /tmp/rulefind/allPredicates/
```
4. Download all the dependencies from Google Drive link:
https://drive.google.com/drive/folders/1xup0eVNB84BgJz3GSrFCy9X_bG2GF8Lm?usp=sharing, then move the directory lib/ into mls-server/example/:
```
cd mls-server/
mv lib/ example/
```
5. Compile and build the project:
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

# rule discovery on full data
./discovery_full.sh

# rule discovery on samples
./discovery_sampling.sh

# constant recovery after rule discovery on samples
./discovery_constantRecovery.sh
```
The results will be shown in discoveryResults/, as 'resRootFile' in run_unit_*.sh shows.

## Datasets
Only contain a small dataset Airport.

The others are in the following link:
https://drive.google.com/drive/folders/1oUv3tglQXjGdBWbmIwUMlsbexYYfplI-?usp=sharing