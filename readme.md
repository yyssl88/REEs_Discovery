# REE Discovery
This project studies two questions about rule discovery. Can we
characterize the usefulness of rules using quantitative criteria? How
can we discover rules using those criteria? As a testbed, we consider
entity enhancing rules (REEs), which subsume common association
rules and data quality rules as special cases. We characterize REEs
using a bi-criteria model, with both objective measures such as
support and confidence, and subjective measures for the user’s
needs; we learn the subjective measure and the weight vectors via
active learning. Based on the bi-criteria model, we develop a top-𝑘
algorithm to discover top-ranked REEs, and an any-time algorithm
for successive discovery via lazy evaluation. 

The codes mainly include two parts:
1. mls-server: rule discovery;  
2. REEs_model: interestingness model;  

## Installation
Before building the projects, the following prerequisites need to be installed:
* Java JDK 1.8
* Maven ---
* 

## REEs_model
--- The source code of dynamic predicate filtering and rule interestingness

## mls-server    
This code is for REEs discovery.
Below we give an toy example.

1. Put the datasets into HDFS:
```
hdfs dfs -put airports.csv /data_path/
```
2. Then revise the data path in code:
```
set the path of line 2004 in src/main/java/sics/seiois/mlsserver/biz/mock/RuleFindRequestMock.java to be hdfs:///data_path/airports.csv
```
3. put the files related to interestingness model into HDFS:
```
hdfs dfs mkdir -p /tmp/rulefind/interestingness/airports_topk/
hdfs dfs -put tokenVobs.txt /tmp/rulefind/interestingness/airports_topk/
hdfs dfs -put interestingnessModel.txt /tmp/rulefind/interestingness/airports_topk/
hdfs dfs -put filterRegressionModel.txt /tmp/rulefind/interestingness/airports_topk/
hdfs dfs -put airports_predicates.txt /tmp/rulefind/interestingness/airports_topk/
```
4. Compile and build applications with IDE, such as IntelliJ IDEA. Choose *sics.seiois.mlsserver.service.impl.RuleFinder* as the main class.
If your path is, for example, *mls-server/out/artifacts/mls_server/*, then put all the *.jar files from this path into [example/lib/](https://github.com/yyssl88/REEs_Discovery/tree/top-k/mls-server/example/lib/)
5. After all these preparation, run the toy example:
```
cd example/scripts/
./discovery.sh
```
The results will be shown in discoveryResults/, as 'resRootFile' in run_unit.sh shows.

## Datasets
Only contain a small dataset Airport.

The others are in the following link:
https://drive.google.com/drive/folders/1oUv3tglQXjGdBWbmIwUMlsbexYYfplI-?usp=sharing