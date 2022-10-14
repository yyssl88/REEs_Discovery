#!/bin/bash

PWD=`pwd`
for i in ./lib/*;
do CLASSPATH=$PWD/$i:"$CLASSPATH";
done


export JAVA_HOME='/opt/jdk1.8.0_231/'
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_HOME='/usr/hdp/3.1.0.0-78/spark2'
export CLASSPATH=".:./conf:/etc/hadoop/conf:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar:$CLASSPATH"

java -cp $CLASSPATH   sics.seiois.mlsserver.service.impl.RuleFinder   $*
