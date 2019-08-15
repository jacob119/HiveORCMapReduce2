#!/usr/bin/env bash
# Running application after maven build.
mvn clean package -DskipTests=true
# hadoop jar ./target/HiveORCMapReduce-1.0-SNAPSHOT.jar org.jacob.hivemr.Driver /wordcount/input /wordcount/output
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home
export HADOOP_IDENT_STRING=$USER
export TEZ_JARS=/Users/jacob/dev/apache-tez-0.7.0-src/tez-dist/target/tez-0.7.0
export TEZ_CONF_DIR=/Users/jacob/dev/apache-tez-0.7.0-src/tez-dist/target/tez-0.7.0
export HIVE_HOME=/Users/jacob/dev/hive
export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${TEZ_CONF_DIR}:${TEZ_JARS}/*:${TEZ_JARS}/lib/*:${HIVE_HOME}/lib/*

echo $HADOOP_CLASSPATH
# hadoop jar ./target/HiveORCMapReduce-1.0-SNAPSHOT.jar org.jacob.hivemr.Driver /wordcount/input /wordcount/output
hadoop jar ./target/HiveORCMapReduce-1.0-SNAPSHOT.jar org.jacob.hivemr.ORCReaderDriver \
      /user/hive/warehouse/carrier_code_orc  /wordcount/output
