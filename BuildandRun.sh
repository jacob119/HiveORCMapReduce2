#!/usr/bin/env bash
# Running application after maven build.
mvn clean package -DskipTests=true
hadoop jar ./target/HiveORCMapReduce-1.0-SNAPSHOT.jar org.jacob.hivemr.Driver /wordcount/input /wordcount/output
