#!/usr/bin/env bash
set -euo pipefail
export HADOOP_HOME=/usr/local/hadoop
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin

sudo service ssh start || true

hdfs --daemon start namenode
hdfs --daemon start datanode
hdfs --daemon start secondarynamenode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager

jps || true
echo "NameNode UI: http://localhost:9870"
echo "YARN UI    : http://localhost:8088"
