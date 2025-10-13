#!/usr/bin/env bash
set -euo pipefail
export HADOOP_HOME=/usr/local/hadoop
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin

yarn --daemon stop nodemanager || true
yarn --daemon stop resourcemanager || true
hdfs --daemon stop secondarynamenode || true
hdfs --daemon stop datanode || true
hdfs --daemon stop namenode || true

echo "Hadoop stopped."
