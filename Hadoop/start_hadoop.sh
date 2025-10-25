#!/usr/bin/env bash
set -euo pipefail

# ====== Hadoop Environment ======
export HADOOP_HOME=/usr/local/hadoop
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$PATH

# ====== Start SSH (needed by Hadoop daemons) ======
echo "[INFO] Starting SSH service..."
sudo service ssh start || true

# ====== Start HDFS daemons ======
echo "[INFO] Starting HDFS daemons..."
$HADOOP_HOME/bin/hdfs --daemon start namenode
$HADOOP_HOME/bin/hdfs --daemon start datanode
$HADOOP_HOME/bin/hdfs --daemon start secondarynamenode

# ====== Start YARN daemons ======
echo "[INFO] Starting YARN daemons..."
$HADOOP_HOME/bin/yarn --daemon start resourcemanager
$HADOOP_HOME/bin/yarn --daemon start nodemanager

# ====== Check Java Processes ======
echo "[INFO] Active Java processes:"
jps || true

echo "======================================"
echo "✅ NameNode UI: http://localhost:9870"
echo "✅ YARN UI    : http://localhost:8088"
echo "======================================"
