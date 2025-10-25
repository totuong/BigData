#!/usr/bin/env bash
set -euo pipefail

# ====== Hadoop Environment ======
export HADOOP_HOME=/usr/local/hadoop
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
# Đặt Hadoop PATH lên đầu để ưu tiên YARN của Hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$PATH

# ====== Stop Hadoop daemons ======
echo "[INFO] Stopping YARN daemons..."
$HADOOP_HOME/bin/yarn --daemon stop nodemanager || true
$HADOOP_HOME/bin/yarn --daemon stop resourcemanager || true

echo "[INFO] Stopping HDFS daemons..."
$HADOOP_HOME/bin/hdfs --daemon stop secondarynamenode || true
$HADOOP_HOME/bin/hdfs --daemon stop datanode || true
$HADOOP_HOME/bin/hdfs --daemon stop namenode || true

echo "====================================="
echo "✅ Hadoop stopped successfully."
echo "====================================="
