#!/usr/bin/env bash
set -euo pipefail

# ========== CONFIG ==========
SPARK_VERSION="3.5.7"
SPARK_PACKAGE="spark-${SPARK_VERSION}-bin-hadoop3-scala2.13"
SPARK_TGZ="${SPARK_PACKAGE}.tgz"
SPARK_URL="https://downloads.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"
SPARK_HOME="/usr/local/spark"
HADOOP_HOME="/usr/local/hadoop"
JAVA_HOME_DEFAULT="/usr/lib/jvm/java-11-openjdk-amd64"

echo ">>> [1] Checking environment..."
if ! java -version &>/dev/null; then
  echo "Java not found. Installing OpenJDK 11..."
  sudo apt update && sudo apt install -y openjdk-11-jdk
fi

if [ ! -d "$HADOOP_HOME" ]; then
  echo "❌ Hadoop not found at $HADOOP_HOME. Please install Hadoop first!"
  exit 1
fi

echo ">>> [2] Downloading Spark ${SPARK_VERSION} ..."
cd ~
if [ ! -f "$SPARK_TGZ" ]; then
  wget -q --show-progress "$SPARK_URL"
else
  echo "Spark package already exists, skipping download."
fi

echo ">>> [3] Extracting Spark ..."
tar -xzf "$SPARK_TGZ"
sudo rm -rf "$SPARK_HOME"
sudo mv "$SPARK_PACKAGE" "$SPARK_HOME"

echo ">>> [4] Configuring environment variables..."
if ! grep -q "SPARK_HOME" ~/.bashrc; then
cat <<EOF >> ~/.bashrc

# ===== Spark + Hadoop environment =====
export SPARK_HOME=${SPARK_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export JAVA_HOME=${JAVA_HOME_DEFAULT}
export PATH=\$PATH:\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export SPARK_DIST_CLASSPATH=\$(\$HADOOP_HOME/bin/hadoop classpath)
export SPARK_LOCAL_IP=127.0.0.1
EOF
fi

source ~/.bashrc

echo ">>> [5] Configuring Spark conf files..."
sudo cp -f $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
sudo tee $SPARK_HOME/conf/spark-env.sh > /dev/null <<EOF
export JAVA_HOME=${JAVA_HOME_DEFAULT}
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export SPARK_DIST_CLASSPATH=\$(\$HADOOP_HOME/bin/hadoop classpath)
export SPARK_MASTER_HOST=localhost
EOF

sudo cp -f $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
sudo tee $SPARK_HOME/conf/spark-defaults.conf > /dev/null <<EOF
spark.master                     yarn
spark.hadoop.fs.defaultFS        hdfs://localhost:9000
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://localhost:9000/spark-logs
spark.history.fs.logDirectory    hdfs://localhost:9000/spark-logs
EOF

echo ">>> [6] Creating log directory on HDFS (if available)..."
hdfs dfs -mkdir -p /spark-logs || true

echo ">>> [7] Testing Spark installation..."
spark-shell --version || { echo "⚠️ Spark test failed"; exit 1; }

echo "✅ Spark ${SPARK_VERSION} installed successfully!"
echo "Run: spark-shell --master yarn"
