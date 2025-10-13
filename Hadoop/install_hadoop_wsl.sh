#!/usr/bin/env bash
set -euo pipefail

# ========== CONFIG ==========
HADOOP_VERSION="${HADOOP_VERSION:-3.3.6}"
HADOOP_TGZ="hadoop-${HADOOP_VERSION}.tar.gz"
HADOOP_HOME="/usr/local/hadoop"
JAVA_HOME_DEFAULT="/usr/lib/jvm/java-11-openjdk-amd64"
NN_DIR="/usr/local/hadoop_data/hdfs/namenode"
DN_DIR="/usr/local/hadoop_data/hdfs/datanode"

echo ">>> [0] Pre-flight checks"
if ! grep -qi "microsoft" /proc/version 2>/dev/null; then
  echo "This installer is intended for WSL2 on Windows (Ubuntu). Proceeding anyway..."
fi
if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required"; exit 1
fi

echo ">>> [1] Update & install packages (OpenJDK 11, SSH, tools)"
sudo apt update -y
sudo apt install -y openjdk-11-jdk openssh-server wget tar curl coreutils procps lsof net-tools

echo ">>> [2] Ensure SSH localhost works"
if ! systemctl is-active --quiet ssh 2>/dev/null; then
  sudo service ssh start || true
fi
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
if [ ! -f "$HOME/.ssh/id_rsa.pub" ]; then
  ssh-keygen -t rsa -P "" -f "$HOME/.ssh/id_rsa"
fi
touch "$HOME/.ssh/authorized_keys"
grep -q "$(cat "$HOME/.ssh/id_rsa.pub")" "$HOME/.ssh/authorized_keys" || cat "$HOME/.ssh/id_rsa.pub" >> "$HOME/.ssh/authorized_keys"
chmod 600 "$HOME/.ssh/authorized_keys"

echo ">>> [3] Download & install Hadoop ${HADOOP_VERSION}"
if [ ! -d "$HADOOP_HOME" ]; then
  cd /usr/local
  if [ ! -f "$HADOOP_TGZ" ]; then
    sudo wget -q "https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_TGZ}"
  fi
  sudo tar -xzf "$HADOOP_TGZ"
  sudo mv "hadoop-${HADOOP_VERSION}" hadoop
  sudo chown -R "$USER:$USER" "$HADOOP_HOME"
fi

echo ">>> [4] Setup environment variables in ~/.bashrc (idempotent)"
append_if_missing() { local l="$1"; local f="$2"; grep -qxF "$l" "$f" || echo "$l" >> "$f"; }
BASHRC="$HOME/.bashrc"
append_if_missing "" "$BASHRC"
append_if_missing "# >>> Hadoop env (auto-added)" "$BASHRC"
append_if_missing "export HADOOP_HOME=$HADOOP_HOME" "$BASHRC"
append_if_missing "export HADOOP_INSTALL=\$HADOOP_HOME" "$BASHRC"
append_if_missing "export HADOOP_MAPRED_HOME=\$HADOOP_HOME" "$BASHRC"
append_if_missing "export HADOOP_COMMON_HOME=\$HADOOP_HOME" "$BASHRC"
append_if_missing "export HADOOP_HDFS_HOME=\$HADOOP_HOME" "$BASHRC"
append_if_missing "export YARN_HOME=\$HADOOP_HOME" "$BASHRC"
append_if_missing "export HADOOP_COMMON_LIB_NATIVE_DIR=\$HADOOP_HOME/lib/native" "$BASHRC"
append_if_missing "export JAVA_HOME=${JAVA_HOME_DEFAULT}" "$BASHRC"
append_if_missing "export PATH=\$PATH:\$HADOOP_HOME/sbin:\$HADOOP_HOME/bin:\$JAVA_HOME/bin" "$BASHRC"
# shellcheck disable=SC1090
source "$BASHRC"

echo ">>> [5] Configure Hadoop XMLs"
sudo mkdir -p "$NN_DIR" "$DN_DIR"
sudo chown -R "$USER:$USER" /usr/local/hadoop_data

cd "$HADOOP_HOME/etc/hadoop"

cat > core-site.xml <<'XML'
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
XML

cat > hdfs-site.xml <<XML
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:${NN_DIR}</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:${DN_DIR}</value>
  </property>
</configuration>
XML

[ -f mapred-site.xml ] || cp mapred-site.xml.template mapred-site.xml
cat > mapred-site.xml <<'XML'
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
XML

cat > yarn-site.xml <<'XML'
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
XML

# Ensure Hadoop sees JAVA_HOME even in non-interactive shells
sudo sed -i '1i export HADOOP_HOME=/usr/local/hadoop' hadoop-env.sh
sudo sed -i 's|^#\?export JAVA_HOME=.*|export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64|' hadoop-env.sh

echo ">>> [6] Format NameNode if needed"
if [ ! -d "${NN_DIR}/current" ]; then
  "$HADOOP_HOME/bin/hdfs" namenode -format -force -nonInteractive
else
  echo "NameNode already formatted. Skipping."
fi

echo ">>> [7] Start HDFS + YARN (daemon mode, no SSH env issues)"
"$HADOOP_HOME/bin/hdfs" --daemon start namenode
"$HADOOP_HOME/bin/hdfs" --daemon start datanode
"$HADOOP_HOME/bin/hdfs" --daemon start secondarynamenode
"$HADOOP_HOME/bin/yarn" --daemon start resourcemanager
"$HADOOP_HOME/bin/yarn" --daemon start nodemanager

echo ">>> [8] Smoke test"
echo "Hello Hadoop" > /tmp/hello.txt
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /user/$USER
"$HADOOP_HOME/bin/hdfs" dfs -put -f /tmp/hello.txt /user/$USER/
"$HADOOP_HOME/bin/hdfs" dfs -ls -R /user/$USER/ | tail -n 5

echo "============================================================"
echo " Hadoop ${HADOOP_VERSION} installed & started successfully!"
echo " NameNode UI         : http://localhost:9870"
echo " DataNode UI         : http://localhost:9864"
echo " YARN ResourceManager: http://localhost:8088"
echo " HDFS home (/user/$USER) has hello.txt uploaded."
echo " To stop:  ./stop_hadoop.sh"
echo " To start: ./start_hadoop.sh"
echo "============================================================"
