#!/usr/bin/env bash
set -euo pipefail

# ================= CONFIG =================
HADOOP_VERSION="${HADOOP_VERSION:-3.3.6}"
HADOOP_TGZ="hadoop-${HADOOP_VERSION}.tar.gz"
HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
JAVA_HOME_DEFAULT="/usr/lib/jvm/java-11-openjdk-amd64"
NN_DIR="${NN_DIR:-/usr/local/hadoop_data/hdfs/namenode}"
DN_DIR="${DN_DIR:-/usr/local/hadoop_data/hdfs/datanode}"

# ================= SAFETY CHECKS =================
if [ "$(id -u)" -eq 0 ]; then
  echo "⚠️  Không chạy script bằng root. Hãy dùng user thường, script sẽ tự sudo khi cần."
  exit 1
fi

echo ">>> [1] Cài gói cần thiết (OpenJDK 11, SSH, tools)"
sudo apt update -y
sudo apt install -y openjdk-11-jdk openssh-server wget curl tar procps lsof net-tools coreutils

echo ">>> [2] Bật SSH localhost (Hadoop đôi khi cần)"
sudo service ssh start || true
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
if [ ! -f "$HOME/.ssh/id_rsa.pub" ]; then
  ssh-keygen -t rsa -P "" -f "$HOME/.ssh/id_rsa"
fi
touch "$HOME/.ssh/authorized_keys"
grep -q "$(cat "$HOME/.ssh/id_rsa.pub")" "$HOME/.ssh/authorized_keys" || cat "$HOME/.ssh/id_rsa.pub" >> "$HOME/.ssh/authorized_keys"
chmod 600 "$HOME/.ssh/authorized_keys"

echo ">>> [3] Xác định JAVA_HOME"
JAVA_HOME="${JAVA_HOME_DEFAULT}"
if [ ! -x "${JAVA_HOME}/bin/java" ]; then
  JAVA_PATH="$(readlink -f "$(which java)" | sed 's|/bin/java||')"
  if [ -x "${JAVA_PATH}/bin/java" ]; then JAVA_HOME="${JAVA_PATH}"; fi
fi
echo "    JAVA_HOME=${JAVA_HOME}"

echo ">>> [4] Tải & giải nén Hadoop ${HADOOP_VERSION} (Apache chính thức)"
if [ ! -d "${HADOOP_HOME}" ]; then
  sudo mkdir -p "$(dirname "${HADOOP_HOME}")"
  sudo chown -R "$USER:$USER" "$(dirname "${HADOOP_HOME}")"
  wget -q "https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_TGZ}" -O "/tmp/${HADOOP_TGZ}"
  tar -xzf "/tmp/${HADOOP_TGZ}" -C "$(dirname "${HADOOP_HOME}")"
  mv "$(dirname "${HADOOP_HOME}")/hadoop-${HADOOP_VERSION}" "${HADOOP_HOME}"
  sudo chown -R "$USER:$USER" "${HADOOP_HOME}"
else
  echo "    Hadoop đã tồn tại ở ${HADOOP_HOME}, bỏ qua bước tải."
fi

echo ">>> [5] Thêm biến môi trường vào ~/.bashrc (idempotent)"
append_if_missing() { local l="$1"; local f="$2"; grep -qxF "$l" "$f" || echo "$l" >> "$f"; }
BASHRC="$HOME/.bashrc"
append_if_missing "" "$BASHRC"
append_if_missing "# >>> Hadoop Environment (auto-added)" "$BASHRC"
append_if_missing "export HADOOP_HOME=${HADOOP_HOME}" "$BASHRC"
append_if_missing "export HADOOP_INSTALL=\$HADOOP_HOME" "$BASHRC"
append_if_missing "export HADOOP_COMMON_HOME=\$HADOOP_HOME" "$BASHRC"
append_if_missing "export HADOOP_HDFS_HOME=\$HADOOP_HOME" "$BASHRC"
append_if_missing "export HADOOP_MAPRED_HOME=\$HADOOP_HOME" "$BASHRC"
append_if_missing "export YARN_HOME=\$HADOOP_HOME" "$BASHRC"
append_if_missing "export HADOOP_COMMON_LIB_NATIVE_DIR=\$HADOOP_HOME/lib/native" "$BASHRC"
append_if_missing "export JAVA_HOME=${JAVA_HOME}" "$BASHRC"
append_if_missing "export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$JAVA_HOME/bin" "$BASHRC"

# Nạp env ngay cho phiên hiện tại (để format/khởi động không lỗi)
export HADOOP_HOME JAVA_HOME
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"

echo ">>> [6] Tạo thư mục dữ liệu HDFS & quyền"
sudo mkdir -p "$NN_DIR" "$DN_DIR"
sudo chown -R "$USER:$USER" /usr/local/hadoop_data

echo ">>> [7] Ghi cấu hình Hadoop XML cơ bản"
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

# Đảm bảo hadoop-env.sh có JAVA_HOME & HADOOP_HOME
if ! grep -q '^export HADOOP_HOME=' hadoop-env.sh; then
  sed -i '1i export HADOOP_HOME='"${HADOOP_HOME}" hadoop-env.sh
fi
if grep -q '^export JAVA_HOME=' hadoop-env.sh; then
  sed -i 's|^export JAVA_HOME=.*|export JAVA_HOME='"${JAVA_HOME}"'|' hadoop-env.sh
else
  echo "export JAVA_HOME=${JAVA_HOME}" >> hadoop-env.sh
fi

echo ">>> [8] Format NameNode (chỉ lần đầu) — đảm bảo env đã export"
echo "    Using JAVA_HOME=${JAVA_HOME}"
echo "    Using HADOOP_HOME=${HADOOP_HOME}"
if [ ! -d "${NN_DIR}/current" ]; then
  "${HADOOP_HOME}/bin/hdfs" namenode -format -force -nonInteractive
else
  echo "    Đã format trước đó. Bỏ qua."
fi

cat <<EOF

============================================================
✅ Hadoop ${HADOOP_VERSION} cài đặt xong!
🔹 HADOOP_HOME  : ${HADOOP_HOME}
🔹 JAVA_HOME    : ${JAVA_HOME}
🔹 NameNode dir : ${NN_DIR}
🔹 DataNode dir : ${DN_DIR}

Tiếp theo:
  1) Mở terminal mới (để ~/.bashrc có hiệu lực) hoặc: source ~/.bashrc
  2) Khởi động:   ./start_hadoop.sh
  3) Truy cập UI: NameNode http://localhost:9870
                  DataNode http://localhost:9864
                  YARN RM  http://localhost:8088
Dừng dịch vụ: ./stop_hadoop.sh
============================================================
EOF
