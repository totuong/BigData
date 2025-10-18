# Hadoop WSL Installer (Windows 11 + WSL2 + Ubuntu 22.04)

A one-liner to install Hadoop (HDFS + YARN) single-node on WSL2:

```bash
bash -c "$(curl -fsSL https://raw.githubusercontent.com/<your-username>/hadoop-wsl-installer/main/install_hadoop_wsl.sh)"

bash install_hadoop_wsl.sh
Cấp quyền chạy:
chmod +x ~/start_hadoop.sh
Từ lần sau chỉ cần:
bash ~/start_hadoop.sh
Cấp quyền:
chmod +x ~/stop_hadoop.sh

Spark
https://downloads.apache.org/spark/spark-3.5.7/

nano install_spark_wsl.sh
# (Dán toàn bộ nội dung ở trên, rồi Ctrl+O, Enter, Ctrl+X)
chmod +x install_spark_wsl.sh
bash install_spark_wsl.sh
spark-shell --master yarn

