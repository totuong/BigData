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
