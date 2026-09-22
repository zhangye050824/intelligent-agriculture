# -*- coding: utf-8 -*-
"""
智慧农业大棚环境 —— 一键启动（Windows 本地执行）
用法:  python start_vm_all.py

功能：
  1) 自动连接虚拟机 192.168.138.128（连不上会提示先开机）
  2) 上传并执行 ~/start_all.sh（幂等：已在运行的服务自动跳过）
  3) 按顺序启动: MySQL -> ZooKeeper/Kafka -> sensor_producer -> Spark -> Flisk -> Flask
  4) 实时打印每个服务的状态，最后给出大屏地址
"""
import sys
import time
import warnings

warnings.filterwarnings("ignore")
import paramiko

HOST, USER, PWD = "192.168.138.128", "hadoop", "123456"

# ============ 本地 -> VM 文件同步清单（每次运行前自动上传，避免 VM 跑旧版代码）============
# 格式: (本地相对路径, VM 绝对路径)
# 项目根目录 = 本脚本所在目录
import os
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SYNC_FILES = [
    ("sensor_producer.py",      "/home/hadoop/project/sensor_producer.py"),
    ("spark_write_mysql.py",    "/home/hadoop/project/spark_write_mysql.py"),
    ("flask_server.py",         "/home/hadoop/agri_project/flask_server.py"),
    ("templates/index.html",    "/home/hadoop/agri_project/templates/index.html"),
    ("flink_window_stat.py",    "/home/hadoop/flink_project/flink_window_stat.py"),
]


def sync_local_files(sftp):
    """把本地 5 个核心文件同步到 VM（覆盖），防止 VM 跑旧版导致大屏空白。"""
    print(">>> 同步本地代码到 VM ...")
    for local_rel, remote_abs in SYNC_FILES:
        local_abs = os.path.join(_PROJECT_ROOT, local_rel.replace("/", os.sep))
        if not os.path.isfile(local_abs):
            print(f"  [跳过] 本地不存在: {local_rel}")
            continue
        # 确保远端目录存在
        remote_dir = os.path.dirname(remote_abs).replace("\\", "/")
        try:
            sftp.stat(remote_dir)
        except IOError:
            # 递归创建（简单实现：逐级 mkdir）
            parts = remote_dir.split("/")
            cur = ""
            for p in parts[1:]:
                cur += "/" + p
                try:
                    sftp.stat(cur)
                except IOError:
                    try:
                        sftp.mkdir(cur)
                    except Exception:
                        pass
        sftp.put(local_abs, remote_abs)
        print(f"  [OK] {local_rel}  ->  {remote_abs}")


# ============ 虚拟机端启动脚本（每次运行自动上传覆盖）============
START_SH = r'''#!/bin/bash
# 智慧农业大棚 一键启动脚本
# = 重要：每次运行都做干净启动 =
#   杀掉所有旧进程 + 清理 Kafka/ZK 数据目录 + 清理 Spark/Flink checkpoint
#   避免 OOM 损坏 checkpoint / Kafka Cluster ID 不匹配 等常见故障
source ~/.bashrc

KAFKA_HOME=/opt/module/kafka-3.0.0
SUDO_PWD=123456
LOG=~/start_all.log
echo "================ 一键启动  $(date '+%F %T')  ================" | tee $LOG

# 进程判断用 ps+grep（Kafka/Spark 命令行超长，pgrep -f 会漏匹配）
alive() { ps -ef | grep -v grep | grep -F "$1" >/dev/null 2>&1; }
port()  { netstat -tln 2>/dev/null | grep -q "$1"; }

echo "==== 0/6  清理旧进程 + 损坏的 Checkpoint/Kafka 数据 ===="
echo "  杀掉所有可能残留的进程 ..."
pkill -f "[k]afka"           2>/dev/null
pkill -f "[z]ookeeper"       2>/dev/null
pkill -f "[s]ensor_producer" 2>/dev/null
pkill -f "[s]park-submit"    2>/dev/null
pkill -f "[s]park_write_mysql" 2>/dev/null
pkill -f "[f]link_window"    2>/dev/null
pkill -f "[p]yflink"         2>/dev/null
pkill -f "[f]lask_server"     2>/dev/null
sleep 2

echo "  清理 Kafka/ZK 数据目录（防止 Cluster ID 不匹配）..."
rm -rf /tmp/kafka-logs /tmp/zookeeper

echo "  清理 Spark Checkpoint（防止 OOM 损坏的 Incomplete log file）..."
rm -rf /tmp/greenhouse_chk_v2 /tmp/greenhouse_chk_v3 /tmp/greenhouse_chk_kafka

echo "  清理 Flink Checkpoint ..."
rm -rf /home/hadoop/flink_checkpoint

echo "  [OK] 清理完成"

echo "==== 1/6  MySQL (mysqld, 端口3306) ===="
if port ':3306'; then
  echo "[跳过] MySQL 已在运行"
else
  echo "$SUDO_PWD" | sudo -S systemctl start mysqld >/dev/null 2>&1
  sleep 3
  port ':3306' && echo "[成功] MySQL 已启动" || echo "[失败] MySQL 未启动，检查: sudo systemctl status mysqld"
fi

echo "==== 2/6  ZooKeeper + Kafka (端口2181/9092) ===="
cd $KAFKA_HOME || exit 1
setsid nohup bin/zookeeper-server-start.sh config/zookeeper.properties > ~/zookeeper.log 2>&1 </dev/null &
echo "  ZooKeeper 启动中，等待 8s ..."; sleep 8
port ':2181' || { echo "[失败] ZK 没起来，检查 ~/zookeeper.log"; exit 1; }

export KAFKA_HEAP_OPTS="-Xms128m -Xmx256m"
cd $KAFKA_HOME || exit 1
setsid nohup bin/kafka-server-start.sh config/server.properties > ~/kafka.log 2>&1 </dev/null &
echo "  Kafka 启动中，等待 15s ..."; sleep 15
port ':9092' || { echo "[失败] Kafka 没起来，最后10行日志:"; tail -10 ~/kafka.log; exit 1; }
echo "[成功] Kafka 9092 已监听"

echo "  创建/确认 topic ..."
cd $KAFKA_HOME
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --if-not-exists --topic sensor_data --partitions 1 --replication-factor 1 >/dev/null 2>&1
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --if-not-exists --topic agri_sensor --partitions 1 --replication-factor 1 >/dev/null 2>&1
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --if-not-exists --topic agri_result --partitions 1 --replication-factor 1 >/dev/null 2>&1
echo "  [OK] topic 就绪"

echo "==== 3/6  sensor_producer (传感器数据生产) ===="
cd ~/project || exit 1
setsid nohup python3 sensor_producer.py > producer.log 2>&1 </dev/null &
sleep 3
alive sensor_producer.py && echo "[成功] producer 已启动" || { echo "[失败] 检查 ~/project/producer.log"; exit 1; }

echo "==== 4/6  Spark 双流作业 (window_stat + agri_result) ===="
# 等 producer 先跑一会儿产生数据，避免 Spark catch-up 吃内存
echo "  等 producer 产生数据（5s）..."; sleep 5
cd ~/project || exit 1
setsid nohup spark-submit --master local[1] \
    --conf spark.driver.memory=512M \
    --conf spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider \
    --conf spark.sql.shuffle.partitions=1 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.mariadb.jdbc:mariadb-java-client:2.7.10 \
    spark_write_mysql.py > spark.log 2>&1 </dev/null &
echo "  Spark JVM 初始化中，等待 30s ..."; sleep 30
alive spark_write_mysql.py && echo "[成功] Spark 作业已提交" || { echo "[失败] Spark 最后20行:"; tail -20 ~/project/spark.log; exit 1; }

echo "==== 5/6  Flink 事件时间窗口作业 (window_stat_flink) ===="
cd ~/flink_project || exit 1
setsid nohup env PATH=$HOME/bin:$PATH _JAVA_OPTIONS=-Xmx900m PYFLINK_CLIENT_EXECUTABLE=python3 \
    python3 flink_window_stat.py > flink.log 2>&1 </dev/null &
echo "  Flink 启动中，等待 15s ..."; sleep 15
alive flink_window_stat.py && echo "[成功] Flink 作业已启动" || { echo "[失败] Flink 最后20行:"; tail -20 ~/flink_project/flink.log; exit 1; }

echo "==== 6/6  Flask 实时监测大屏 (端口5000) ===="
cd ~/agri_project || exit 1
setsid nohup python3 flask_server.py > flask.log 2>&1 </dev/null &
sleep 5
alive flask_server.py && echo "[成功] Flask 已启动" || { echo "[失败] Flask 最后20行:"; tail -20 ~/agri_project/flask.log; exit 1; }

echo "================ 状态总览 ================"
chk() { if alive "$2"; then echo "  $1: RUNNING"; else echo "  $1: NOT RUNNING"; fi; }
port ':3306'  && echo "  MySQL(3306): RUNNING"  || echo "  MySQL(3306): NOT RUNNING"
port ':9092'  && echo "  Kafka(9092): RUNNING"  || echo "  Kafka(9092): NOT RUNNING"
chk Producer sensor_producer.py
chk Spark    spark_write_mysql.py
chk Flink    flink_window_stat.py
chk Flask    flask_server.py
port ':5000'  && echo "  大屏地址: http://192.168.138.128:5000" || echo "  大屏未监听"
free -m | awk 'NR==2{printf "  内存: 已用 %sMB / 共 %sMB / 可用 %sMB\n",$3,$2,$7}'
echo "================ 启动流程结束 ================"
'''


def main():
    print(f">>> 连接虚拟机 {HOST} ...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(HOST, port=22, username=USER, password=PWD, timeout=10)
    except Exception as e:
        print(f"[连接失败] {e}")
        print(">>> 请确认：1) 虚拟机已开机  2) 网卡是 NAT(192.168.138.x)  3) 能 ping 通该 IP")
        sys.exit(1)
    print(">>> 连接成功")

    sftp = ssh.open_sftp()
    # 第 1 步：同步本地最新代码到 VM（避免 VM 跑旧版导致大屏空白）
    sync_local_files(sftp)
    # 第 2 步：上传启动脚本
    print(">>> 上传 ~/start_all.sh ...")
    with sftp.file("/home/hadoop/start_all.sh", "w") as f:
        f.write(START_SH)
    sftp.chmod("/home/hadoop/start_all.sh", 0o755)
    sftp.close()

    print(">>> 开始执行启动脚本（Spark/Flink 初始化需约 1 分钟，请等待）...\n")
    chan = ssh.get_transport().open_session()
    chan.settimeout(300)
    chan.exec_command("bash ~/start_all.sh 2>&1; echo __RC=$?")
    while True:
        while chan.recv_ready():
            sys.stdout.write(chan.recv(4096).decode("utf-8", errors="replace"))
            sys.stdout.flush()
        while chan.recv_stderr_ready():
            sys.stdout.write(chan.recv_stderr(4096).decode("utf-8", errors="replace"))
            sys.stdout.flush()
        if chan.exit_status_ready() and not chan.recv_ready() and not chan.recv_stderr_ready():
            break
        time.sleep(0.5)

    rest = chan.recv(4096).decode("utf-8", errors="replace")
    sys.stdout.write(rest)
    rc = chan.recv_exit_status()
    ssh.close()

    if rc == 0:
        print("\n>>> 全部完成！浏览器打开（建议 Ctrl+Shift+R 强制刷新）：")
        print("    http://192.168.138.128:5000")
        print("\n>>> 提示：本脚本已自动同步本地代码到 VM，且幂等启动（已运行的会跳过）。")
        print("    若你刚改了 producer/flask 代码想立即生效，先在 VM 上 pkill 对应进程，")
        print("    再跑一次本脚本即可重启（Spark/Flink 长任务不建议随意重启）。")
    else:
        print("\n>>> 脚本异常结束，请查看上面的 [失败] 项及对应日志")
        sys.exit(1)


if __name__ == "__main__":
    main()
