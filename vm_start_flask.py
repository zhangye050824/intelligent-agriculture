# -*- coding: utf-8 -*-
"""检查 VM 上各进程状态，未启动的 Flask 则启动它"""
import time
import warnings
warnings.filterwarnings("ignore")
import paramiko

HOST, USER, PWD = "192.168.138.128", "hadoop", "123456"

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, username=USER, password=PWD, timeout=10)

def run(cmd, timeout=20):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    return stdout.read().decode("utf-8", "ignore").strip(), stderr.read().decode("utf-8", "ignore").strip()

# 0. 确认 IP
out, _ = run("hostname -I")
print("VM IP:", out)

# 1. 各进程状态
jobs = [("Flask", "flask_server.py"), ("Spark", "spark_write_mysql.py"),
        ("Flink", "flink_window_stat.py"), ("Producer", "sensor_producer.py")]
pids = {}
for name, pat in jobs:
    out, _ = run(f"pgrep -f {pat}")
    pids[name] = out
    print(f"[{name:8s}] {'RUNNING pid=' + out.replace(chr(10), ',') if out else 'NOT RUNNING'}")

out, _ = run("ss -tln | grep -E ':5000|:9092'")
print("ports:\n", out or "(none)")

# 2. 启动 Flask（如果没在跑）
if not pids["Flask"]:
    print(">>> starting flask_server.py ...")
    run("cd ~/agri_project && setsid nohup python3 flask_server.py > flask.log 2>&1 < /dev/null &")
    time.sleep(4)
    out, _ = run("pgrep -f flask_server.py")
    print("flask pid after start:", out or "(still dead)")
else:
    print(">>> Flask already running, skip")

# 3. 看日志尾部
out, err = run("tail -20 ~/agri_project/flask.log")
print("--- flask.log tail ---")
print(out if out else err if err else "(empty)")

ssh.close()
