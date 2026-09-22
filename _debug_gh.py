import paramiko

HOST, USER, PWD = "192.168.138.128", "hadoop", "123456"
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(HOST, port=22, username=USER, password=PWD, timeout=15)

# 1. 直接跑 gh_latest 的 SQL
print("=== SQL 1: 大棚一最新 ===")
stdin, stdout, stderr = client.exec_command(
    "mysql -uroot -p123456 agriculture_analysis -e \""
    "SELECT win_start, avg_temp, avg_humidity, avg_soil_humidity, max_light "
    "FROM window_stat WHERE greenhouse_id='gh_01' ORDER BY win_start DESC LIMIT 1\"",
    timeout=10)
print(stdout.read().decode(errors='replace'))

# 2. 查 Flask 日志里 gh_latest 请求的 traceback
print("=== Flask log (gh_latest) ===")
stdin, stdout, stderr = client.exec_command(
    "grep -A5 'gh_latest\\|大棚一\\|ERROR\\|Traceback' /home/hadoop/agri_project/flask.log | tail -30",
    timeout=10)
print(stdout.read().decode(errors='replace'))

# 3. 直接用 Python 测一下 Flask 内部逻辑
print("=== Quick Python test ===")
PY = r'''
import sys
sys.path.insert(0, '/home/hadoop/agri_project')
# 先验证映射
from flask_server import _GH_MAP
inv = {v: k for k, v in _GH_MAP.items()}
print("_GH_MAP:", _GH_MAP)
print("inv_map:", inv)
print("大棚一 ->", inv.get('大棚一'))

# 再测意图识别
from flask_server import _detect_intent, _qa_answer_intent
intent, params = _detect_intent("大棚一最近温度多少")
print("intent:", intent, "params:", params)

# 直接调答案
answer = _qa_answer_intent(intent, params)
print("answer:", answer)
'''
with client.open_sftp().file('/tmp/debug_gh.py', 'w') as f:
    f.write(PY)
stdin, stdout, stderr = client.exec_command("cd /home/hadoop/agri_project && python3 /tmp/debug_gh.py 2>&1", timeout=15)
print(stdout.read().decode(errors='replace'))

client.close()
