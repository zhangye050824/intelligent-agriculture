# -*- coding: utf-8 -*-
import paramiko, urllib.parse, json, time

HOST, USER, PWD = "192.168.138.128", "hadoop", "123456"

# Step 1: upload
print("=== Uploading flask_server.py ===")
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(HOST, port=22, username=USER, password=PWD, timeout=15)
sftp = client.open_sftp()
sftp.put(r"e:\大四实训\最终项目\project\flask_server.py", "/home/hadoop/agri_project/flask_server.py")
sftp.put(r"e:\大四实训\最终项目\project\templates\index.html", "/home/hadoop/agri_project/templates/index.html")
sftp.close()
print("Uploaded both files")

# Step 2: kill + restart
print("=== Restarting Flask ===")
stdin, stdout, stderr = client.exec_command("pkill -f flask_server; echo killed", timeout=8)
stdout.channel.recv_exit_status()
time.sleep(1)
stdin, stdout, stderr = client.exec_command(
    "cd /home/hadoop/agri_project && setsid nohup python3 flask_server.py > flask.log 2>&1 < /dev/null &", timeout=5)
stdout.channel.recv_exit_status()
time.sleep(3)
stdin, stdout, stderr = client.exec_command("pgrep -af flask_server", timeout=5)
print("Flask proc:", stdout.read().decode(errors='replace').strip())

# Step 3: test
print("=== Running test ===")
PY = r'''
import urllib.parse, urllib.request, json
tests = [
    "大棚一最近温度多少",
    "哪个大棚告警最多",
    "Spark 和 Flink 差异",
    "为什么会高温",
    "大棚一和大棚二对比",
    "各大棚平均温度排行",
]
for q in tests:
    url = "http://localhost:5000/api/qa?q=" + urllib.parse.quote(q)
    try:
        r = urllib.request.urlopen(url, timeout=8).read().decode("utf-8")
        d = json.loads(r)
        print("Q:", q)
        print("  意图:", d.get("intent"))
        print("  回答:", d.get("answer","")[:300])
        print()
    except Exception as e:
        print("Q:", q, " ERROR:", e)
        print()
'''
with client.open_sftp().file('/tmp/test_api3.py', 'w') as f:
    f.write(PY)
stdin, stdout, stderr = client.exec_command("python3 /tmp/test_api3.py", timeout=30)
print(stdout.read().decode(errors='replace'))

client.close()
