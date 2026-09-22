# -*- coding: utf-8 -*-
import paramiko, time

HOST, USER, PWD = "192.168.138.128", "hadoop", "123456"

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(HOST, port=22, username=USER, password=PWD, timeout=15)

# Step 1: kill old
print("=== Step 1: kill old Flask ===")
stdin, stdout, stderr = client.exec_command("pkill -f flask_server; echo done1", timeout=10)
stdout.channel.recv_exit_status()
time.sleep(1)
print(stdout.read().decode(errors='replace'))

# Step 2: verify killed
stdin, stdout, stderr = client.exec_command("pgrep -f flask_server || echo 'no flask alive'", timeout=5)
print(stdout.read().decode(errors='replace'))

# Step 3: start new (daemon)
print("=== Step 2: start new Flask ===")
cmd = "cd /home/hadoop/agri_project && setsid nohup python3 flask_server.py > flask.log 2>&1 < /dev/null &"
stdin, stdout, stderr = client.exec_command(cmd, timeout=5)
stdout.channel.recv_exit_status()
time.sleep(2)

# Step 4: verify
print("=== Step 3: verify Flask alive ===")
stdin, stdout, stderr = client.exec_command("pgrep -af flask_server", timeout=5)
out = stdout.read().decode(errors='replace').strip()
print(out if out else "NO PROCESS")

# Step 5: tail log
print("=== Step 4: Flask log ===")
stdin, stdout, stderr = client.exec_command("tail -10 /home/hadoop/agri_project/flask.log", timeout=5)
print(stdout.read().decode(errors='replace'))

# Step 6: quick smoke test
print("=== Step 5: smoke test /api/qa ===")
stdin, stdout, stderr = client.exec_command("curl -s 'http://localhost:5000/api/qa?q=test'", timeout=8)
print(stdout.read().decode(errors='replace')[:300])

client.close()
print("Done!")
