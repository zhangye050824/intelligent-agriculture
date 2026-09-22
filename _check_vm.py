import paramiko
HOST, USER, PWD = "192.168.138.128", "hadoop", "123456"
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(HOST, port=22, username=USER, password=PWD, timeout=15)
stdin, stdout, stderr = client.exec_command(
    "cat /home/hadoop/project/spark_write_mysql.py 2>/dev/null; "
    "echo '===== FLINK ====='; "
    "cat /home/hadoop/flink_project/flink_window_stat.py 2>/dev/null | head -80; "
    "echo '===== PROCESS ====='; pgrep -af spark_write_mysql; pgrep -af flink_window"
)
print(stdout.read().decode())
print(stderr.read().decode())
client.close()
