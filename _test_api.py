import paramiko, urllib.parse, json

HOST, USER, PWD = "192.168.138.128", "hadoop", "123456"
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(HOST, port=22, username=USER, password=PWD, timeout=15)

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
        print("  回答:", d.get("answer","")[:250])
        print()
    except Exception as e:
        print("Q:", q, " ERROR:", e)
        print()
'''

# 写脚本到 VM
sftp = client.open_sftp()
with sftp.file('/tmp/test_api2.py', 'w') as f:
    f.write(PY)
sftp.close()

# 执行
stdin, stdout, stderr = client.exec_command("python3 /tmp/test_api2.py", timeout=30)
print("=== Q&A 测试结果 ===")
print(stdout.read().decode(errors='replace'))
err = stderr.read().decode(errors='replace')
if err:
    print("[STDERR]", err)

# 也测下 alarm_reason
print("=== 归因接口测试 ===")
stdin, stdout, stderr = client.exec_command("curl -s 'http://localhost:5000/api/alarm_reason' | python3 -m json.tool | head -30", timeout=10)
print(stdout.read().decode(errors='replace'))

client.close()
