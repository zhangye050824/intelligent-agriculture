import paramiko

HOST, USER, PWD = "192.168.138.128", "hadoop", "123456"
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(HOST, port=22, username=USER, password=PWD, timeout=15)

# 查 greenhouse_id 实际值
print("=== greenhouse_id 实际值 ===")
for table in ['window_stat', 'window_stat_flink']:
    stdin, stdout, stderr = client.exec_command(
        f"mysql -uroot -p123456 agriculture_analysis -e "
        f"\"SELECT '{table}' as tbl, greenhouse_id, COUNT(*) FROM {table} "
        f"GROUP BY greenhouse_id ORDER BY COUNT(*) DESC LIMIT 10\"", timeout=10)
    print(stdout.read().decode(errors='replace'))

# 查 大棚一 有多少行
print("=== gh_01 vs 大棚一 查询 ===")
for gh_val in ['gh_01', '大棚一', '1', '']:
    stdin, stdout, stderr = client.exec_command(
        f"mysql -uroot -p123456 agriculture_analysis -e "
        f"\"SELECT '{gh_val}' as gh_val, COUNT(*) FROM window_stat "
        f"WHERE greenhouse_id = {'NULL' if gh_val == '' else repr(gh_val)}\"", timeout=10)
    print(stdout.read().decode(errors='replace'))

client.close()
