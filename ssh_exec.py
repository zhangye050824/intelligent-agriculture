# -*- coding: utf-8 -*-
"""SSH 远程命令执行辅助脚本（适配 paramiko 密码登录）
命令通过环境变量 REMOTE_CMD 传入，避免本地 shell 引号/重定向干扰。
用法: $env:REMOTE_CMD="..."; python ssh_exec.py [超时秒]
"""
import os
import sys
import paramiko

HOST = "192.168.138.128"
USER = "hadoop"
PWD = "123456"


def run_cmd(cmd, timeout=120):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, port=22, username=USER, password=PWD, timeout=15)
    # 登录 shell 会加载 ~/.bash_profile / ~/.bashrc（JAVA_HOME、SPARK_HOME 等）
    full = "source ~/.bashrc 2>/dev/null; " + cmd
    stdin, stdout, stderr = client.exec_command(
        full, timeout=timeout, get_pty=False)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    code = stdout.channel.recv_exit_status()
    if out:
        print(out)
    if err:
        print("[STDERR]", err, file=sys.stderr)
    print(f"[EXIT CODE] {code}")
    client.close()
    return code


if __name__ == "__main__":
    cmd = os.environ.get("REMOTE_CMD", "")
    if not cmd:
        print("REMOTE_CMD not set")
        sys.exit(2)
    t = int(sys.argv[1]) if len(sys.argv) > 1 else 120
    sys.exit(0 if run_cmd(cmd, t) == 0 else 1)
