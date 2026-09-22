# -*- coding: utf-8 -*-
"""SFTP 上传文件到虚拟机。用法: $env:LOCAL_FILE="..."; $env:REMOTE_FILE="..."; python ssh_put.py"""
import os
import paramiko

HOST, USER, PWD = "192.168.138.128", "hadoop", "123456"

local = os.environ["LOCAL_FILE"]
remote = os.environ["REMOTE_FILE"]

t = paramiko.Transport((HOST, 22))
t.connect(username=USER, password=PWD)
sftp = paramiko.SFTPClient.from_transport(t)
sftp.put(local, remote)
print("uploaded:", local, "->", remote)
sftp.close()
t.close()
