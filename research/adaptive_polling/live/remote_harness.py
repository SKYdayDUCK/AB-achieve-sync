"""Tiny paramiko helper used by the live experiments (password login for the *setup/upload* side only).

env: AAS_HOST, AAS_PORT, AAS_PW (and optional AAS_USER, default root). The host key must already be in
~/.ssh/known_hosts (RejectPolicy), same strictness as AAS itself. Install: pip install paramiko
"""
import os
import paramiko


def client():
    c = paramiko.SSHClient()
    c.load_host_keys(os.path.expanduser("~/.ssh/known_hosts"))
    c.set_missing_host_key_policy(paramiko.RejectPolicy())
    c.connect(os.environ["AAS_HOST"], int(os.environ.get("AAS_PORT", "22")), os.environ.get("AAS_USER", "root"),
              password=os.environ["AAS_PW"], timeout=20, allow_agent=False, look_for_keys=False)
    return c


def run(c, cmd, timeout=600):
    _, o, e = c.exec_command(cmd, timeout=timeout)
    out, err = o.read().decode(errors="replace"), e.read().decode(errors="replace")
    return o.channel.recv_exit_status(), out, err
