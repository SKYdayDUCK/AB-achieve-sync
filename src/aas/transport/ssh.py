"""B->A OpenSSH forced-command transport; stdout control JSON or raw range bytes."""
from __future__ import annotations
import json
from pathlib import Path
import shlex
import subprocess
from typing import Protocol
from aas.common.config import BConfig
from aas.common.core import AASError, AuthenticationError, IntegrityError, ProtocolError, TemporaryError


class Transport(Protocol):
    def hello(self) -> dict: ...
    def list_ready(self, after_event_id: int, page_size: int, snapshot_high: int | None = None) -> dict: ...
    def wait_ready(self, after_event_id: int, timeout_seconds: float) -> dict: ...
    def info(self, batch_id: str) -> dict: ...
    def get_range(self, batch_id: str, offset: int, length: int) -> bytes: ...
    def ack(self, request: dict) -> dict: ...
    def ledger(self, since_seq: int = 0) -> dict: ...


class SSHTransport:
    def __init__(self, config: BConfig):
        self.cfg = config

    def _base(self, *, emergency: bool = False) -> list[str]:
        cmd = ["ssh", "-T", "-p", str(self.cfg.remote_port),
            "-o", "StrictHostKeyChecking=yes", "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=2"]
        if self.cfg.address_family != "any":  # avoids multi-second AAAA lookup stalls on IPv4-only paths
            cmd += ["-o", f"AddressFamily={self.cfg.address_family}"]
        identity = self.cfg.emergency_identity_file if emergency else self.cfg.identity_file
        if identity:
            cmd += ["-i", str(Path(identity).expanduser())]
        if self.cfg.known_hosts_file:
            cmd += ["-o", f"UserKnownHostsFile={Path(self.cfg.known_hosts_file).expanduser()}"]
        cmd.append(f"{self.cfg.remote_user}@{self.cfg.remote_host}")
        return cmd

    def command(self, *args: str, timeout: int = 60, stdin: bytes | None = None) -> bytes:
        remote = " ".join(shlex.quote(token) for token in ("aasctl", *args))
        try:
            proc = subprocess.run(self._base() + [remote], input=stdin, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise TemporaryError(f"SSH transport failure: {exc}") from exc
        if proc.returncode:
            stderr = proc.stderr.decode("utf-8", errors="replace")[:700]
            if proc.returncode == 255 and any(k in stderr.lower() for k in ("permission denied", "host key", "identification has changed", "publickey")):
                raise AuthenticationError(f"SSH authentication/host key failed: {stderr}")
            if proc.returncode == 2:
                raise ProtocolError(f"remote protocol error: {stderr}")
            raise TemporaryError(f"SSH error {proc.returncode}: {stderr}")
        return proc.stdout

    def json(self, *args: str, timeout: int | None = None) -> dict:
        output = self.command(*args, timeout=timeout or self.cfg.remote_query_timeout_seconds)
        if len(output) > 8 * 1024 * 1024:
            raise ProtocolError("SSH control JSON exceeds 8MiB")
        try:
            obj = json.loads(output)
        except ValueError as exc:
            raise ProtocolError("SSH control response is not JSON") from exc
        if not isinstance(obj, dict):
            raise ProtocolError("SSH control response root is not object")
        return obj

    def hello(self) -> dict:
        return self.json("hello")

    def list_ready(self, after_event_id: int, page_size: int, snapshot_high: int | None = None) -> dict:
        args = ["list-ready", "--after-event-id", str(after_event_id), "--page-size", str(page_size)]
        if snapshot_high is not None:
            args += ["--snapshot-high", str(snapshot_high)]
        return self.json(*args)

    def wait_ready(self, after_event_id: int, timeout_seconds: float) -> dict:
        return self.json("wait-ready", "--after-event-id", str(after_event_id), "--timeout-seconds", str(timeout_seconds),
                         timeout=int(timeout_seconds) + 20)

    def info(self, batch_id: str) -> dict:
        return self.json("info", "--batch-id", batch_id)

    def get_range(self, batch_id: str, offset: int, length: int) -> bytes:
        if length < 1 or length > 16777216:
            raise ProtocolError("range exceeds remote limit")
        result = self.command("get", "--batch-id", batch_id, "--offset", str(offset), "--length", str(length), timeout=120)
        if len(result) != length:
            raise TemporaryError(f"partial SSH get_range: expected {length}, received {len(result)}")
        return result

    def ack(self, request: dict) -> dict:
        # A re-hashes the bundle before deleting it inside this call: allow far more than a query.
        return self.json("ack", "--json", json.dumps(request, sort_keys=True, separators=(",", ":")), timeout=1800)

    def ledger(self, since_seq: int = 0) -> dict:
        return self.json("ledger", "--since-seq", str(since_seq))

    def crypto_probe(self) -> dict:
        return self.json("crypto-probe")
