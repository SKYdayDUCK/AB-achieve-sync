"""Emergency B client: length framed raw bytes; ACK each file only after fsync + SHA."""
from __future__ import annotations
import json
import os
from pathlib import Path
import shlex
import struct
import subprocess
from aas.common.core import IntegrityError, ProtocolError, atomic_write, fsync_dir, hash_file, safe_image_name
from aas.transport.ssh import SSHTransport


def read_exact(stream, size: int) -> bytes:
    parts = []
    remaining = size
    while remaining:
        block = stream.read(min(1024 * 1024, remaining))
        if not block:
            raise IntegrityError("truncated emergency frame")
        parts.append(block)
        remaining -= len(block)
    return b"".join(parts)


def receive_session(reader, writer, target_root: Path):
    target_root.mkdir(parents=True, exist_ok=True)
    received = 0
    while True:
        header_len = struct.unpack("!I", read_exact(reader, 4))[0]
        if not 0 < header_len <= 65536:
            raise ProtocolError("invalid emergency header length")
        header = json.loads(read_exact(reader, header_len))
        if header.get("type") == "END":
            return received
        if header.get("type") != "FILE":
            raise ProtocolError("invalid emergency frame type")
        name = safe_image_name(header["name"])
        token = header["token"]
        if len(token) != 32 or any(ch not in "0123456789abcdef" for ch in token):
            raise ProtocolError("invalid emergency token")
        remaining = header["bytes"]
        if type(remaining) is not int or not 0 < remaining <= 17179869184:
            raise ProtocolError("invalid emergency length")
        path = target_root / f"{token}_{name}"
        with path.open("xb") as outfile:
            while remaining:
                block = read_exact(reader, min(1024 * 1024, remaining))
                outfile.write(block)
                remaining -= len(block)
            outfile.flush()
            os.fsync(outfile.fileno())
        fsync_dir(target_root)
        if hash_file(path)[0] != header["sha256"]:
            raise IntegrityError("emergency file SHA mismatch; no ACK")
        writer.write((json.dumps({"ok": token, "sha256": header["sha256"]}) + "\n").encode())
        writer.flush()
        received += 1


def emergency_ssh(cfg):
    if not cfg.emergency_identity_file:
        raise ProtocolError("manual emergency requires a separate authorized emergency SSH identity")
    transport = SSHTransport(cfg)
    # SSH still initiates B->A, no inbound port needed on B.
    remote = " ".join(shlex.quote(k) for k in ("aasctl", "emergency-stream"))
    proc = subprocess.Popen(transport._base(emergency=True) + [remote], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
    assert proc.stdout and proc.stdin and proc.stderr
    error = None
    try:
        count = receive_session(proc.stdout, proc.stdin, cfg.root / "output" / "emergency")
    except BaseException as exc:
        error = exc
        proc.kill()
        raise
    finally:
        proc.stdin.close()
        proc.stdout.close()
        proc.wait(timeout=15)
        diagnostic = proc.stderr.read(4000).decode(errors="replace")
        proc.stderr.close()
    if proc.returncode != 0:
        raise ProtocolError(f"A emergency stream failed: {diagnostic}")
    return count
