"""Deliberately weaker manual emergency streaming, independent of normal batches."""
from __future__ import annotations
import json
import os
from pathlib import Path
import select
import struct
import sys
import time
import uuid
from aas.common.core import IntegrityError, ProtocolError, TemporaryError, fsync_dir, hash_file, must_regular, process_lock, safe_image_name, canonical


def send_header(output, header: dict):
    raw = canonical(header)
    if len(raw) > 65536:
        raise ProtocolError("emergency header too large")
    output.write(struct.pack("!I", len(raw)))
    output.write(raw)
    output.flush()


def emergency_stream(service, reader=None, writer=None):
    if not service.cfg.emergency_enabled:
        raise ProtocolError("emergency channel disabled")
    incoming = reader or sys.stdin.buffer
    outgoing = writer or sys.stdout.buffer
    end_time = time.monotonic() + service.cfg.emergency_max_seconds
    # Same flock used by normal source claiming, held for the entire emergency session.
    with process_lock(service.state / "input_claim.lock"):
        while time.monotonic() < end_time:
            candidates = service._eligible_names(emergency=True, limit=1)
            if not candidates:
                send_header(outgoing, {"type": "END"})
                return
            name = safe_image_name(candidates[0])
            original = service.input / name
            before = must_regular(original)
            token = uuid.uuid4().hex
            fixed = service.emergency_claim / f"{token}__{name}"
            os.rename(original, fixed)
            fsync_dir(service.input)
            fsync_dir(service.emergency_claim)
            after = must_regular(fixed)
            if (before.st_ino, before.st_dev, before.st_size) != (after.st_ino, after.st_dev, after.st_size):
                raise IntegrityError("emergency claim identity changed")
            digest, size = hash_file(fixed)
            send_header(outgoing, {"type": "FILE", "token": token, "name": name, "bytes": size, "sha256": digest})
            with fixed.open("rb") as file:
                while data := file.read(1024 * 1024):
                    outgoing.write(data)
                outgoing.flush()
            remaining = min(service.cfg.emergency_idle_seconds, max(0, end_time - time.monotonic()))
            if remaining <= 0 or not select.select([incoming], [], [], remaining)[0]:
                raise TemporaryError("emergency client timed out; unconfirmed file retained")
            reply = incoming.readline(4096)
            try:
                ack = json.loads(reply)
            except ValueError as exc:
                raise ProtocolError("bad emergency file ACK") from exc
            if not isinstance(ack, dict) or ack.get("ok") != token or ack.get("sha256") != digest:
                raise ProtocolError("emergency ACK token/hash mismatch")
            st = must_regular(fixed)
            if (st.st_dev, st.st_ino, st.st_size) != (after.st_dev, after.st_ino, size):
                raise IntegrityError("emergency claimed file changed before unlink")
            fixed.unlink()
            fsync_dir(service.emergency_claim)
            service.db.audit(None, "EMERGENCY_SOURCE_UNLINKED", f"token={token}, filename={name}")
    raise TemporaryError("emergency session maximum duration reached")
