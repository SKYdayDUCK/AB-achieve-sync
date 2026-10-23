"""Forced-command, allow-listed SSH server. All user input is parsed, never shell-executed."""
from __future__ import annotations
import argparse
import base64
import io
import json
import os
from pathlib import Path
import shlex
import sys
import tempfile
import secrets
from aas.common.core import AASError, ProtocolError, canonical, sha_bytes, validate_id
from aas.crypto.core import AgeCrypto
from .core import AService
from .emergency import emergency_stream


def parser():
    p = argparse.ArgumentParser(prog="aasctl", add_help=False)
    subs = p.add_subparsers(dest="command", required=True)
    subs.add_parser("hello", add_help=False)
    ready = subs.add_parser("list-ready", add_help=False)
    ready.add_argument("--after-event-id", type=int, default=0)
    ready.add_argument("--page-size", type=int, default=100)
    ready.add_argument("--snapshot-high", type=int)
    wait = subs.add_parser("wait-ready", add_help=False)
    wait.add_argument("--after-event-id", type=int, required=True)
    wait.add_argument("--timeout-seconds", type=float, default=55.0)
    info = subs.add_parser("info", add_help=False)
    info.add_argument("--batch-id", required=True)
    get = subs.add_parser("get", add_help=False)
    get.add_argument("--batch-id", required=True)
    get.add_argument("--offset", type=int, required=True)
    get.add_argument("--length", type=int, required=True)
    ack = subs.add_parser("ack", add_help=False)
    ack.add_argument("--json", required=True)
    ledger = subs.add_parser("ledger", add_help=False)
    ledger.add_argument("--since-seq", type=int, default=0)
    subs.add_parser("status", add_help=False)
    subs.add_parser("crypto-probe", add_help=False)
    subs.add_parser("emergency-stream", add_help=False)
    return p


def crypto_probe(service: AService):
    data = secrets.token_bytes(32)
    if service.cfg.crypto_scheme == "none":
        return {"scheme": "none", "data_base64": base64.b64encode(data).decode(), "sha256": sha_bytes(data)}
    age = AgeCrypto()
    age.selftest()
    with tempfile.TemporaryDirectory(dir=service.state) as td:
        source = Path(td) / "challenge"
        output = Path(td) / "cipher"
        source.write_bytes(data)
        args = ["age", "-o", str(output)]
        for pub in age.normalize_recipients(service.cfg.recipients):
            args.extend(("-r", pub))
        args.append(str(source))
        age.run(*args)
        return {"scheme": "age_x25519", "data_base64": base64.b64encode(output.read_bytes()).decode(), "sha256": sha_bytes(data)}


def dispatch(service: AService, argv: list[str], *, stdout=None):
    args = parser().parse_args(argv)
    output = stdout or sys.stdout.buffer
    def emit(data):
        output.write(canonical(data))
        output.flush()
    if args.command == "hello":
        emit(service.hello())
    elif args.command == "list-ready":
        emit(service.list_ready(after_event_id=args.after_event_id, page_size=args.page_size, snapshot_high=args.snapshot_high))
    elif args.command == "wait-ready":
        emit(service.wait_ready(args.after_event_id, args.timeout_seconds))
    elif args.command == "info":
        emit(service.info(validate_id(args.batch_id)))
    elif args.command == "get":
        service.get_range(validate_id(args.batch_id), args.offset, args.length, output)
    elif args.command == "ack":
        if len(args.json) > 8192:
            raise ProtocolError("oversized ACK JSON")
        request = json.loads(args.json)
        emit({"result": service.ack(request)})
    elif args.command == "ledger":
        emit({"items": service.ledger(args.since_seq)})
    elif args.command == "status":
        emit(service.status())
    elif args.command == "crypto-probe":
        emit(crypto_probe(service))
    elif args.command == "emergency-stream":
        emergency_stream(service, writer=output)
    else:
        raise ProtocolError("unrecognized command")


def run_forced(service: AService, ssh_original_command: str, *, stdout=None, role: str = "consumer"):
    if role not in ("consumer", "emergency"):
        raise ProtocolError("invalid forced SSH role")
    if not ssh_original_command or len(ssh_original_command) > 16384:
        raise ProtocolError("missing or oversized forced SSH command")
    try:
        argv = shlex.split(ssh_original_command)
    except ValueError as exc:
        raise ProtocolError("bad SSH command syntax") from exc
    if not argv or argv[0] != "aasctl":
        raise ProtocolError("only aasctl allow-listed commands may run")
    command = argv[1] if len(argv) > 1 else None
    if (role == "emergency" and command != "emergency-stream") or (role == "consumer" and command == "emergency-stream"):
        raise ProtocolError("forced SSH key role does not permit requested operation")
    dispatch(service, argv[1:], stdout=stdout)
