"""A/B/operator/forced-SSH CLI entry points."""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
from aas.common.config import AConfig, BConfig
from aas.common.core import AASError, ProtocolError, process_lock
from aas.a.core import AService
from aas.a.ssh_ctl import run_forced, crypto_probe
from aas.b.core import BService
from aas.b.emergency import emergency_ssh
from aas.crypto.core import decrypt_with_identities
from aas.common.core import sha_bytes
import base64
from pathlib import Path
import tempfile


def main_a():
    p = argparse.ArgumentParser(prog="aas-a")
    p.add_argument("--config", required=True)
    sub = p.add_subparsers(dest="action", required=True)
    sub.add_parser("init")
    sub.add_parser("once")
    run = sub.add_parser("run")
    run.add_argument("--interval", type=float, default=15.0)
    sub.add_parser("recover")
    retry = sub.add_parser("retry-blocked")
    retry.add_argument("--batch-id", required=True)
    reannounce = sub.add_parser("reannounce-ready")
    reannounce.add_argument("--batch-id", required=True)
    sub.add_parser("status")
    args = p.parse_args()
    try:
        service = AService(AConfig.load(args.config))
        if args.action == "init":
            print(json.dumps(service.hello()))
        elif args.action == "once":
            with process_lock(service.state / "producer.lock"):
                print(json.dumps({"batch_id": service.once()}))
        elif args.action == "recover":
            with process_lock(service.state / "producer.lock"):
                service.recover()
        elif args.action == "retry-blocked":
            with process_lock(service.state / "producer.lock"):
                service.retry_blocked(args.batch_id)
        elif args.action == "reannounce-ready":
            with process_lock(service.state / "producer.lock"):
                print(json.dumps({"ready_event_id": service.reannounce_ready(args.batch_id)}))
        elif args.action == "run":
            if args.interval <= 0:
                raise ProtocolError("run interval must be positive")
            with process_lock(service.state / "producer.lock"):
                while True:
                    try:
                        service.once()
                    except (AASError, OSError) as exc:
                        print(f"A cycle failed: {exc}", file=sys.stderr)
                    time.sleep(args.interval)
        else:
            print(json.dumps(service.status(), ensure_ascii=False))
    except (AASError, OSError, ValueError) as exc:
        print(f"aas-a: {exc}", file=sys.stderr)
        raise SystemExit(2)


def main_b():
    p = argparse.ArgumentParser(prog="aas-b")
    p.add_argument("--config", required=True)
    sub = p.add_subparsers(dest="action", required=True)
    sub.add_parser("init")
    sub.add_parser("once")
    sub.add_parser("run")
    sub.add_parser("status")
    retry_held = sub.add_parser("retry-held")
    retry_held.add_argument("--batch-id", required=True)
    retry_held.add_argument("--reason", required=True)
    sub.add_parser("emergency")
    sub.add_parser("crypto-probe")
    resolve = sub.add_parser("resolve-handoff")
    resolve.add_argument("--batch-id", required=True)
    resolve.add_argument("--action", choices=("confirm-delivered", "hold"), required=True)
    resolve.add_argument("--reason", required=True)
    args = p.parse_args()
    try:
        cfg = BConfig.load(args.config)
        service = BService(cfg)
        if args.action == "init":
            print(json.dumps({"consumer_id": cfg.consumer_id, "root": str(cfg.root)}))
        elif args.action == "once":
            with process_lock(service.state / "b_instance.lock"):
                print(json.dumps(service.once()))
        elif args.action == "run":
            service.run()
        elif args.action == "status":
            print(json.dumps(service.db.rows(), ensure_ascii=False, indent=2))
        elif args.action == "emergency":
            print(json.dumps({"emergency_received": emergency_ssh(cfg)}))
        elif args.action == "retry-held":
            with process_lock(service.state / "b_instance.lock"):
                service.retry_held(args.batch_id, reason=args.reason)
        elif args.action == "resolve-handoff":
            with process_lock(service.state / "b_instance.lock"):
                service.resolve_uncertain(args.batch_id, action=args.action, reason=args.reason)
        elif args.action == "crypto-probe":
            response = service.transport.crypto_probe()
            source = base64.b64decode(response["data_base64"], validate=True)
            if response["scheme"] != cfg.crypto_scheme:
                raise ProtocolError("probe crypto scheme mismatched")
            with tempfile.TemporaryDirectory(dir=service.state) as tmp:
                secret = Path(tmp) / "encrypted"
                plain = Path(tmp) / "decrypted"
                secret.write_bytes(source)
                decrypt_with_identities(secret, plain, {"scheme": response["scheme"]}, cfg.identity_keys)
                if sha_bytes(plain.read_bytes()) != response["sha256"]:
                    raise ProtocolError("B crypto probe result digest mismatch")
            print("Crypto probe passed")
    except (AASError, OSError, ValueError) as exc:
        print(f"aas-b: {exc}", file=sys.stderr)
        raise SystemExit(2)


def main_shell():
    p = argparse.ArgumentParser(prog="aasctl-shell")
    p.add_argument("--config", required=True)
    p.add_argument("--role", choices=("consumer", "emergency"), default="consumer")
    args = p.parse_args()
    try:
        service = AService(AConfig.load(args.config))
        run_forced(service, os.environ.get("SSH_ORIGINAL_COMMAND", ""), role=args.role)
    except (AASError, OSError, ValueError) as exc:
        print(f"aasctl-shell rejected command: {exc}", file=sys.stderr)
        raise SystemExit(2)

if __name__ == "__main__":
    raise SystemExit("Use one of the installed console scripts: aas-a, aas-b, aasctl-shell")
