"""Real SSHTransport + real aasctl-shell subprocess per call, with only the sshd hop replaced.

A fake 'ssh' (this file, run with --shim) receives the remote command string exactly like sshd
would (as SSH_ORIGINAL_COMMAND) and runs the forced-command entry point. It validates quoting,
binary stdout, exit-code mapping and the ACK/cleanup path without needing sshd.
Never used for production.
"""
from __future__ import annotations
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def shim(config: str, role: str) -> int:
    os.environ["SSH_ORIGINAL_COMMAND"] = sys.argv[-1]
    sys.argv = ["aasctl-shell", "--config", config, "--role", role]
    from aas.cli import main_shell
    main_shell()
    return 0


def main() -> None:
    from PIL import Image
    from aas.a.core import AService
    from aas.b.core import BService
    from aas.common.config import AConfig, BConfig
    from aas.transport.ssh import SSHTransport
    with tempfile.TemporaryDirectory(prefix="aas-shim-") as tmp:
        root = Path(tmp)
        a_yaml = root / "a.yaml"
        a_yaml.write_text(
            f"node: {{producer_id: shimA, consumer_id: shimB}}\n"
            f"paths: {{root_dir: '{(root / 'cloud').as_posix()}', input_dir: '{(root / 'gen').as_posix()}'}}\n"
            "batch: {max_files: 2, max_wait_seconds: 0, min_age_seconds: 0}\nwatch: {hold_back_newest: 0}\ncrypto: {strategy: none}\n", encoding="utf-8")
        a = AService(AConfig.load(a_yaml))
        for i in range(3):
            Image.new("RGB", (48, 48), (30 * i, 80, 160)).save(a.input / f"shim-{i}.png")
        ids = [a.once(), a.once()]
        print("A batches:", ids, [a.db.get(b)["state"] for b in ids if b])

        class ShimTransport(SSHTransport):
            def _base(self, *, emergency: bool = False):
                return [sys.executable, str(Path(__file__).resolve()), "--shim", str(a_yaml), "emergency" if emergency else "consumer"]

        b = BService(BConfig("shimB", "shimA", root / "local", "h", "u", chunk_bytes=4096, interval_seconds=1), None)
        b.transport = b.discovery.transport = ShimTransport(b.cfg)
        print("B once:", b.once())
        for batch in filter(None, ids):
            print(batch, "A=", a.db.get(batch)["state"], "B=", b.db.state("shimA", batch), "files=", len(list((b.output / "shimA" / batch).iterdir())))
        try:
            b.transport.command("emergency-stream", timeout=10)
        except Exception as exc:  # consumer role must be refused
            print("consumer key refused emergency-stream:", type(exc).__name__)


if __name__ == "__main__":
    if len(sys.argv) > 3 and sys.argv[1] == "--shim":
        raise SystemExit(shim(sys.argv[2], sys.argv[3]))
    main()
