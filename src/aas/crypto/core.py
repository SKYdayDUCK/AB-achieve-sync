"""Whitelisted crypto backends. VerifyGate, not this module, authorizes source deletion."""
from __future__ import annotations
import base64
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from typing import Protocol
from aas.common.core import AASError, IntegrityError, ProtocolError, TemporaryError, sha_bytes


class CryptoStrategy(Protocol):
    scheme: str
    version: int
    def selftest(self) -> None: ...
    def encrypt_file(self, source: Path, destination: Path, *, recipients: tuple[str, ...], audit_key: Path | None) -> dict: ...
    def decrypt_file(self, source: Path, destination: Path, *, identity: Path | None) -> None: ...


class NoneCrypto:
    scheme = "none"
    version = 1

    def selftest(self) -> None:
        return

    def encrypt_file(self, source: Path, destination: Path, *, recipients=(), audit_key=None) -> dict:
        shutil.copyfile(source, destination)
        with destination.open("r+b") as f:  # fsync needs a writable handle on Windows
            os.fsync(f.fileno())
        return {"scheme": self.scheme, "version": self.version, "recipient_key_ids": []}

    def decrypt_file(self, source: Path, destination: Path, *, identity=None) -> None:
        shutil.copyfile(source, destination)
        with destination.open("r+b") as f:  # fsync needs a writable handle on Windows
            os.fsync(f.fileno())


class AgeCrypto:
    scheme = "age_x25519"
    version = 1

    def selftest(self) -> None:
        if not shutil.which("age") or not shutil.which("age-keygen"):
            raise ProtocolError("age and age-keygen binaries required")

    @staticmethod
    def run(*cmd: str, timeout: float | None = None) -> subprocess.CompletedProcess:
        try:
            # Large 4GiB archives may legitimately exceed five minutes.
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        except (subprocess.TimeoutExpired, OSError) as exc:
            raise TemporaryError(f"age subprocess unavailable/timed out: {exc}") from exc
        if proc.returncode:
            raise IntegrityError(f"age command failed ({proc.returncode}): {proc.stderr.decode(errors='replace')[:400]}")
        return proc

    @staticmethod
    def normalize_recipients(recipients: tuple[str, ...]) -> tuple[str, ...]:
        result = []
        for r in recipients:
            candidate = Path(r).expanduser()
            pub = candidate.read_text(encoding="utf-8").strip() if candidate.is_file() else r.strip()
            if not re.fullmatch(r"age1[023456789acdefghjklmnpqrstuvwxyz]{20,120}", pub):
                raise ProtocolError(f"invalid age public key: {r}")
            result.append(pub)
        return tuple(result)

    def make_audit_key(self, key_path: Path) -> str:
        self.selftest()
        key_path.parent.mkdir(parents=True, exist_ok=True)
        if key_path.exists():
            key_path.unlink()  # interrupted pre-verification build; never reused for next build
        self.run("age-keygen", "-o", str(key_path), timeout=30)
        os.chmod(key_path, 0o600)
        return self.run("age-keygen", "-y", str(key_path), timeout=30).stdout.decode().strip()

    def encrypt_file(self, source: Path, destination: Path, *, recipients: tuple[str, ...], audit_key: Path | None) -> dict:
        self.selftest()
        pubs = self.normalize_recipients(recipients)
        if audit_key is None or not audit_key.is_file():
            raise ProtocolError("A audit identity missing")
        audit_pub = self.run("age-keygen", "-y", str(audit_key), timeout=30).stdout.decode().strip()
        cmd = ["age", "-o", str(destination)]
        for recipient in (*pubs, audit_pub):
            cmd.extend(("-r", recipient))
        cmd.append(str(source))
        self.run(*cmd)
        with destination.open("r+b") as f:  # fsync needs a writable handle on Windows
            os.fsync(f.fileno())
        return {"scheme": self.scheme, "version": self.version, "recipient_key_ids": [sha_bytes(r.encode())[:16] for r in pubs]}

    def decrypt_file(self, source: Path, destination: Path, *, identity: Path | None) -> None:
        self.selftest()
        if identity is None or not identity.is_file():
            raise ProtocolError("age private identity required")
        self.run("age", "-d", "-i", str(identity), "-o", str(destination), str(source))
        with destination.open("r+b") as f:  # fsync needs a writable handle on Windows
            os.fsync(f.fileno())


CRYPTO_REGISTRY = {"none": NoneCrypto, "age_x25519": AgeCrypto}


def crypto_for(scheme: str) -> CryptoStrategy:
    if scheme not in CRYPTO_REGISTRY:
        raise ProtocolError("unregistered crypto strategy")
    obj = CRYPTO_REGISTRY[scheme]()
    obj.selftest()
    return obj


def decrypt_with_identities(source: Path, destination: Path, metadata: dict, keys: tuple[str, ...]) -> None:
    strategy = crypto_for(metadata["scheme"])
    if metadata["scheme"] == "none":
        strategy.decrypt_file(source, destination, identity=None)
        return
    failures = []
    for raw in keys:
        try:
            strategy.decrypt_file(source, destination, identity=Path(raw).expanduser())
            return
        except (AASError, OSError) as exc:
            failures.append(str(exc)[:150])
            destination.unlink(missing_ok=True)
    raise IntegrityError("no configured age identity decrypted bundle: " + "; ".join(failures))
