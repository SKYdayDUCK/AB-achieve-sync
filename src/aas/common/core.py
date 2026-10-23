"""Shared trusted primitives. No remote path is ever interpreted as a filesystem path."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import sys
import tempfile
import unicodedata
import uuid


class AASError(Exception):
    pass


class IntegrityError(AASError):
    pass


class ProtocolError(AASError):
    pass


class AuthenticationError(ProtocolError):
    pass


class TemporaryError(AASError):
    pass


class HandoffUncertain(AASError):
    pass


class CapacityError(TemporaryError):
    pass


ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,95}$")
IMAGE_SUFFIXES = frozenset((".png", ".jpeg", ".jpg", ".webp", ".bmp", ".gif", ".tif", ".tiff"))
WIN_RESERVED = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def canonical(obj: object) -> bytes:
    return (json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")


def sha_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def hash_obj(obj: object) -> str:
    return sha_bytes(canonical(obj))


def hash_file(path: Path, *, chunk: int = 1024 * 1024) -> tuple[str, int]:
    h = hashlib.sha256()
    count = 0
    with path.open("rb") as stream:
        while piece := stream.read(chunk):
            h.update(piece)
            count += len(piece)
    return h.hexdigest(), count


def validate_id(identifier: str) -> str:
    if not isinstance(identifier, str) or not ID_PATTERN.fullmatch(identifier):
        raise ProtocolError(f"unsafe identifier: {identifier!r}")
    return identifier


def safe_image_name(name: str) -> str:
    if not isinstance(name, str) or not name or len(name.encode("utf-8")) > 220:
        raise IntegrityError("invalid image basename")
    if unicodedata.normalize("NFC", name) != name:
        raise IntegrityError("image basename must be NFC normalized")
    if name in (".", "..") or name[-1] in (".", " "):
        raise IntegrityError("unsafe image basename")
    if any(c in '/\\:*?"<>|' or ord(c) < 32 for c in name):
        raise IntegrityError("unsafe image basename")
    if name.split(".")[0].upper() in WIN_RESERVED:
        raise IntegrityError("Windows reserved file basename")
    if Path(name).suffix.lower() not in IMAGE_SUFFIXES:
        raise IntegrityError("not an allowed image format")
    return name


def new_batch_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:12]


def fsync_dir(path: Path) -> None:
    if os.name == "nt":
        return  # Windows cannot portably fsync directory handles via the Python stdlib.
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write(path: Path, data: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(prefix=".aas-write-", dir=path.parent)
    try:
        if os.name != "nt":
            os.fchmod(fd, mode)
        with os.fdopen(fd, "wb") as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp, path)
        fsync_dir(path.parent)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def write_json(path: Path, obj: object) -> None:
    atomic_write(path, canonical(obj))


def read_json(path: Path) -> dict:
    try:
        obj = json.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"invalid or missing JSON {path}") from exc
    if not isinstance(obj, dict):
        raise IntegrityError("JSON root not an object")
    return obj


def no_symlink_dir(path: Path) -> None:
    if path.is_symlink() or not path.is_dir():
        raise IntegrityError(f"directory missing or symlink: {path}")


def must_regular(path: Path) -> os.stat_result:
    st = path.lstat()
    if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
        raise IntegrityError(f"not an ordinary unlinked regular file: {path}")
    return st


def rename_new(source: Path, target: Path) -> None:
    """Atomic directory rename without clobber, Linux/Windows only. Fail closed elsewhere."""
    if os.name == "nt":
        if target.exists() or target.is_symlink():
            raise FileExistsError(str(target))
        os.rename(source, target)  # Windows refuses replacing an existing directory.
    elif sys.platform.startswith("linux"):
        import ctypes
        libc = ctypes.CDLL(None, use_errno=True)
        func = getattr(libc, "renameat2", None)
        if func is None:
            raise OSError("renameat2 unavailable; fail-closed no-clobber delivery")
        func.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        func.restype = ctypes.c_int
        rc = func(-100, os.fsencode(source), -100, os.fsencode(target), 1)
        if rc != 0:
            err = ctypes.get_errno()
            raise OSError(err, os.strerror(err), str(target))
    else:
        raise OSError("atomic no-clobber directory rename not supported on this OS")
    fsync_dir(source.parent)
    if source.parent != target.parent:
        fsync_dir(target.parent)


def remove_owned_tree(path: Path) -> None:
    """Only for private B restore/build scratch, NEVER user output or A published data."""
    if path.exists():
        no_symlink_dir(path)
        for child in path.iterdir():
            if child.is_symlink():
                child.unlink()
            elif child.is_dir():
                remove_owned_tree(child)
            else:
                child.unlink()
        path.rmdir()
        fsync_dir(path.parent)


def available_bytes(path: Path) -> int:
    return shutil.disk_usage(path).free


@contextmanager
def process_lock(path: Path):
    """Cross-platform nonblocking advisory lock held by the process for a critical section."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as f:
        if os.name == "nt":
            import msvcrt
            f.seek(0, os.SEEK_END)
            if f.tell() == 0:
                f.write(b"0")
                f.flush()
            f.seek(0)
            try:
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise TemporaryError(f"lock busy: {path}") from exc
            try:
                yield
            finally:
                f.seek(0)
                msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise TemporaryError(f"lock busy: {path}") from exc
            try:
                yield
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
