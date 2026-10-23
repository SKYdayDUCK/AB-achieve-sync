"""Tar creation and authoritative safe full archive verification/extraction."""
from __future__ import annotations
import os
from pathlib import Path
import tarfile
import time
import hashlib
from collections.abc import Callable
from aas.common.core import IntegrityError, hash_file, safe_image_name, must_regular, fsync_dir


def file_index(snapshot: list[dict]) -> list[dict]:
    entries = []
    used = set()
    for i, item in enumerate(snapshot):
        name = safe_image_name(item["name"])
        key = name.casefold()
        if key in used:
            raise IntegrityError("case-insensitive file name collision")
        used.add(key)
        entries.append({"name": name, "archive_path": f"images/{i:08d}__{name}", "size": item["size"], "sha256": item["sha256"]})
    return entries


def create_tar(staging: Path, entries: list[dict], target: Path, snapshot: list[dict]) -> None:
    expected = {r["name"]: r for r in snapshot}
    with target.open("wb") as raw:
        with tarfile.open(fileobj=raw, mode="w|", format=tarfile.PAX_FORMAT) as tar:
            for record in entries:
                path = staging / safe_image_name(record["name"])
                st = must_regular(path)
                source = expected[record["name"]]
                if (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns) != (source["dev"], source["ino"], source["size"], source["mtime_ns"]):
                    raise IntegrityError("source changed during packaging")
                info = tarfile.TarInfo(name=record["archive_path"])
                info.size = record["size"]
                info.mtime = 0
                info.mode = 0o644
                info.uid = info.gid = 0
                info.uname = info.gname = ""
                with path.open("rb") as file:
                    tar.addfile(info, file)
                after = must_regular(path)
                if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns):
                    raise IntegrityError("source changed while being archived")
        raw.flush()
        os.fsync(raw.fileno())


def verify_tar(archive: Path, entries: list[dict], *, destination: Path | None = None, max_total_bytes: int | None = None) -> None:
    """Full streaming tar read; rejects special files, extras, duplicates, path collisions.

    If destination is supplied, extract only trusted manifest basenames into a private empty dir.
    """
    if not isinstance(entries, list) or len(entries) > 100000:
        raise IntegrityError("bad manifest file index")
    expect = {}
    output_names = set()
    total = 0
    for e in entries:
        name = safe_image_name(e["name"])
        output_key = name.casefold()
        if output_key in output_names:
            raise IntegrityError("case-insensitive output collision")
        output_names.add(output_key)
        member = e["archive_path"]
        if not isinstance(member, str) or member.startswith("/") or "\\" in member or member != f"images/{len(expect):08d}__{name}" or member in expect:
            raise IntegrityError("invalid archive member path")
        if type(e["size"]) is not int or e["size"] < 0 or not isinstance(e["sha256"], str) or len(e["sha256"]) != 64:
            raise IntegrityError("bad manifest entry")
        expect[member] = e
        total += e["size"]
    if max_total_bytes is not None and total > max_total_bytes:
        raise IntegrityError("manifest exceeds size budget")
    if destination is not None:
        destination.mkdir(parents=True, exist_ok=False)
    visited = set()
    actual_total = 0
    try:
        with tarfile.open(archive, mode="r|*") as tar:
            for member in tar:
                if member.name not in expect or member.name in visited or not member.isfile():
                    raise IntegrityError(f"unknown, duplicate, or special tar member: {member.name!r}")
                e = expect[member.name]
                if member.size != e["size"]:
                    raise IntegrityError("tar member size mismatch")
                reader = tar.extractfile(member)
                if reader is None:
                    raise IntegrityError("tar member unreadable")
                writer = None
                try:
                    if destination is not None:
                        out = destination / e["name"]
                        writer = out.open("xb")
                    h = hashlib.sha256()
                    consumed = 0
                    while block := reader.read(1024 * 1024):
                        consumed += len(block)
                        if consumed > e["size"]:
                            raise IntegrityError("oversized tar member")
                        h.update(block)
                        if writer is not None:
                            writer.write(block)
                    if consumed != e["size"] or h.hexdigest() != e["sha256"]:
                        raise IntegrityError("image content mismatch")
                    if writer is not None:
                        writer.flush()
                        os.fsync(writer.fileno())
                finally:
                    if writer is not None:
                        writer.close()
                visited.add(member.name)
                actual_total += consumed
        if visited != set(expect) or actual_total != total:
            raise IntegrityError("archive incomplete")
        if destination is not None:
            fsync_dir(destination)
    except (tarfile.TarError, EOFError, OSError) as exc:
        raise IntegrityError("tar parsing/extracting failed") from exc
