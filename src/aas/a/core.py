"""A workflow: durable claim → final payload VerifyGate → publish → source cleanup
and authorized ACK → tombstone → bundle deletion. No remote caller chooses a path.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
import shutil
import stat
import sys
import time
from PIL import Image, UnidentifiedImageError
from aas.common.config import AConfig
from aas.common.core import (
    AASError, CapacityError, IntegrityError, ProtocolError, TemporaryError,
    IMAGE_SUFFIXES, atomic_write, available_bytes, fsync_dir, hash_file, hash_obj, must_regular,
    new_batch_id, no_symlink_dir, now_utc, process_lock, read_json, rename_new,
    remove_owned_tree, safe_image_name, sha_bytes, validate_id, write_json,
)
from aas.crypto.core import crypto_for, AgeCrypto
from aas.archive.tar import file_index, create_tar, verify_tar
from .store import AStore


class VerifyGate:
    """Trusted verifier. Never checks strategy-provided 'success' flags."""
    VERSION = "aas-verifygate/1"

    @staticmethod
    def verify_final_bundle(bundle: Path, manifest: dict, snapshot: list[dict], *, audit_key: Path | None, temporary_plain: Path) -> dict:
        must_regular(bundle)
        got_sha, got_bytes = hash_file(bundle)
        if got_sha != manifest["payload_sha256"] or got_bytes != manifest["payload_bytes"]:
            raise IntegrityError("VerifyGate: final encrypted payload bytes/hash mismatch")
        crypto = crypto_for(manifest["crypto"]["scheme"])
        try:
            crypto.decrypt_file(bundle, temporary_plain, identity=audit_key)
            raw_sha, _ = hash_file(temporary_plain)
            if raw_sha != manifest["plaintext_archive_sha256"]:
                raise IntegrityError("VerifyGate: plaintext archive hash mismatch")
            index = file_index(snapshot)
            if hash_obj(index) != manifest["files_index_sha256"] or index != manifest["files"]:
                raise IntegrityError("VerifyGate: archived index differs from immutable snapshot")
            if hash_obj(snapshot) != manifest["source_snapshot_sha256"]:
                raise IntegrityError("VerifyGate: source snapshot mismatch")
            verify_tar(temporary_plain, index, max_total_bytes=manifest["source_bytes"])
            if manifest["file_count"] != len(index) or manifest["source_bytes"] != sum(x["size"] for x in index):
                raise IntegrityError("VerifyGate: bad manifest file accounting")
        finally:
            temporary_plain.unlink(missing_ok=True)
        return {
            "schema_version": 1,
            "producer_id": manifest["producer_id"],
            "batch_id": manifest["batch_id"],
            "result": "PASS",
            "final_payload_sha256": got_sha,
            "payload_bytes": got_bytes,
            "manifest_sha256": hash_obj(manifest),
            "source_snapshot_sha256": manifest["source_snapshot_sha256"],
            "files_index_sha256": manifest["files_index_sha256"],
            "plaintext_archive_sha256": manifest["plaintext_archive_sha256"],
            "verified_at": now_utc(),
            "verifier_version": VerifyGate.VERSION,
        }


class AService:
    def __init__(self, config: AConfig):
        self.cfg = config
        self.root = config.root
        self.input = config.input_dir
        self.staging = self.root / "staging"
        self.building = self.root / "building"
        self.ready = self.root / "ready"
        self.state = self.root / "state"
        self.emergency_claim = self.root / "emergency_claim"
        for path in (self.root, self.input, self.staging, self.building, self.ready, self.state, self.emergency_claim, self.root / "quarantine", self.state / "audit_keys"):
            path.mkdir(parents=True, exist_ok=True)
            no_symlink_dir(path)
        if os.stat(self.staging).st_dev != os.stat(self.input).st_dev:
            raise ProtocolError("input and staging must be on the same filesystem for atomic claims")
        if os.stat(self.building).st_dev != os.stat(self.ready).st_dev:
            raise ProtocolError("building and ready require a shared filesystem")
        if os.stat(self.emergency_claim).st_dev != os.stat(self.input).st_dev:
            raise ProtocolError("emergency_claim must be on input filesystem")
        self.db = AStore(self.state / "aas_a.sqlite3")
        self.last_idle = False

    def _owned_dir(self, parent: Path, batch_id: str, *, must_exist=True) -> Path:
        target = parent / validate_id(batch_id)
        if must_exist:
            no_symlink_dir(target)
        return target

    def _audit_key(self, batch_id: str) -> Path:
        return self.state / "audit_keys" / (validate_id(batch_id) + ".key")

    def _open_paths(self) -> set[str]:
        """Paths under input/ that some process still holds open (Linux /proc). A writer that is
        mid-write and paused longer than min_age_seconds is invisible to the mtime heuristic but not to this."""
        if not sys.platform.startswith("linux"):
            return set()
        prefix = str(self.input) + os.sep
        held: set[str] = set()
        try:
            pids = [p for p in os.listdir("/proc") if p.isdigit()]
        except OSError:
            return held
        for pid in pids:
            try:
                fds = os.listdir(f"/proc/{pid}/fd")
            except OSError:
                continue
            for fd in fds:
                try:
                    target = os.readlink(f"/proc/{pid}/fd/{fd}")
                except OSError:
                    continue
                if target.startswith(prefix):
                    held.add(target)
        return held

    def input_summary(self) -> dict:
        """What the directory watcher sees, including files it will NOT pick up (so nothing is silently stuck)."""
        waiting, subdirs, temp, other, rejected, oldest = 0, 0, 0, 0, [], 0.0
        for path in self.input.iterdir():
            if path.is_dir():
                subdirs += 1
            elif path.suffix.lower() in IMAGE_SUFFIXES:
                try:
                    safe_image_name(path.name)
                    if path.is_symlink():
                        raise IntegrityError("symlink")
                    waiting += 1
                    oldest = max(oldest, time.time() - path.stat().st_mtime)
                except (IntegrityError, OSError):
                    rejected.append(path.name)
            elif path.suffix.lower() in (".tmp", ".part"):
                temp += 1
            else:
                other += 1
        return {"images_waiting": waiting, "oldest_waiting_age_s": round(oldest, 1), "in_progress_tmp_files": temp,
                "rejected_unsafe_names": rejected[:20], "non_image_ignored": other, "subdirs_ignored": subdirs}

    def _scan_input(self) -> tuple[list[tuple[int, str]], int]:
        """All plausible image files as (mtime_ns, name) oldest first, plus the newest sign of producer activity
        (image or in-progress .tmp/.part). Cheap: one lstat per entry, no decoding."""
        entries, activity = [], 0
        for path in self.input.iterdir():
            try:
                if path.is_symlink() or not path.is_file():
                    continue
                suffix = path.suffix.lower()
                st = path.lstat()
                if suffix in (".tmp", ".part"):
                    activity = max(activity, st.st_mtime_ns)
                elif suffix in IMAGE_SUFFIXES:
                    safe_image_name(path.name)  # unsafe names are never packed, so they must not hold others back
                    entries.append((st.st_mtime_ns, path.name))
            except (OSError, IntegrityError):
                continue
        entries.sort()
        if entries:
            activity = max(activity, entries[-1][0])
        return entries, activity

    def idle_threshold_seconds(self, entries: list[tuple[int, str]]) -> float:
        """factor x median gap between consecutive image mtimes (recent claimed + waiting), clamped.
        Until 3 gaps are known the (long) maximum is used, so a cold start never flushes early."""
        stamps = sorted(set(self.db.recent_claimed_mtimes_ns()) | {m for m, _ in entries})
        gaps = [(b - a) / 1e9 for a, b in zip(stamps, stamps[1:])][-20:]
        if len(gaps) < 3:
            return self.cfg.idle_flush_max_seconds
        gaps.sort()
        median = gaps[len(gaps) // 2]
        return min(max(self.cfg.idle_flush_factor * median, self.cfg.idle_flush_min_seconds), self.cfg.idle_flush_max_seconds)

    def watch_state(self) -> dict:
        entries, activity = self._scan_input()
        if not entries and not activity:
            return {"idle": False, "held_back": [], "last_activity_age_s": None, "idle_flush_threshold_s": None}
        threshold = self.idle_threshold_seconds(entries)
        age = time.time() - activity / 1e9
        idle = age >= threshold
        held = [] if idle or self.cfg.hold_back_newest == 0 else [n for _, n in entries[-self.cfg.hold_back_newest:]]
        return {"idle": idle, "held_back": held, "last_activity_age_s": round(age, 1), "idle_flush_threshold_s": round(threshold, 1)}

    def _eligible_names(self, *, emergency: bool = False, limit: int | None = None) -> list[str]:
        candidates = []
        open_paths: set[str] | None = None
        entries, activity = self._scan_input()
        self.last_idle = False
        held: set[str] = set()
        if entries and not emergency:
            self.last_idle = (time.time() - activity / 1e9) >= self.idle_threshold_seconds(entries)
            if self.cfg.hold_back_newest and not self.last_idle:
                # A sequential producer that has started a newer file is done with the older ones.
                held = {name for _, name in entries[-self.cfg.hold_back_newest:]}
        for _, listed_name in entries:  # oldest first = generation order
            if limit is not None and len(candidates) >= limit:
                break  # do not Image.verify() an unbounded backlog on every scan
            if listed_name in held:
                continue
            path = self.input / listed_name
            try:
                name = safe_image_name(path.name)
                stat_before = must_regular(path)
                if stat_before.st_size < 1 or time.time() - stat_before.st_mtime < self.cfg.min_age_seconds:
                    continue
                if self.cfg.skip_open_files:
                    if open_paths is None:
                        open_paths = self._open_paths()
                    if str(path) in open_paths:
                        continue  # a writer still holds it open: not complete
                with Image.open(path) as image:
                    image.verify()
                stat_after = must_regular(path)
                if (stat_before.st_ino, stat_before.st_mtime_ns, stat_before.st_size) != (stat_after.st_ino, stat_after.st_mtime_ns, stat_after.st_size):
                    continue
                candidates.append((name, stat_before.st_size))
            except (IntegrityError, OSError, ValueError, UnidentifiedImageError):
                continue  # preserve incomplete/unknown inputs; never delete them
        selected, used = [], 0
        for name, size in candidates:  # already in generation order
            if emergency:
                selected.append(name)
                continue
            if len(selected) >= self.cfg.max_files or (selected and used + size > min(self.cfg.max_bytes, self.cfg.max_batch_bytes)):
                break
            if size > self.cfg.max_batch_bytes:
                continue
            selected.append(name)
            used += size
        return selected

    def recover_emergency_claims(self):
        """Unconfirmed emergency claims may repeat by design. Never overwrite new input."""
        with process_lock(self.state / "input_claim.lock"):
            for path in self.emergency_claim.iterdir():
                must_regular(path)
                if "__" not in path.name:
                    continue
                _, name = path.name.split("__", 1)
                try:
                    safe_image_name(name)
                except IntegrityError:
                    continue
                target = self.input / name
                if target.exists() or target.is_symlink():
                    prefix = f"recovered_{os.urandom(6).hex()}_"
                    stem, suffix = name[: -len(Path(name).suffix)], Path(name).suffix
                    budget = 220 - len((prefix + suffix).encode("utf-8"))
                    new_name = prefix + stem.encode("utf-8")[:budget].decode("utf-8", errors="ignore") + suffix
                    safe_image_name(new_name)
                    target = self.input / new_name
                rename_new(path, target)  # atomic NOREPLACE even if generator creates same name

    def claim(self) -> str | None:
        with process_lock(self.state / "input_claim.lock"):
            names = self._eligible_names(limit=self.cfg.max_files)
            if not names:
                return None
            chosen_bytes = sum((self.input / n).stat().st_size for n in names)
            oldest_age = max(time.time() - (self.input / n).stat().st_mtime for n in names)
            if len(names) < self.cfg.max_files and chosen_bytes < self.cfg.max_bytes and oldest_age < self.cfg.max_wait_seconds and not self.last_idle:
                return None  # batch threshold not reached; don't produce tiny batches every scan
            batch_id = new_batch_id()
            self.db.create(batch_id)
            dest = self.staging / batch_id
            dest.mkdir()
            fsync_dir(self.staging)
            count = 0
            for name in names:
                source = self.input / name
                target = dest / name
                try:
                    before = must_regular(source)
                    os.rename(source, target)
                    after = must_regular(target)
                    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                        raise IntegrityError("identity changed while claiming")
                    count += 1
                except FileNotFoundError:
                    continue  # file removed by producer; remaining claims are still owned
            fsync_dir(dest)
            fsync_dir(self.input)
            if count:
                self.db.update(batch_id, "CLAIMING", "PACKAGING")
            else:
                dest.rmdir()
                self.db.update(batch_id, "CLAIMING", "EMPTY")
            return batch_id if count else None

    def _snapshot(self, batch_id: str) -> list[dict]:
        folder = self._owned_dir(self.staging, batch_id)
        result = []
        for source in sorted(folder.iterdir(), key=lambda p: p.name):
            name = safe_image_name(source.name)
            st = must_regular(source)
            digest, size = hash_file(source)
            after = must_regular(source)
            if (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                raise IntegrityError("claimed file changed while hashing")
            result.append({"name": name, "size": size, "sha256": digest, "dev": st.st_dev, "ino": st.st_ino, "mtime_ns": st.st_mtime_ns})
        if not result:
            raise IntegrityError("no claimed items")
        if len({x["name"].casefold() for x in result}) != len(result):
            raise IntegrityError("case insensitive input collision")
        return result

    def _assert_capacity(self, total: int):
        # staging original remains. Tar + encrypted bundle + audit decrypted tar may coexist.
        required = 3 * (total + max(1048576, total // 100)) + self.cfg.critical_free_bytes
        if available_bytes(self.root) < required:
            raise CapacityError(f"A insufficient capacity; need {required} additional free bytes")

    def package(self, batch_id: str, *, before_publish=None) -> None:
        row = self.db.get(batch_id)
        if row["state"] != "PACKAGING":
            raise IntegrityError("package requires PACKAGING")
        folder = self._owned_dir(self.staging, batch_id)
        snapshot = self._snapshot(batch_id)
        index = file_index(snapshot)
        total = sum(x["size"] for x in snapshot)
        self._assert_capacity(total)
        build = self.building / batch_id
        if build.exists():
            remove_owned_tree(build)  # private scratch, only pre-cert during PACKAGING
        build.mkdir()
        fsync_dir(self.building)
        tar_path = build / "plaintext.tar.part"
        bundle = build / "bundle"
        decrypted = build / "verified.tar.part"
        key = self._audit_key(batch_id)
        if key.exists():
            key.unlink()  # rebuild requires new temporary identity
        strategy = crypto_for(self.cfg.crypto_scheme)
        try:
            create_tar(folder, index, tar_path, snapshot)
            plain_sha, _ = hash_file(tar_path)
            if self.cfg.crypto_scheme == "age_x25519":
                assert isinstance(strategy, AgeCrypto)
                strategy.make_audit_key(key)
            metadata = strategy.encrypt_file(tar_path, bundle, recipients=self.cfg.recipients, audit_key=key if key.is_file() else None)
            payload_sha, payload_bytes = hash_file(bundle)
            manifest = {
                "schema_version": 1,
                "producer_id": self.cfg.producer_id, "batch_id": batch_id, "seq": row["seq"],
                "payload_sha256": payload_sha, "payload_bytes": payload_bytes,
                "plaintext_archive_sha256": plain_sha, "source_snapshot_sha256": hash_obj(snapshot),
                "files_index_sha256": hash_obj(index), "files": index,
                "file_count": len(index), "source_bytes": total,
                "crypto": metadata, "archive": "tar",
            }
            manifest_path = build / "manifest.json"
            write_json(manifest_path, manifest)
            cert = VerifyGate.verify_final_bundle(bundle, manifest, snapshot, audit_key=key if key.is_file() else None, temporary_plain=decrypted)
            write_json(build / "verification_certificate.json", cert)
            tar_path.unlink()
            for final_file in (bundle, manifest_path, build / "verification_certificate.json"):
                if os.name != "nt":  # read-only attribute would block unlink on Windows
                    final_file.chmod(0o440)  # published artifacts are read-only to ordinary process writes
            fsync_dir(build)
            self.db.update(batch_id, "PACKAGING", "PUBLISHING",
                snapshot_json=json.dumps(snapshot, ensure_ascii=False, sort_keys=True),
                snapshot_sha256=manifest["source_snapshot_sha256"],
                files_index_sha256=manifest["files_index_sha256"],
                manifest_sha256=cert["manifest_sha256"],
                payload_sha256=payload_sha, payload_bytes=payload_bytes, plaintext_sha256=plain_sha,
                file_count=len(index), source_bytes=total)
            if before_publish:
                before_publish()  # test-only fault point; source must be untouched
            self.resume_publish(batch_id)
        except Exception:
            # Before persistent PUBLISHING, all source images remain in staging.
            if self.db.get(batch_id)["state"] == "PACKAGING":
                if key.exists():
                    key.unlink()
                self.db.audit(batch_id, "BUILD_RETRYABLE_FAILURE", "source files retained")
            raise

    def _validate_proof(self, batch_id: str, folder: Path) -> tuple[dict, dict]:
        row = self.db.get(batch_id)
        no_symlink_dir(folder)
        for name in ("bundle", "manifest.json", "verification_certificate.json"):
            must_regular(folder / name)
        manifest_path = folder / "manifest.json"
        bundle_path = folder / "bundle"
        cert = read_json(folder / "verification_certificate.json")
        manifest = read_json(manifest_path)
        manifest_sha, _ = hash_file(manifest_path)
        payload_sha, payload_bytes = hash_file(bundle_path)
        # Json is written canonically. Byte-level hashes detect unexpected rewrites.
        if cert.get("schema_version") != 1 or cert.get("result") != "PASS" or cert.get("batch_id") != batch_id or cert.get("producer_id") != self.cfg.producer_id or cert.get("verifier_version") != VerifyGate.VERSION:
            raise IntegrityError("certificate identity/format invalid")
        bindings = (
            ("manifest_sha256", manifest_sha), ("final_payload_sha256", payload_sha),
            ("payload_bytes", payload_bytes), ("source_snapshot_sha256", row["snapshot_sha256"]),
            ("files_index_sha256", row["files_index_sha256"]), ("plaintext_archive_sha256", row["plaintext_sha256"]),
        )
        if any(cert.get(k) != expected for k, expected in bindings) or row["manifest_sha256"] != manifest_sha or row["payload_sha256"] != payload_sha or row["payload_bytes"] != payload_bytes:
            raise IntegrityError("persisted verification certificate/DB/payload mismatch")
        if manifest.get("batch_id") != batch_id or manifest.get("producer_id") != self.cfg.producer_id or manifest.get("seq") != row["seq"] or manifest.get("source_snapshot_sha256") != row["snapshot_sha256"] or manifest.get("files_index_sha256") != row["files_index_sha256"]:
            raise IntegrityError("manifest/DB identity mismatch")
        snapshot = json.loads(row["snapshot_json"])
        if hash_obj(snapshot) != row["snapshot_sha256"] or file_index(snapshot) != manifest["files"]:
            raise IntegrityError("snapshot/manifest mismatch")
        if any(x.get("name") != safe_image_name(x.get("name")) for x in snapshot):
            raise IntegrityError("invalid snapshot member")
        return manifest, cert

    def resume_publish(self, batch_id: str):
        row = self.db.get(batch_id)
        if row["state"] != "PUBLISHING":
            raise IntegrityError("resume_publish needs PUBLISHING")
        source = self.building / batch_id
        target = self.ready / batch_id
        if target.exists():
            self._validate_proof(batch_id, self._owned_dir(self.ready, batch_id))
        else:
            self._validate_proof(batch_id, self._owned_dir(self.building, batch_id))
            rename_new(source, target)
        self.db.update(batch_id, "PUBLISHING", "SOURCE_CLEANING")
        self._audit_key(batch_id).unlink(missing_ok=True)  # audit credential not needed by recovery
        self.clean_sources(batch_id)

    def clean_sources(self, batch_id: str, *, after_delete=None):
        row = self.db.get(batch_id)
        if row["state"] != "SOURCE_CLEANING":
            raise IntegrityError("source cleaning requires SOURCE_CLEANING")
        self._audit_key(batch_id).unlink(missing_ok=True)  # certificate replaces it; never keep a decrypt key around
        try:
            self._validate_proof(batch_id, self._owned_dir(self.ready, batch_id))
        except Exception as exc:
            self.db.update(batch_id, "SOURCE_CLEANING", "VERIFY_RECOVERY_BLOCKED", error=str(exc)[:500])
            raise
        snapshot = json.loads(row["snapshot_json"])
        stage = self.staging / validate_id(batch_id)
        if not stage.exists():
            # Crash after rmdir(staging/<id>) but before DB READY commit is recoverable
            # only if the persistent deletion audit proves every source was unlinked.
            with self.db.tx() as c:
                done = {r[0] for r in c.execute("SELECT detail FROM audit_log WHERE batch_id=? AND action IN ('SOURCE_UNLINKED','SOURCE_ABSENT_CONFIRMED')", (batch_id,))}
            if done != {record["name"] for record in snapshot}:
                self.db.update(batch_id, "SOURCE_CLEANING", "VERIFY_RECOVERY_BLOCKED", error="missing staging directory without complete unlink ledger")
                raise IntegrityError("staging absent without complete source deletion evidence")
            self.db.mark_ready(batch_id)
            return
        no_symlink_dir(stage)
        for record in snapshot:
            name = safe_image_name(record["name"])
            path = stage / name
            if path.exists() or path.is_symlink():
                stat_now = must_regular(path)
                expected = (record["dev"], record["ino"], record["size"], record["mtime_ns"])
                actual = (stat_now.st_dev, stat_now.st_ino, stat_now.st_size, stat_now.st_mtime_ns)
                if actual != expected:
                    self.db.update(batch_id, "SOURCE_CLEANING", "VERIFY_RECOVERY_BLOCKED", error="staging file identity changed")
                    raise IntegrityError("staging identity changed: do not delete")
                path.unlink()
                fsync_dir(stage)
                self.db.audit(batch_id, "SOURCE_UNLINKED", name)
                if after_delete:
                    after_delete(name)  # test-only crash injection
            else:
                # Crash may occur after fsync(unlink) but before its audit insert.
                # Earlier PASS certificate proves membership; do not invent content.
                self.db.audit(batch_id, "SOURCE_ABSENT_CONFIRMED", name)
        unknown = list(stage.iterdir())
        if unknown:
            self.db.update(batch_id, "SOURCE_CLEANING", "VERIFY_RECOVERY_BLOCKED", error="unknown staging files")
            raise IntegrityError("unknown staging files remain")
        stage.rmdir()
        fsync_dir(self.staging)
        self.db.mark_ready(batch_id)

    def info(self, batch_id: str) -> dict:
        row = self.db.get(batch_id)
        if row["state"] != "READY":
            raise ProtocolError("batch not READY")
        manifest_path = self._owned_dir(self.ready, batch_id) / "manifest.json"
        must_regular(manifest_path)
        data = read_json(manifest_path)
        h, _ = hash_file(manifest_path)
        if h != row["manifest_sha256"]:
            raise IntegrityError("manifest altered after publication")
        return data

    def get_range(self, batch_id: str, offset: int, length: int, sink):
        row = self.db.get(batch_id)
        if row["state"] != "READY":
            raise ProtocolError("batch not READY")
        if type(offset) is not int or type(length) is not int or offset < 0 or length < 1 or length > 16777216 or offset + length > row["payload_bytes"]:
            raise ProtocolError("invalid get range")
        bundle = self._owned_dir(self.ready, batch_id) / "bundle"
        st = must_regular(bundle)
        if st.st_size != row["payload_bytes"]:
            raise IntegrityError("published payload size changed")
        with bundle.open("rb") as stream:
            stream.seek(offset)
            remaining = length
            while remaining:
                piece = stream.read(min(1024 * 1024, remaining))
                if not piece:
                    raise IntegrityError("short get_range read")
                sink.write(piece)
                remaining -= len(piece)
        sink.flush()

    def ack(self, request: dict) -> str:
        result = self.db.accept_ack(request, producer_id=self.cfg.producer_id, consumer_id=self.cfg.consumer_id)
        if result in ("accepted", "already_acked"):
            try:
                self.clean_bundle(request["batch_id"])
            except (OSError, IntegrityError) as exc:
                self.db.audit(request["batch_id"], "BUNDLE_CLEAN_RETRY", str(exc))
        return result

    def clean_bundle(self, batch_id: str, *, after_delete=None):
        row = self.db.get(batch_id)
        if row["state"] not in ("ACKED", "DELETING") or not self.db.is_acked(batch_id):
            raise IntegrityError("bundle deletion without durable matching ACK")
        target = self.ready / validate_id(batch_id)
        if row["state"] == "DELETING" and not target.exists() and not target.is_symlink():
            # Crash after rmdir(ready/<id>) but before the terminal DB transaction.
            self.db.update(batch_id, "DELETING", "DELETED")
            return
        no_symlink_dir(target)
        if row["state"] == "ACKED":
            # First check bundle+manifest identity, then transition to durable DELETING.
            self._validate_proof(batch_id, target)
            self.db.update(batch_id, "ACKED", "DELETING")
        # After DELETING, per-file absence is allowed ONLY under ACK tombstone and prior proof.
        for name, attr in (("bundle", "payload_sha256"), ("manifest.json", "manifest_sha256"), ("verification_certificate.json", None)):
            path = target / name
            if path.exists() or path.is_symlink():
                must_regular(path)
                if attr is not None and hash_file(path)[0] != row[attr]:
                    raise IntegrityError("deletion target replaced/corrupted")
                if attr is None:
                    cert = read_json(path)
                    if cert.get("batch_id") != batch_id or cert.get("final_payload_sha256") != row["payload_sha256"] or cert.get("manifest_sha256") != row["manifest_sha256"]:
                        raise IntegrityError("deletion certificate mismatch")
                path.unlink()
                fsync_dir(target)
                if after_delete:
                    after_delete(name)  # test-only crash injection
        if list(target.iterdir()):
            raise IntegrityError("unexpected file in deletion target")
        target.rmdir()
        fsync_dir(self.ready)
        self.db.update(batch_id, "DELETING", "DELETED")

    def recover(self):
        self.recover_emergency_claims()
        for row in self.db.rows("CLAIMING"):
            bid = row["batch_id"]
            stage = self.staging / bid
            if stage.is_dir() and any(stage.iterdir()):
                self.db.update(bid, "CLAIMING", "PACKAGING")
            elif stage.is_dir():
                stage.rmdir()
                self.db.update(bid, "CLAIMING", "EMPTY")
            else:
                self.db.update(bid, "CLAIMING", "EMPTY")
        for row in self.db.rows("PACKAGING", "PUBLISHING", "SOURCE_CLEANING", "ACKED", "DELETING"):
            bid, state = row["batch_id"], row["state"]
            try:
                if state == "PACKAGING":
                    self.package(bid)
                elif state == "PUBLISHING":
                    self.resume_publish(bid)
                elif state == "SOURCE_CLEANING":
                    self.clean_sources(bid)
                else:
                    self.clean_bundle(bid)
            except (AASError, OSError, ValueError, RuntimeError) as exc:
                self.db.audit(bid, "RECOVERY_ERROR", f"{state}: {exc}")
                # Leave PUBLISHING/PACKAGING/ACKED/DELETING retryable where safe.

    def reannounce_ready(self, batch_id: str) -> int:
        """Operator-only reissue of READY event, preserving seq and immutable payload."""
        bid = validate_id(batch_id)
        row = self.db.get(bid)
        if row["state"] != "READY":
            raise ProtocolError("only READY may be reannounced")
        self._validate_proof(bid, self._owned_dir(self.ready, bid))
        return self.db.reannounce_ready(bid)

    def retry_blocked(self, batch_id: str):
        """Operator-only retry after independently repairing an exact-byte proof mismatch."""
        bid = validate_id(batch_id)
        row = self.db.get(bid)
        if row["state"] != "VERIFY_RECOVERY_BLOCKED":
            raise ProtocolError("batch is not VERIFY_RECOVERY_BLOCKED")
        # A human may restore the original exact-byte payload from a trusted backup.
        # Merely toggling a state cannot authorize source deletion.
        self._validate_proof(bid, self._owned_dir(self.ready, bid))
        self.db.audit(bid, "OPERATOR_REVALIDATED_BLOCKED", "exact-byte certificate and DB proof matched")
        self.db.update(bid, "VERIFY_RECOVERY_BLOCKED", "SOURCE_CLEANING", error=None)
        self.clean_sources(bid)

    def once(self) -> str | None:
        self.recover()
        bid = self.claim()
        if bid:
            self.package(bid)
        return bid

    def hello(self) -> dict:
        with self.db.tx() as c:
            high = c.execute("SELECT COALESCE(MAX(event_id),0) FROM ready_events").fetchone()[0]
        return {"protocol_version": 1, "producer_id": self.cfg.producer_id, "protocol_epoch": self.db.epoch(), "last_ready_event_id": high, "server_time": now_utc(), "capabilities": ["list-ready", "wait-ready", "info", "get-range", "ack", "ledger", "crypto-probe", "emergency-stream"]}

    def wait_ready(self, after_event_id: int, timeout_seconds: float, *, tick: float = 0.5) -> dict:
        """Read-only wake-up hint for B: block until a READY event newer than `after_event_id` exists.
        It carries no data B must trust: B still runs the normal paginated list-ready scan afterwards."""
        if type(after_event_id) is not int or after_event_id < 0:
            raise ProtocolError("invalid after_event_id")
        timeout = min(max(float(timeout_seconds), 0.1), 110.0)
        deadline = time.monotonic() + timeout
        while True:
            high = self.db.max_event_id()
            if high > after_event_id or time.monotonic() >= deadline:
                return {"changed": high > after_event_id, "last_ready_event_id": high, "server_time": now_utc()}
            time.sleep(min(tick, max(0.01, deadline - time.monotonic())))

    def list_ready(self, *, after_event_id=0, page_size=100, snapshot_high=None):
        result = self.db.list_ready(after_event_id=after_event_id, page_size=page_size, snapshot_high=snapshot_high)
        result["producer_id"] = self.cfg.producer_id
        result["server_time"] = now_utc()  # A-clock instant of the listing; extra field, older B ignores it
        return result

    def ledger(self, since_seq: int = 0) -> list[dict]:
        if since_seq < 0:
            raise ProtocolError("bad seq")
        with self.db.tx() as c:
            return [dict(r) for r in c.execute("SELECT batch_id,seq,state,ready_event_id,payload_sha256 FROM batches WHERE seq>? ORDER BY seq LIMIT 1000", (since_seq,))]

    def status(self):
        with self.db.tx() as c:
            statuses = [dict(r) for r in c.execute("SELECT state,COUNT(*) AS count FROM batches GROUP BY state")]
        free = available_bytes(self.root)
        return {"producer_id": self.cfg.producer_id, "free_bytes": free,
                "capacity_warning": free < self.cfg.warn_free_bytes,
                "capacity_critical": free < self.cfg.critical_free_bytes,
                "batches_by_state": statuses, "input": self.input_summary(), "watch": self.watch_state()}
