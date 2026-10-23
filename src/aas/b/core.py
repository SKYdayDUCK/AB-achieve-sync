"""B discovery, durable queue, bounded transfers, validation, handoff receipts, ACK."""
from __future__ import annotations
import concurrent.futures
import json
import os
from pathlib import Path
import shutil
import threading
import time
from aas.archive.tar import verify_tar
from aas.common.config import BConfig
from aas.common.core import (
    AASError, AuthenticationError, CapacityError, HandoffUncertain, IntegrityError,
    ProtocolError, TemporaryError, available_bytes, fsync_dir, hash_file, hash_obj,
    no_symlink_dir, now_utc, process_lock, read_json, remove_owned_tree, rename_new,
    safe_image_name, validate_id, write_json,
)
from aas.crypto.core import decrypt_with_identities
from aas.transport.ssh import Transport, SSHTransport
from .scheduling.policy import DiscoveryObservation, FixedIntervalPoll, PollContext, RetryPolicy
from .store import BStore


class DiscoveryCoordinator:
    def __init__(self, producer_id: str, db: BStore, transport: Transport):
        self.producer_id = producer_id
        self.db = db
        self.transport = transport
        self._single_flight = threading.Lock()
        self.last_high: int | None = None  # newest READY event id seen by a complete scan (feeds the wake-up thread)

    def scan(self, *, full_reconcile: bool = False) -> DiscoveryObservation:
        with self._single_flight:
            hello = self.transport.hello()
            if hello.get("producer_id") != self.producer_id or hello.get("protocol_version") != 1:
                raise ProtocolError("producer identity/protocol version mismatch")
            epoch = validate_id(hello["protocol_epoch"])
            cursor = 0 if full_reconcile else self.db.cursor(self.producer_id, epoch)
            if type(hello.get("last_ready_event_id")) is not int or hello["last_ready_event_id"] < cursor:
                raise ProtocolError("READY high-water mark regressed")
            total, new, pages, high = 0, 0, 0, None
            seen_events, server_time = [], ""
            while True:
                page = self.transport.list_ready(cursor, 100, high)
                if page.get("producer_id") != self.producer_id or page.get("protocol_epoch") != epoch:
                    raise ProtocolError("page producer/epoch mismatch")
                if high is None:
                    high = page["snapshot_high"]
                elif page["snapshot_high"] != high:
                    raise ProtocolError("pagination snapshot changed mid-scan")
                next_cursor = page["next_event_id"]
                if page["has_more"] and next_cursor <= cursor:
                    raise ProtocolError("nonprogressing READY page")
                new += self.db.apply_page(page, full_reconcile=full_reconcile)
                seen_events += [i["ready_event_id"] for i in page["items"]]
                server_time = page.get("server_time", server_time)
                total += len(page["items"])
                pages += 1
                if not page["has_more"]:
                    break
                if pages > 100000:
                    raise ProtocolError("unbounded READY pagination")
                cursor = next_cursor
            self.last_high = max(hello["last_ready_event_id"], high or 0)
            observed = DiscoveryObservation(total, new, pages, time.monotonic())
            self.db.observe("reconcile" if full_reconcile else "success", total, new, pages,
                            "server_time=%s;events=%s" % (server_time, ",".join(map(str, seen_events))))
            return observed


class BService:
    def __init__(self, cfg: BConfig, transport: Transport | None = None):
        self.cfg = cfg
        self.transport = transport if transport is not None else SSHTransport(cfg)
        self.root = cfg.root
        self.incoming = self.root / "incoming"
        self.restoring = self.root / "restoring"
        self.output = self.root / "output"
        self.state = self.root / "state"
        self.receipts = self.state / "receipts" / cfg.producer_id
        for folder in (self.root, self.incoming, self.restoring, self.output, self.state, self.receipts, self.root / "quarantine"):
            folder.mkdir(parents=True, exist_ok=True)
            no_symlink_dir(folder)
        if os.stat(self.restoring).st_dev != os.stat(self.output).st_dev:
            raise ProtocolError("restoring and output must share a filesystem")
        self.db = BStore(self.state / "aas_b.sqlite3", cfg.consumer_id)
        self.discovery = DiscoveryCoordinator(cfg.producer_id, self.db, self.transport)
        self.policy = FixedIntervalPoll(cfg.interval_seconds)
        self.retry_policy = RetryPolicy(cfg.retry_initial_seconds, cfg.retry_max_seconds, cfg.retry_jitter)

    def _receipt_paths(self, batch_id: str):
        bid = validate_id(batch_id)
        return self.receipts / (bid + ".PREPARED.json"), self.receipts / (bid + ".json")

    def _target(self, batch_id: str):
        prod_dir = self.output / self.cfg.producer_id
        prod_dir.mkdir(exist_ok=True)
        no_symlink_dir(prod_dir)
        return prod_dir / validate_id(batch_id)

    def _file_for(self, batch_id: str, suffix: str) -> Path:
        return self.incoming / (validate_id(batch_id) + suffix)

    def _restore_dir(self, batch_id: str) -> Path:
        return self.restoring / validate_id(batch_id)

    def recover(self):
        """DELIVERED is permanent history; do not inspect user output to recover it."""
        self.db.recover_running()
        # Reconstruct derived SQLite index if it was lost while receipts survived.
        for receipt_path in self.receipts.glob("*.json"):
            if receipt_path.name.endswith(".PREPARED.json") or receipt_path.is_symlink():
                continue
            receipt = read_json(receipt_path)
            if receipt.get("receipt_status") != "DELIVERED":
                continue
            summary = receipt.get("summary")
            if not isinstance(summary, dict) or summary.get("producer_id") != self.cfg.producer_id:
                raise IntegrityError("invalid producer in retained receipt")
            bid = validate_id(summary["batch_id"])
            if receipt_path.name != bid + ".json":
                raise IntegrityError("receipt filename/identity mismatch")
            self.db.import_delivered(summary)

    def _validate_manifest(self, record: dict, manifest: dict):
        if manifest.get("schema_version") != 1 or manifest.get("producer_id") != self.cfg.producer_id or manifest.get("batch_id") != record["batch_id"] or manifest.get("seq") != record["seq"]:
            raise IntegrityError("manifest batch identity mismatch")
        if hash_obj(manifest) != record["manifest_sha256"]:
            raise IntegrityError("immutable manifest hash mismatch")
        for field in ("payload_sha256", "payload_bytes", "files_index_sha256", "file_count", "source_bytes"):
            if manifest.get(field) != record[field]:
                raise IntegrityError(f"manifest {field} differs from READY listing")
        if manifest.get("archive") != "tar" or manifest.get("crypto", {}).get("scheme") != self.cfg.crypto_scheme:
            raise ProtocolError("archive/crypto mismatch")
        if manifest["file_count"] != len(manifest.get("files", [])) or hash_obj(manifest["files"]) != manifest["files_index_sha256"] or sum(x["size"] for x in manifest["files"]) != manifest["source_bytes"]:
            raise IntegrityError("manifest files index invalid")
        if record["payload_bytes"] < 1 or record["source_bytes"] < 1:
            raise IntegrityError("negative/empty archive")

    def _download(self, record: dict) -> Path:
        path = self._file_for(record["batch_id"], ".bundle.part")
        expected = record["payload_bytes"]
        if available_bytes(self.incoming) - self.cfg.min_free_bytes < max(0, expected - (path.stat().st_size if path.exists() else 0)):
            raise CapacityError("insufficient space on B incoming volume")
        if path.is_symlink():
            raise IntegrityError("incoming part must not be a symlink")
        current = path.stat().st_size if path.exists() else 0
        if current > expected:
            raise IntegrityError("incoming part larger than immutable payload")
        with path.open("ab") as writer:
            while current < expected:
                size = min(self.cfg.chunk_bytes, expected - current)
                chunk = self.transport.get_range(record["batch_id"], current, size)
                if len(chunk) != size:
                    raise TemporaryError("remote returned truncated range")
                writer.write(chunk)
                writer.flush()
                os.fsync(writer.fileno())
                current += size
        digest, total = hash_file(path)
        if digest != record["payload_sha256"] or total != expected:
            path.unlink(missing_ok=True)  # private scratch; a retry must re-download, not re-hash the same bad bytes
            raise IntegrityError("downloaded final encrypted bundle hash mismatch")
        return path

    def _restore(self, record: dict, manifest: dict, payload: Path):
        if available_bytes(self.restoring) - self.cfg.min_free_bytes < record["source_bytes"]:
            raise CapacityError("insufficient B space for restored images")
        if available_bytes(self.incoming) - self.cfg.min_free_bytes < record["source_bytes"] + 1048576:
            raise CapacityError("insufficient B incoming space for decrypted temporary tar")
        bid = record["batch_id"]
        decrypted = self._file_for(bid, ".verified.tar.part")
        stage = self._restore_dir(bid)
        if stage.exists():
            remove_owned_tree(stage)  # private only, before PREPARED receipt
        decrypted.unlink(missing_ok=True)
        try:
            decrypt_with_identities(payload, decrypted, manifest["crypto"], self.cfg.identity_keys)
            plain_digest, _ = hash_file(decrypted)
            if plain_digest != manifest["plaintext_archive_sha256"]:
                raise IntegrityError("decrypted tar hash mismatch")
            verify_tar(decrypted, manifest["files"], destination=stage, max_total_bytes=record["source_bytes"])
        except BaseException:
            if stage.exists():
                remove_owned_tree(stage)
            raise
        finally:
            decrypted.unlink(missing_ok=True)

    def _make_prepared(self, record: dict, manifest: dict):
        bid = record["batch_id"]
        target = self._target(bid)
        if target.exists() or target.is_symlink():
            raise HandoffUncertain("output target already exists before prepared handoff; refusing overwrite")
        prepared, delivered = self._receipt_paths(bid)
        if delivered.exists() or prepared.exists():
            raise IntegrityError("receipt already exists; run receipt recovery")
        fields = ("producer_id", "batch_id", "seq", "payload_sha256", "payload_bytes", "manifest_sha256", "files_index_sha256", "file_count", "source_bytes", "epoch", "event_id")
        summary = {field: record[field] for field in fields}
        receipt = {"schema_version": 1, "receipt_status": "PREPARED", "summary": summary,
            "files": manifest["files"], "target_path": str(target), "prepared_at": now_utc()}
        write_json(prepared, receipt)
        return receipt

    def _verify_output_snapshot(self, target: Path, entries: list[dict]) -> bool:
        try:
            no_symlink_dir(target)
            if len(list(target.iterdir())) != len(entries):
                return False
            for record in entries:
                path = target / safe_image_name(record["name"])
                if path.is_symlink() or not path.is_file() or hash_file(path) != (record["sha256"], record["size"]):
                    return False
            return True
        except (OSError, IntegrityError):
            return False

    def _complete_delivered(self, prepared_receipt: dict, *, manual: bool = False, reason: str = ""):
        bid = validate_id(prepared_receipt["summary"]["batch_id"])
        prepared_path, delivered_path = self._receipt_paths(bid)
        receipt = {**prepared_receipt, "receipt_status": "DELIVERED", "delivered_at": now_utc(),
                   "confirmation": "MANUAL_ATTESTATION" if manual else "AUTOMATED_VERIFICATION"}
        if manual:
            receipt["operator_reason"] = reason
        write_json(delivered_path, receipt)
        prepared_path.unlink(missing_ok=True)
        fsync_dir(self.receipts)
        # State stays RUNNING for the owning worker: ACK_PENDING is claimable, which let a second
        # worker send a concurrent duplicate ACK. The DELIVERED receipt is the durable truth.
        return receipt

    def _handoff_or_recover(self, record: dict, receipt: dict):
        bid = record["batch_id"]
        target = self._target(bid)
        private = self._restore_dir(bid)
        _, delivered = self._receipt_paths(bid)
        if delivered.exists():
            return read_json(delivered)
        # Do NOT infer never-delivered from missing target; user can move it.
        if target.exists() or target.is_symlink():
            if self._verify_output_snapshot(target, receipt["files"]):
                return self._complete_delivered(receipt)
            raise HandoffUncertain("target exists but does not match PREPARED snapshot")
        if private.exists():
            if not self._verify_output_snapshot(private, receipt["files"]):
                raise HandoffUncertain("private PREPARED staging failed recovery validation")
            rename_new(private, target)
            return self._complete_delivered(receipt)
        raise HandoffUncertain("target and private staging absent after PREPARED; user may have moved output")

    def _ack(self, record: dict, receipt: dict):
        if receipt.get("receipt_status") != "DELIVERED":
            raise IntegrityError("ACK requires durable DELIVERED receipt")
        for field in ("producer_id", "batch_id", "epoch", "seq", "payload_sha256", "payload_bytes", "manifest_sha256", "files_index_sha256", "file_count", "source_bytes"):
            if receipt.get("summary", {}).get(field) != record[field]:
                raise IntegrityError(f"DELIVERED receipt mismatches transfer: {field}")
        request = {"producer_id": self.cfg.producer_id, "consumer_id": self.cfg.consumer_id,
            "batch_id": record["batch_id"], "seq": record["seq"], "payload_sha256": record["payload_sha256"],
            "file_count": record["file_count"], "committed_bytes": record["source_bytes"], "receipt_version": 1}
        answer = self.transport.ack(request)
        if answer.get("result") not in ("accepted", "already_acked", "already_deleted"):
            raise ProtocolError("A did not accept matching ACK")
        self.db.set_state(self.cfg.producer_id, record["batch_id"], "ACK_CONFIRMED")
        # Never inspect, delete or restore the user-owned output directory afterwards.
        self._file_for(record["batch_id"], ".bundle.part").unlink(missing_ok=True)

    def process_one(self, record: dict):
        bid = record["batch_id"]
        prepared, delivered = self._receipt_paths(bid)
        try:
            if delivered.exists():
                receipt = read_json(delivered)
                self._ack(record, receipt)
                return
            if prepared.exists():
                receipt = self._handoff_or_recover(record, read_json(prepared))
                self._ack(record, receipt)
                return
            if self._target(bid).exists():
                raise IntegrityError("existing output directory without internal receipt: manual conflict, do not overwrite")
            manifest = self.transport.info(bid)
            self._validate_manifest(record, manifest)
            payload = self._download(record)
            self._restore(record, manifest, payload)
            receipt = self._make_prepared(record, manifest)
            self._ack(record, self._handoff_or_recover(record, receipt))
        except HandoffUncertain as exc:
            self.db.set_state(self.cfg.producer_id, bid, "HANDOFF_UNCERTAIN", error=str(exc))
        except (AuthenticationError, ProtocolError, IntegrityError) as exc:
            self.db.set_state(self.cfg.producer_id, bid, "MANUAL_HOLD", error=str(exc))
        except (TemporaryError, OSError) as exc:
            # Keep ACK_PENDING if the historical DELIVERED receipt is durable.
            next_state = "ACK_PENDING" if delivered.exists() else "RETRY"
            self.db.set_state(self.cfg.producer_id, bid, next_state, error=str(exc), delay=min(300, 5 * 2 ** min(record["attempts"], 6)))
        except BaseException:
            # Never leave the queue permanently RUNNING after unexpected bug.
            self.db.set_state(self.cfg.producer_id, bid, "MANUAL_HOLD", error="unexpected worker exception; inspect logs")
            raise

    def scan_once(self, *, include_recheck: bool = True) -> DiscoveryObservation:
        obs = self.discovery.scan()
        if include_recheck and self.cfg.immediate_recheck and obs.newly_enqueued_batches > 0:
            self.discovery.scan()  # exactly one bounded catch-up
        return obs

    def reconcile(self) -> dict:
        """Independently audits full READY plus A's historical ledger; never changes DELIVERED."""
        observation = self.discovery.scan(full_reconcile=True)
        last_seq, audited, alerts = 0, 0, 0
        for _ in range(100000):
            page = self.transport.ledger(last_seq)
            items = page.get("items")
            if not isinstance(items, list) or len(items) > 1000:
                raise ProtocolError("invalid A ledger page")
            for item in items:
                seq = item["seq"]
                if type(seq) is not int or seq <= last_seq:
                    raise ProtocolError("A ledger seq regressed/nonprogressing")
                last_seq = seq
                audited += 1
                bid = validate_id(item["batch_id"])
                state = item["state"]
                if state in ("ACKED", "DELETING", "DELETED"):
                    receipt = self._receipt_paths(bid)[1]
                    if not receipt.exists():
                        alerts += 1
                        self.db.audit(bid, "RECONCILE_MISSING_DELIVERED_RECEIPT", f"A ledger state={state}; no inferred B delivery")
                if state in ("VERIFY_RECOVERY_BLOCKED", "QUARANTINED"):
                    alerts += 1
                    self.db.audit(bid, "RECONCILE_A_OPERATOR_ACTION", f"A ledger state={state}")
            if len(items) < 1000:
                break
        else:
            raise ProtocolError("unbounded A ledger pagination")
        self.db.observe("ledger_audit", audited, 0, 0, f"alerts={alerts}")
        return {"ready_seen": observation.observed_ready_events, "ledger_audited": audited, "alerts": alerts}

    def once(self, *, max_jobs: int = 1000):
        self.recover()
        obs = self.scan_once()
        jobs = 0
        while jobs < max_jobs and (job := self.db.next_job()) is not None:
            self.process_one(job)
            jobs += 1
        return {"observed": obs.observed_ready_events, "new": obs.newly_enqueued_batches, "jobs": jobs}

    def _wake_loop(self, stop: threading.Event, wake: threading.Event):
        """Optional accelerator: A tells us when a new READY event exists. Pure hint - the normal
        paginated scan (single flight, cursor+queue in one transaction) is still what discovers batches."""
        backoff = RetryPolicy(self.cfg.retry_initial_seconds, self.cfg.retry_max_seconds, self.cfg.retry_jitter)
        while not stop.is_set():
            high = self.discovery.last_high
            if high is None:
                stop.wait(0.5)
                continue
            try:
                answer = self.transport.wait_ready(high, self.cfg.wake_timeout_seconds)
                backoff.reset()
            except (TemporaryError, OSError):
                stop.wait(backoff.next_delay())
                continue
            except AASError:
                stop.wait(30.0)  # auth/protocol problems are surfaced by the main loop's scans
                continue
            if answer.get("changed"):
                wake.set()
                deadline = time.monotonic() + 5.0  # let the scan advance `last_high`; never hot-loop
                while not stop.is_set() and self.discovery.last_high == high and time.monotonic() < deadline:
                    stop.wait(0.1)

    def run(self, stop_event: threading.Event | None = None):
        stop = stop_event if stop_event is not None else threading.Event()
        with process_lock(self.state / "b_instance.lock"):
            self.recover()
            next_due = time.monotonic()  # initial scan immediately
            next_reconcile = time.monotonic() + self.cfg.reconcile_seconds
            futures = set()
            wake = threading.Event()
            if self.cfg.wake_mode == "long_poll":
                threading.Thread(target=self._wake_loop, args=(stop, wake), daemon=True, name="aas-wake").start()
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.cfg.max_workers) as pool:
                while not stop.is_set():
                    completed = {f for f in futures if f.done()}
                    for future in completed:
                        future.result()  # propagate unexpected worker bugs, do not lose exceptions
                    futures.difference_update(completed)
                    now = time.monotonic()
                    if now >= next_due or wake.is_set():
                        wake.clear()
                        try:
                            obs = self.scan_once()
                        except AuthenticationError as exc:
                            self.db.observe("auth_failure", 0, 0, 0, str(exc))
                            raise
                        except ProtocolError as exc:
                            self.db.observe("protocol_failure", 0, 0, 0, str(exc))
                            raise
                        except (TemporaryError, OSError) as exc:
                            self.db.observe("network_failure", 0, 0, 0, str(exc))
                            next_due = time.monotonic() + self.retry_policy.next_delay()
                        else:
                            self.retry_policy.reset()
                            self.policy.observe(obs)
                            next_due = time.monotonic() + self.policy.next_delay_seconds(PollContext(time.monotonic(), bool(futures), stop.is_set()))
                    if time.monotonic() >= next_reconcile:
                        try:
                            self.reconcile()
                        except (TemporaryError, OSError) as exc:
                            self.db.observe("reconcile_failure", 0, 0, 0, str(exc))
                        next_reconcile = time.monotonic() + self.cfg.reconcile_seconds
                    while len(futures) < self.cfg.max_workers:
                        job = self.db.next_job()
                        if job is None:
                            break
                        futures.add(pool.submit(self.process_one, job))
                    wait = max(0.05, min(0.5, next_due - time.monotonic(), next_reconcile - time.monotonic()))
                    wake.wait(wait)

    def retry_held(self, batch_id: str, *, reason: str):
        """Operator must fix root cause first. Never delete existing user output."""
        bid = validate_id(batch_id)
        if len(reason.strip()) < 8:
            raise ProtocolError("manual retry requires a descriptive reason")
        if self.db.state(self.cfg.producer_id, bid) != "MANUAL_HOLD":
            raise ProtocolError("only MANUAL_HOLD can be retried")
        self.db.audit(bid, "OPERATOR_RETRY", reason)
        self.db.set_state(self.cfg.producer_id, bid, "DISCOVERED")

    def resolve_uncertain(self, batch_id: str, *, action: str, reason: str):
        if len(reason.strip()) < 8:
            raise ProtocolError("manual resolution needs a descriptive reason (>=8 chars)")
        record = self.db.get(self.cfg.producer_id, validate_id(batch_id))
        if record["state"] != "HANDOFF_UNCERTAIN":
            raise ProtocolError("manual resolution only applies to HANDOFF_UNCERTAIN")
        prepared, delivered = self._receipt_paths(batch_id)
        if not prepared.exists() or delivered.exists():
            raise IntegrityError("manual resolution requires an actual PREPARED receipt")
        self.db.audit(batch_id, "OPERATOR_DECISION", f"{action}: {reason}")
        if action == "confirm-delivered":
            # Explicit HUMAN claim, not an automated proof of the current output tree.
            self._complete_delivered(read_json(prepared), manual=True, reason=reason)
            self.db.set_state(self.cfg.producer_id, batch_id, "ACK_PENDING")  # let the daemon send the ACK
        elif action == "hold":
            self.db.set_state(self.cfg.producer_id, batch_id, "MANUAL_HOLD", error="operator hold: " + reason)
        else:
            raise ProtocolError("unknown manual resolution action")
