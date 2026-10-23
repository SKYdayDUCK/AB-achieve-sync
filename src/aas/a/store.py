"""A's authoritative SQLite ledger. state transitions use BEGIN IMMEDIATE."""
from __future__ import annotations
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import uuid
from aas.common.core import IntegrityError, ProtocolError, hash_obj, now_utc


class AStore:
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self.init()

    @contextmanager
    def tx(self):
        con = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA busy_timeout=30000")
        con.execute("PRAGMA synchronous=FULL")
        try:
            con.execute("BEGIN IMMEDIATE")
            yield con
            con.commit()
        except BaseException:
            con.rollback()
            raise
        finally:
            con.close()

    def init(self):
        setup = sqlite3.connect(self.path)
        try:
            setup.execute("PRAGMA journal_mode=WAL")
            setup.execute("PRAGMA synchronous=FULL")
        finally:
            setup.close()  # sqlite3's context manager only commits; it never closes
        with self.tx() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS batches (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    snapshot_json TEXT,
                    snapshot_sha256 TEXT,
                    files_index_sha256 TEXT,
                    manifest_sha256 TEXT,
                    payload_sha256 TEXT,
                    payload_bytes INTEGER,
                    plaintext_sha256 TEXT,
                    file_count INTEGER,
                    source_bytes INTEGER,
                    ready_event_id INTEGER,
                    ready_at TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS ready_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL,
                    ready_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS acknowledgements (
                    batch_id TEXT PRIMARY KEY,
                    consumer_id TEXT NOT NULL,
                    ack_json TEXT NOT NULL,
                    accepted_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, occurred_at TEXT NOT NULL,
                    batch_id TEXT, action TEXT NOT NULL, detail TEXT NOT NULL);
            """)
            c.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('protocol_epoch',?)", (uuid.uuid4().hex,))

    def max_event_id(self) -> int:
        """Cheap read-only high-water mark of READY events (no write lock; used by wait-ready)."""
        con = sqlite3.connect(self.path, timeout=30)
        try:
            return int(con.execute("SELECT COALESCE(MAX(event_id),0) FROM ready_events").fetchone()[0])
        finally:
            con.close()

    def recent_claimed_mtimes_ns(self, batches: int = 6) -> list[int]:
        """mtime_ns of files in the latest batches (taken from the durable claim snapshot)."""
        con = sqlite3.connect(self.path, timeout=30)
        try:
            rows = con.execute("SELECT snapshot_json FROM batches WHERE snapshot_json IS NOT NULL ORDER BY seq DESC LIMIT ?", (batches,)).fetchall()
        finally:
            con.close()
        return [int(item["mtime_ns"]) for (blob,) in rows for item in json.loads(blob)]

    def epoch(self) -> str:
        with self.tx() as c:
            return c.execute("SELECT value FROM meta WHERE key='protocol_epoch'").fetchone()[0]

    def create(self, batch_id: str):
        with self.tx() as c:
            c.execute("INSERT INTO batches(batch_id,state,created_at,updated_at) VALUES(?,?,?,?)", (batch_id, "CLAIMING", now_utc(), now_utc()))
            return c.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()

    def get(self, batch_id: str):
        with self.tx() as c:
            row = c.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        if row is None:
            raise ProtocolError("unknown batch")
        return dict(row)

    def rows(self, *states: str):
        if not states:
            return []
        with self.tx() as c:
            query = "SELECT * FROM batches WHERE state IN (" + ",".join("?" * len(states)) + ") ORDER BY seq"
            return [dict(r) for r in c.execute(query, states)]

    def update(self, batch_id: str, prior: str, state: str, **fields):
        with self.tx() as c:
            keys = ["state", "updated_at", *fields]
            values = [state, now_utc(), *fields.values()]
            sql = "UPDATE batches SET " + ",".join(f"{k}=?" for k in keys) + " WHERE batch_id=? AND state=?"
            cur = c.execute(sql, (*values, batch_id, prior))
            if cur.rowcount != 1:
                raise IntegrityError(f"CAS failed {batch_id}: {prior}->{state}")
            c.execute("INSERT INTO audit_log(occurred_at,batch_id,action,detail) VALUES(?,?,?,?)", (now_utc(), batch_id, f"{prior}->{state}", ""))

    def mark_ready(self, batch_id: str, prior: str = "SOURCE_CLEANING") -> int:
        with self.tx() as c:
            row = c.execute("SELECT state FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if row is None or row["state"] != prior:
                raise IntegrityError("cannot mark READY from current state")
            at = now_utc()
            event = c.execute("INSERT INTO ready_events(batch_id,ready_at) VALUES(?,?)", (batch_id, at)).lastrowid
            c.execute("UPDATE batches SET state='READY',ready_event_id=?,ready_at=?,updated_at=? WHERE batch_id=?", (event, at, at, batch_id))
            return event

    def reannounce_ready(self, batch_id: str) -> int:
        """Admin-only reannounce an unchanged READY bundle; seq stays constant."""
        with self.tx() as c:
            row = c.execute("SELECT state FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if row is None or row[0] != "READY":
                raise IntegrityError("reannounce requires READY")
            at = now_utc()
            event = c.execute("INSERT INTO ready_events(batch_id,ready_at) VALUES(?,?)", (batch_id,at)).lastrowid
            c.execute("UPDATE batches SET ready_event_id=?,ready_at=?,updated_at=? WHERE batch_id=?", (event,at,at,batch_id))
            c.execute("INSERT INTO audit_log(occurred_at,batch_id,action,detail) VALUES(?,?,?,?)",(at,batch_id,"REANNOUNCE_READY","immutable bundle; new event ID"))
            return event

    def list_ready(self, *, after_event_id: int = 0, page_size: int = 100, snapshot_high: int | None = None) -> dict:
        if not isinstance(after_event_id, int) or after_event_id < 0 or not 1 <= page_size <= 1000:
            raise ProtocolError("invalid pagination parameters")
        with self.tx() as c:
            actual_high = c.execute("SELECT COALESCE(MAX(event_id),0) FROM ready_events").fetchone()[0]
            if snapshot_high is None:
                snapshot_high = actual_high
            if not isinstance(snapshot_high, int) or snapshot_high < 0 or snapshot_high > actual_high or after_event_id > snapshot_high:
                raise ProtocolError("invalid snapshot high-water mark")
            rows = [dict(r) for r in c.execute("""
                SELECT batch_id,seq,ready_event_id,payload_sha256,payload_bytes,
                file_count,source_bytes,manifest_sha256,files_index_sha256,ready_at
                FROM batches WHERE state='READY' AND ready_event_id>? AND ready_event_id<=?
                ORDER BY ready_event_id,batch_id LIMIT ?
            """, (after_event_id, snapshot_high, page_size + 1))]
            has_more = len(rows) > page_size
            items = rows[:page_size]
            next_event_id = items[-1]["ready_event_id"] if has_more else snapshot_high
            return {"producer_id": None, "protocol_epoch": self.epoch_in_tx(c), "snapshot_high": snapshot_high, "items": items, "next_event_id": next_event_id, "has_more": has_more}

    @staticmethod
    def epoch_in_tx(c):
        return c.execute("SELECT value FROM meta WHERE key='protocol_epoch'").fetchone()[0]

    def accept_ack(self, request: dict, *, producer_id: str, consumer_id: str) -> str:
        required = ("producer_id", "consumer_id", "batch_id", "seq", "payload_sha256", "file_count", "committed_bytes", "receipt_version")
        if not isinstance(request, dict) or any(k not in request for k in required) or request["producer_id"] != producer_id or request["consumer_id"] != consumer_id or request["receipt_version"] != 1:
            raise ProtocolError("invalid ACK fields or unauthorized consumer")
        with self.tx() as c:
            row = c.execute("SELECT * FROM batches WHERE batch_id=?", (request["batch_id"],)).fetchone()
            if row is None or row["state"] not in ("READY", "ACKED", "DELETING", "DELETED"):
                raise ProtocolError("ACK for unknown/unavailable batch")
            for field, expected in (("seq", row["seq"]), ("payload_sha256", row["payload_sha256"]), ("file_count", row["file_count"]), ("committed_bytes", row["source_bytes"])):
                if type(request[field]) is not type(expected) or request[field] != expected:
                    raise ProtocolError(f"ACK {field} mismatch")
            old = c.execute("SELECT ack_json FROM acknowledgements WHERE batch_id=?", (request["batch_id"],)).fetchone()
            if old is not None:
                if json.loads(old[0]) != request:
                    raise ProtocolError("replayed ACK differs from recorded ACK")
                return "already_deleted" if row["state"] == "DELETED" else "already_acked"
            if row["state"] != "READY":
                raise IntegrityError("no ACK tombstone for state")
            c.execute("INSERT INTO acknowledgements(batch_id,consumer_id,ack_json,accepted_at) VALUES(?,?,?,?)", (request["batch_id"], consumer_id, json.dumps(request, sort_keys=True), now_utc()))
            c.execute("UPDATE batches SET state='ACKED',updated_at=? WHERE batch_id=? AND state='READY'", (now_utc(), request["batch_id"]))
            c.execute("INSERT INTO audit_log(occurred_at,batch_id,action,detail) VALUES(?,?,?,?)", (now_utc(), request["batch_id"], "ACK_ACCEPTED", consumer_id))
            return "accepted"

    def is_acked(self, batch_id: str) -> bool:
        with self.tx() as c:
            return c.execute("SELECT 1 FROM acknowledgements WHERE batch_id=?", (batch_id,)).fetchone() is not None

    def audit(self, batch_id: str | None, action: str, detail: str):
        with self.tx() as c:
            c.execute("INSERT INTO audit_log(occurred_at,batch_id,action,detail) VALUES(?,?,?,?)", (now_utc(), batch_id, action, detail[:600]))
