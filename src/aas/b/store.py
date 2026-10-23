"""B transactional durable discovery queue + epoch-scoped incremental cursor."""
from __future__ import annotations
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import time
from aas.common.core import IntegrityError, ProtocolError, now_utc, validate_id


TERMINAL_STATES = frozenset(("ACK_CONFIRMED", "HANDOFF_UNCERTAIN", "MANUAL_HOLD"))


class BStore:
    def __init__(self, path: Path, consumer_id: str):
        self.path = path
        self.consumer_id = validate_id(consumer_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.init()

    @contextmanager
    def tx(self):
        c = sqlite3.connect(self.path, isolation_level=None, timeout=30)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA synchronous=FULL")
        c.execute("PRAGMA busy_timeout=30000")
        try:
            c.execute("BEGIN IMMEDIATE")
            yield c
            c.commit()
        except BaseException:
            c.rollback()
            raise
        finally:
            c.close()

    def init(self):
        setup = sqlite3.connect(self.path)
        try:
            setup.execute("PRAGMA journal_mode=WAL")
            setup.execute("PRAGMA synchronous=FULL")
        finally:
            setup.close()  # sqlite3's context manager only commits; it never closes
        with self.tx() as c:
            c.execute("CREATE TABLE IF NOT EXISTS producer_epoch (producer_id TEXT PRIMARY KEY, epoch TEXT NOT NULL)")
            c.execute("""CREATE TABLE IF NOT EXISTS cursor (
                producer_id TEXT NOT NULL,consumer_id TEXT NOT NULL,epoch TEXT NOT NULL,
                last_event_id INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(producer_id,consumer_id,epoch))""")
            c.execute("""CREATE TABLE IF NOT EXISTS transfers (
                producer_id TEXT NOT NULL,batch_id TEXT NOT NULL,epoch TEXT NOT NULL,
                seq INTEGER NOT NULL,event_id INTEGER NOT NULL,payload_sha256 TEXT NOT NULL,
                payload_bytes INTEGER NOT NULL,manifest_sha256 TEXT NOT NULL,
                files_index_sha256 TEXT NOT NULL,file_count INTEGER NOT NULL,source_bytes INTEGER NOT NULL,
                state TEXT NOT NULL DEFAULT 'DISCOVERED',attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt REAL NOT NULL DEFAULT 0,last_error TEXT,updated_at TEXT NOT NULL,
                PRIMARY KEY(producer_id,batch_id))""")
            c.execute("CREATE INDEX IF NOT EXISTS ix_transfers_pending ON transfers(state,next_attempt)")
            c.execute("""CREATE TABLE IF NOT EXISTS observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,at TEXT NOT NULL,kind TEXT NOT NULL,
                observed INTEGER NOT NULL,new_batches INTEGER NOT NULL,pages INTEGER NOT NULL,detail TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,at TEXT NOT NULL,batch_id TEXT,
                action TEXT NOT NULL,detail TEXT NOT NULL)""")

    @staticmethod
    def _check_epoch(c, producer_id: str, epoch: str):
        record = c.execute("SELECT epoch FROM producer_epoch WHERE producer_id=?", (producer_id,)).fetchone()
        if record and record[0] != epoch:
            raise ProtocolError("A protocol_epoch changed: operator rebootstrap required; do not skip events")
        if record is None:
            c.execute("INSERT INTO producer_epoch VALUES(?,?)", (producer_id, epoch))

    def cursor(self, producer_id: str, epoch: str) -> int:
        with self.tx() as c:
            self._check_epoch(c, producer_id, epoch)
            record = c.execute("SELECT last_event_id FROM cursor WHERE producer_id=? AND consumer_id=? AND epoch=?", (producer_id, self.consumer_id, epoch)).fetchone()
            return int(record[0]) if record else 0

    def apply_page(self, response: dict, *, full_reconcile: bool = False) -> int:
        producer_id = validate_id(response["producer_id"])
        epoch = validate_id(response["protocol_epoch"])
        high = response["snapshot_high"]
        next_event = response["next_event_id"]
        items = response["items"]
        if type(high) is not int or type(next_event) is not int or high < 0 or not 0 <= next_event <= high or not isinstance(items, list):
            raise ProtocolError("invalid pagination response")
        new_count = 0
        last = -1
        with self.tx() as c:
            self._check_epoch(c, producer_id, epoch)
            for item in items:
                bid = validate_id(item["batch_id"])
                event = item["ready_event_id"]
                if type(event) is not int or not 0 <= event <= high or event <= last:
                    raise ProtocolError("unsorted READY events")
                last = event
                vals = (
                    producer_id, bid, epoch, item["seq"], event, item["payload_sha256"],
                    item["payload_bytes"], item["manifest_sha256"], item["files_index_sha256"],
                    item["file_count"], item["source_bytes"],
                )
                existing = c.execute("SELECT * FROM transfers WHERE producer_id=? AND batch_id=?", (producer_id, bid)).fetchone()
                if existing:
                    expected = ("epoch", "seq", "payload_sha256", "payload_bytes", "manifest_sha256", "files_index_sha256", "file_count", "source_bytes")
                    incoming = (epoch, item["seq"], item["payload_sha256"], item["payload_bytes"], item["manifest_sha256"], item["files_index_sha256"], item["file_count"], item["source_bytes"])
                    if any(existing[k] != incoming[i] for i, k in enumerate(expected)):
                        raise IntegrityError(f"immutable batch changed: {bid}")
                    c.execute("UPDATE transfers SET event_id=MAX(event_id,?),updated_at=? WHERE producer_id=? AND batch_id=?", (event, now_utc(), producer_id, bid))
                else:
                    c.execute("""INSERT INTO transfers(
                        producer_id,batch_id,epoch,seq,event_id,payload_sha256,payload_bytes,
                        manifest_sha256,files_index_sha256,file_count,source_bytes,state,updated_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,'DISCOVERED',?)""", (*vals, now_utc()))
                    new_count += 1
            if not full_reconcile:
                old = c.execute("SELECT last_event_id FROM cursor WHERE producer_id=? AND consumer_id=? AND epoch=?", (producer_id, self.consumer_id, epoch)).fetchone()
                prior = int(old[0]) if old else 0
                if high < prior or next_event < prior:
                    raise ProtocolError("READY event id regressed")
                c.execute("""INSERT INTO cursor(producer_id,consumer_id,epoch,last_event_id) VALUES(?,?,?,?)
                    ON CONFLICT(producer_id,consumer_id,epoch) DO UPDATE SET last_event_id=MAX(cursor.last_event_id,excluded.last_event_id)""",
                    (producer_id, self.consumer_id, epoch, next_event))
        return new_count

    def import_delivered(self, summary: dict):
        """Rebuild a lost B SQLite index from an independently durable receipt."""
        producer_id = validate_id(summary["producer_id"])
        bid = validate_id(summary["batch_id"])
        epoch = validate_id(summary["epoch"])
        keys = ("seq", "payload_sha256", "payload_bytes", "manifest_sha256", "files_index_sha256", "file_count", "source_bytes")
        with self.tx() as c:
            self._check_epoch(c, producer_id, epoch)
            old = c.execute("SELECT * FROM transfers WHERE producer_id=? AND batch_id=?", (producer_id, bid)).fetchone()
            if old is None:
                c.execute("""INSERT INTO transfers(producer_id,batch_id,epoch,seq,event_id,payload_sha256,
                    payload_bytes,manifest_sha256,files_index_sha256,file_count,source_bytes,state,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,'ACK_PENDING',?)""",
                    (producer_id, bid, epoch, summary["seq"], summary["event_id"], *(summary[k] for k in keys[1:]), now_utc()))
            else:
                for k in keys:
                    if old[k] != summary[k]:
                        raise IntegrityError("retained DELIVERED receipt conflicts with B queue")
                if old["epoch"] != epoch:
                    raise IntegrityError("retained receipt epoch changed")
                if summary["event_id"] > old["event_id"]:
                    c.execute("UPDATE transfers SET event_id=? WHERE producer_id=? AND batch_id=?", (summary["event_id"], producer_id, bid))
                if old["state"] not in ("ACK_CONFIRMED", "ACK_PENDING"):
                    c.execute("UPDATE transfers SET state='ACK_PENDING',updated_at=? WHERE producer_id=? AND batch_id=?", (now_utc(), producer_id, bid))

    def recover_running(self):
        with self.tx() as c:
            c.execute("UPDATE transfers SET state='DISCOVERED',updated_at=? WHERE state='RUNNING'", (now_utc(),))

    def next_job(self) -> dict | None:
        with self.tx() as c:
            row = c.execute("""SELECT * FROM transfers WHERE state IN ('DISCOVERED','RETRY','ACK_PENDING')
                AND next_attempt<=? ORDER BY CASE WHEN state='ACK_PENDING' THEN 0 ELSE 1 END,seq LIMIT 1""", (time.time(),)).fetchone()
            if row is None:
                return None
            c.execute("UPDATE transfers SET state='RUNNING',attempts=attempts+1,updated_at=? WHERE producer_id=? AND batch_id=?", (now_utc(), row["producer_id"], row["batch_id"]))
            return dict(row)

    def state(self, producer_id: str, batch_id: str) -> str:
        with self.tx() as c:
            row = c.execute("SELECT state FROM transfers WHERE producer_id=? AND batch_id=?", (producer_id, batch_id)).fetchone()
            if row is None:
                raise ProtocolError("unknown transfer")
            return row[0]

    def set_state(self, producer_id: str, batch_id: str, state: str, *, error: str | None = None, delay: float = 0):
        if state not in ("DISCOVERED", "RETRY", "ACK_PENDING", "ACK_CONFIRMED", "HANDOFF_UNCERTAIN", "MANUAL_HOLD"):
            raise ProtocolError("unrecognized transfer state")
        with self.tx() as c:
            current = c.execute("SELECT state FROM transfers WHERE producer_id=? AND batch_id=?", (producer_id, batch_id)).fetchone()
            if current is None or (current[0] == "ACK_CONFIRMED" and state != "ACK_CONFIRMED"):
                raise IntegrityError("attempted to regress finalized transfer")
            c.execute("UPDATE transfers SET state=?,last_error=?,next_attempt=?,updated_at=? WHERE producer_id=? AND batch_id=?", (state, error[:500] if error else None, time.time() + delay, now_utc(), producer_id, batch_id))
            c.execute("INSERT INTO audit_log(at,batch_id,action,detail) VALUES(?,?,?,?)", (now_utc(), batch_id, f"{current[0]}->{state}", error[:500] if error else ""))

    def get(self, producer_id: str, batch_id: str) -> dict:
        with self.tx() as c:
            row = c.execute("SELECT * FROM transfers WHERE producer_id=? AND batch_id=?", (producer_id, batch_id)).fetchone()
        if row is None:
            raise ProtocolError("unknown transfer")
        return dict(row)

    def rows(self):
        with self.tx() as c:
            return [dict(r) for r in c.execute("SELECT * FROM transfers ORDER BY seq")]

    def observe(self, kind: str, observed: int, new: int, pages: int, detail: str = ""):
        with self.tx() as c:
            c.execute("INSERT INTO observations(at,kind,observed,new_batches,pages,detail) VALUES(?,?,?,?,?,?)", (now_utc(), kind, observed, new, pages, detail[:300]))

    def audit(self, batch_id: str, action: str, detail: str):
        with self.tx() as c:
            c.execute("INSERT INTO audit_log(at,batch_id,action,detail) VALUES(?,?,?,?)", (now_utc(), batch_id, action, detail[:500]))
