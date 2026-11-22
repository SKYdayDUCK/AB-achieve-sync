"""Short SQLite transactions for manifest/range resource leases and ACK journals."""
from contextlib import closing, contextmanager
from pathlib import Path
import sqlite3
import time

from aas.common.core import TemporaryError, process_lock


class ResourceStore:
    def __init__(self, root: Path):
        root.mkdir(parents=True, exist_ok=True)
        self.path = root / 'resources.sqlite3'
        deadline = time.monotonic() + 30
        while True:
            try:
                with process_lock(root / 'schema.lock'):
                    self._initialize()
                break
            except TemporaryError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.02)

    def _initialize(self):
        with closing(self.connect()) as db:
            db.executescript('''
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS resources (
                    resource_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, resource_key TEXT NOT NULL,
                    directory TEXT NOT NULL, snapshot TEXT NOT NULL, state TEXT NOT NULL,
                    total_bytes INTEGER NOT NULL, file_count INTEGER NOT NULL,
                    manifest TEXT, consumer_id TEXT, request_id TEXT, lease_token TEXT,
                    lease_expires REAL, reason TEXT, created REAL NOT NULL);
                DROP INDEX IF EXISTS one_active_source_key;
                CREATE INDEX IF NOT EXISTS resources_source_key
                    ON resources(source_id,resource_key);
                CREATE INDEX IF NOT EXISTS resources_source_directory
                    ON resources(source_id,directory);
                CREATE TABLE IF NOT EXISTS ack_journal (
                    resource_id TEXT PRIMARY KEY, consumer_id TEXT NOT NULL, lease_token TEXT NOT NULL,
                    receipt TEXT NOT NULL, done TEXT NOT NULL DEFAULT '[]', complete INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS early_cleanup (
                    resource_id TEXT NOT NULL, name TEXT NOT NULL, signature TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'PENDING', reason TEXT,
                    PRIMARY KEY(resource_id,name));
                CREATE TABLE IF NOT EXISTS cleanup_epochs (
                    epoch_id TEXT PRIMARY KEY, resource_id TEXT NOT NULL);
            ''')

    def connect(self):
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA busy_timeout=30000')
        db.execute('PRAGMA synchronous=FULL')
        return db

    @contextmanager
    def transaction(self):
        db = self.connect()
        try:
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def one(self, resource_id):
        with closing(self.connect()) as db:
            row = db.execute('SELECT * FROM resources WHERE resource_id=?', (resource_id,)).fetchone()
        return dict(row) if row else None

    def rows(self):
        with closing(self.connect()) as db:
            return [dict(row) for row in db.execute('SELECT * FROM resources ORDER BY created,resource_id')]
