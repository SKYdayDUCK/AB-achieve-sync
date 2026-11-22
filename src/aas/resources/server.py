"""Leased, persistent per-file resources. Never builds a tar or copies weights."""
from __future__ import annotations

from contextlib import closing, contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import stat
import time

from aas.common.core import AASError, TemporaryError, canonical, fsync_dir, hash_obj, process_lock, rename_new, validate_id
from .server_store import ResourceStore

MAX_RANGE = 16 * 1024 * 1024
EARLY_STEPS = {100, 200, 300, 400, 500}
TEMP = re.compile(r'(?:\.part|\.tmp|\.temp|\.lock|\.partial)(?:$|\.)', re.I)


class ResourceError(AASError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code

    def __reduce__(self):
        return type(self), (self.code, str(self))


def _json(value):
    return canonical(value).decode('utf-8')


def _name(name):
    if not isinstance(name, str) or not name or name in ('.', '..') or name[-1] in ('.', ' '):
        raise ResourceError('UNSAFE_PATH', 'Expected an ordinary basename')
    if any(c in '/\\:\x00' or ord(c) < 32 for c in name) or len(os.fsencode(name)) > 220:
        raise ResourceError('UNSAFE_PATH', 'Unsafe basename')
    return name


def _sig(info):
    return {'dev': info.st_dev, 'ino': info.st_ino, 'size': info.st_size,
            'mtime_ns': info.st_mtime_ns,
            # Windows lstat/fstat creation-time values can settle after the first
            # open; inode+mtime+size fence there, with full SHA checks before ACK.
            'ctime_ns': info.st_ctime_ns if os.name != 'nt' else 0, 'mode': info.st_mode}


def _ordinary(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or path.is_symlink():
        raise ResourceError('UNSAFE_PATH', 'Resource contains a nonregular file')
    return _sig(info)


def _config_number(value, name, *, positive=False):
    if type(value) not in (int, float):
        raise ValueError(name + ' must be a finite number')
    try:
        valid = math.isfinite(value) and (value > 0 if positive else value >= 0)
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError(name + ' must be finite and ' + ('positive' if positive else 'nonnegative'))
    return value


def _config_path(value, name):
    if not isinstance(value, (str, Path)) or not str(value).strip():
        raise ValueError(name + ' must be a nonempty filesystem path')
    return Path(value)


class ResourceServer:
    def __init__(self, config):
        base = Path.cwd()
        if isinstance(config, (str, Path)):
            import yaml
            path = Path(config).absolute()
            base = path.parent
            config = yaml.safe_load(path.read_text(encoding='utf-8'))
        self.config = config
        node = config['node']
        self.producer_id = validate_id(node['producer_id'])
        self.consumer_id = validate_id(node['consumer_id'])
        self.root = _config_path(config['paths']['root_dir'], 'paths.root_dir')
        if not self.root.is_absolute():
            self.root = base / self.root
        self.root = self.root.absolute()
        if self.root.resolve() != self.root:
            raise ResourceError('UNSAFE_PATH', 'State root must not contain symlinks')
        lease = config.get('lease', {})
        if not isinstance(lease, dict):
            raise ValueError('lease must be an object')
        self.lease_seconds = float(_config_number(lease.get('seconds', 1200), 'lease.seconds', positive=True))
        _config_number(config.get('scan_interval_seconds', 60), 'scan_interval_seconds', positive=True)
        self.sources = {}
        for raw in config.get('sources', []):
            source = dict(raw)
            sid = validate_id(source['id'])
            if sid in self.sources:
                raise ValueError('Duplicate source id')
            root = _config_path(source['root'], 'source.root')
            source['root'] = (root if root.is_absolute() else base / root).absolute()
            if source['root'].resolve() != source['root']:
                raise ResourceError('UNSAFE_PATH', 'Source root must not contain symlinks')
            mode = source.get('resource_mode')
            if mode not in (None, 'file', 'directory'):
                raise ValueError('resource_mode must be file or directory')
            pattern = source.get('subdir_glob', '*')
            if not isinstance(pattern, str) or not pattern:
                raise ValueError('subdir_glob must be a nonempty string')
            if mode and pattern != '.' and (any(part in ('', '.', '..') for part in pattern.split('/')) or '\\' in pattern or ':' in pattern):
                raise ValueError('subdir_glob must stay relative to the source root')
            if not mode and pattern != '.' and ('/' in pattern or '\\' in pattern or '..' in pattern):
                raise ValueError('subdir_glob must select immediate child directories')
            filenames = source.get('filenames')
            if filenames is not None:
                if not isinstance(filenames, list) or not filenames:
                    raise ValueError('filenames must be a nonempty list of exact basenames')
                source['filenames'] = frozenset(_name(name) for name in filenames)
            delete_after_ack = source.get('delete_after_ack', mode is None)
            if type(delete_after_ack) is not bool:
                raise ValueError('delete_after_ack must be a boolean')
            suffixes = source.get('suffixes', ['*'])
            if not isinstance(suffixes, list) or not suffixes or any(not isinstance(x, str) or (x != '*' and (not x.startswith('.') or '/' in x or '\\' in x)) for x in suffixes):
                raise ValueError('Expected explicit suffixes or wildcard')
            source['suffixes'] = [x.lower() for x in suffixes]
            if source.get('completion_marker'):
                _name(source['completion_marker'])
            _config_number(source.get('min_age_seconds', 10), 'min_age_seconds')
            _config_number(source.get('idle_flush_seconds', 300), 'idle_flush_seconds')
            for field in ('verify_images', 'include_final'):
                if field in source and type(source[field]) is not bool:
                    raise ValueError(field + ' must be a boolean')
            if type(source.get('hold_back_newest', 0)) is not int or source.get('hold_back_newest', 0) < 0:
                raise ValueError('hold_back_newest must be a nonnegative integer')
            if type(source.get('target_resource_bytes', 0)) is not int or source.get('target_resource_bytes', 0) < 0:
                raise ValueError('target_resource_bytes must be a nonnegative exact integer')
            if 'checkpoint_min_step' in source:
                if mode:
                    raise ValueError('resource_mode is for files and directories without LoRA checkpoint grouping')
                for field, default in (('checkpoint_min_step', 600), ('checkpoint_interval', 100), ('max_train_steps', 1500)):
                    value = source.get(field, default)
                    if type(value) is not int or value < (0 if field == 'checkpoint_min_step' else 1):
                        raise ValueError(field + ' must be an exact integer in range')
                if source.get('max_train_steps', 1500) < source['checkpoint_min_step']:
                    raise ValueError('max_train_steps is below checkpoint_min_step')
            cleanup = source.get('cleanup', {})
            if not isinstance(cleanup, dict) or type(cleanup.get('enabled', False)) is not bool:
                raise ValueError('cleanup must be an object and cleanup.enabled must be a boolean')
            steps = cleanup.get('checkpoint_steps', [])
            if not isinstance(steps, list) or any(type(x) is not int or x not in EARLY_STEPS for x in steps) or len(steps) != len(set(steps)):
                raise ValueError('checkpoint_steps must be distinct exact integers100..500')
            if cleanup.get('enabled', False):
                if not steps:
                    raise ValueError('Cleanup supports only explicit steps100..500')
                if not source.get('completion_marker') or 'checkpoint_min_step' not in source or source['suffixes'] != ['.safetensors']:
                    raise ValueError('Checkpoint cleanup needs marker, checkpoint rule and .safetensors only')
                if source['checkpoint_min_step'] <= max(steps):
                    raise ValueError('Cleanup steps must be below the published checkpoint range')
            for other in self.sources.values():
                if mode or other.get('resource_mode'):
                    other_root = other['root']
                    if other_root.is_relative_to(source['root']) or source['root'].is_relative_to(other_root):
                        raise ValueError('Mapped source roots must not overlap')
            self.sources[sid] = source
        self.store = ResourceStore(self.root)
        self._clock = time.time

    def _identity(self, consumer_id):
        consumer_id = self.consumer_id if consumer_id is None else consumer_id
        if consumer_id != self.consumer_id:
            raise ResourceError('AUTH', 'Consumer is not bound to this producer')
        return consumer_id

    def _directory(self, source, directory):
        root = source['root']
        if root.resolve() != root or root.is_symlink() or directory.resolve() != directory or directory.is_symlink():
            raise ResourceError('UNSAFE_PATH', 'Symlink resource directory')
        directory.relative_to(root)
        if directory == self.root or self.root in directory.parents:
            raise ResourceError('UNSAFE_PATH', 'Resource directory overlaps private server state')
        if not directory.is_dir():
            raise ResourceError('SOURCE_CHANGED', 'Resource directory disappeared')
        return directory

    def _path(self, row, name):
        _name(name)
        source = self.sources[row['source_id']]
        directory = self._directory(source, Path(row['directory']))
        path = directory / name
        if path.resolve(strict=False) != path or path.is_symlink():
            raise ResourceError('UNSAFE_PATH', 'Symlink resource file')
        return path

    def _checkpoint(self, name, key):
        match = re.fullmatch(r'(.+)-step(\d+)\.safetensors', name)
        if match and (match[1] == key or match[1].startswith(key + '_') or match[1].startswith(key + '-')):
            return match[1], int(match[2])
        return None

    def _snapshot(self, source, directory):
        self._directory(source, directory)
        key = _name(directory.name)
        marker = source.get('completion_marker')
        marker_sig, metadata = None, {}
        if marker:
            path = directory / marker
            if not path.exists():
                return None
            marker_sig = _ordinary(path)
            if marker_sig['size'] > 1024 * 1024:
                return None
            if self._clock() - path.stat().st_mtime < source.get('min_age_seconds', 10):
                return None
            try:
                with path.open('rb') as stream:
                    raw = stream.read(1024 * 1024 + 1)
                if len(raw) > 1024 * 1024:
                    return None
                metadata = json.loads(raw)
                if not isinstance(metadata, dict):
                    return None
            except (OSError, ValueError):
                return None
            if _ordinary(path) != marker_sig:
                return None
        candidates = []
        individual = source.get('resource_mode') == 'file'
        for path in sorted(directory.iterdir()):
            name = path.name
            if name.startswith('.') or name == marker or TEMP.search(name):
                continue
            if '*' not in source['suffixes'] and path.suffix.lower() not in source['suffixes']:
                continue
            if 'filenames' in source and name not in source['filenames']:
                continue
            if 'checkpoint_min_step' in source and not (name.startswith(key + '_') or name.startswith(key + '-') or name == key + '.safetensors'):
                continue
            if path.is_dir() and not path.is_symlink():
                continue
            try:
                _name(name)
                signature = _ordinary(path)
            except (ResourceError, OSError):
                if individual:
                    continue
                raise
            if individual and self._clock() - signature['mtime_ns'] / 1e9 < source.get('min_age_seconds', 10):
                continue
            candidates.append((path, signature))
        cleanup = []
        if 'checkpoint_min_step' in source:
            minimum = int(source['checkpoint_min_step'])
            interval = int(source.get('checkpoint_interval', 100))
            max_step = metadata.get('max_train_steps', source.get('max_train_steps', 1500))
            checkpoints = metadata.get('checkpoints', [])
            found_steps = []
            for entry in checkpoints if isinstance(checkpoints, list) else []:
                if type(entry) is int:
                    found_steps.append(entry)
                elif isinstance(entry, str):
                    found = self._checkpoint(Path(entry).name, key)
                    if found:
                        found_steps.append(found[1])
                elif isinstance(entry, dict):
                    if type(entry.get('step')) is int:
                        found_steps.append(entry['step'])
            if 'max_train_steps' not in metadata and found_steps:
                max_step = max(found_steps)
            if type(max_step) is not int or max_step < minimum or interval <= 0:
                return None
            groups = {}
            for path, signature in candidates:
                item = self._checkpoint(path.name, key)
                if item:
                    groups.setdefault(item[0], {})[item[1]] = (path, signature)
            needed = set(range(minimum, max_step + 1, interval))
            finals = {path.name: (path, signature) for path, signature in candidates}
            valid = [(prefix, values) for prefix, values in groups.items() if needed <= values.keys()
                     and (not source.get('include_final', True) or prefix + '.safetensors' in finals)]
            if len(valid) != 1:
                return None
            prefix, values = valid[0]
            chosen = [values[step] for step in sorted(needed)]
            if source.get('include_final', True):
                chosen.append(finals[prefix + '.safetensors'])
            if source.get('cleanup', {}).get('enabled') and marker_sig:
                cleanup = [values[step] for step in source['cleanup']['checkpoint_steps'] if step in values]
            candidates = chosen
        elif source.get('hold_back_newest', 0) and candidates:
            newest = max(signature['mtime_ns'] / 1e9 for _, signature in candidates)
            if self._clock() - newest < float(source.get('idle_flush_seconds', 300)):
                count = int(source['hold_back_newest'])
                candidates = sorted(candidates, key=lambda pair:(pair[1]['mtime_ns'], pair[0].name))[:-count] if count > 0 else candidates
        if not candidates or (not individual and any(self._clock() - signature['mtime_ns'] / 1e9 < source.get('min_age_seconds', 10) for _, signature in candidates)):
            return None
        snapshot = {'files': [{'name': path.name, 'stat': signature} for path, signature in sorted(candidates)],
                    'marker': {'name': marker, 'stat': marker_sig} if marker else None}
        if source.get('resource_mode') or 'delete_after_ack' in source:
            snapshot['delete_after_ack'] = source.get('delete_after_ack', not source.get('resource_mode'))
        return snapshot, cleanup

    def _hold(self, resource_id, reason):
        with self.store.transaction() as db:
            db.execute("UPDATE resources SET state='HOLD',reason=? WHERE resource_id=? AND state!='ACKED'", (reason, resource_id))

    @contextmanager
    def _resource_lock(self, resource_id, wait=False):
        if not isinstance(resource_id, str) or not re.fullmatch('[0-9a-f]{64}', resource_id):
            raise ResourceError('BAD_REQUEST', 'Invalid resource id')
        deadline = time.monotonic() + self.lease_seconds
        while True:
            try:
                with process_lock(self.root / 'locks' / (resource_id + '.lock')):
                    yield
                    return
            except TemporaryError as exc:
                if not wait or time.monotonic() >= deadline:
                    raise ResourceError('BUSY', 'Resource operation already running') from exc
                time.sleep(0.05)

    def scan(self):
        summary = {'ready': 0, 'held': 0, 'cleaned': 0, 'cleanup_issues': 0, 'missing_sources': [], 'incomplete': 0}
        self._recover_pending()
        recovered, issues = self._recover_early_cleanup()
        summary['cleaned'] += recovered
        summary['cleanup_issues'] += issues
        for sid, source in self.sources.items():
            root = source['root']
            if not root.exists():
                summary['missing_sources'].append(sid)
                continue
            directories = [root] if source.get('subdir_glob', '*') == '.' else sorted(root.glob(source.get('subdir_glob', '*')))
            for directory in directories:
                parts = directory.relative_to(root).parts
                if directory.name.startswith('.') or any(part.startswith('.') for part in parts) or not directory.is_dir() or directory.is_symlink():
                    continue
                try:
                    if source.get('resource_mode'):
                        for part in parts:
                            _name(part)
                        key = directory.relative_to(root).as_posix()
                    else:
                        key = directory.name
                    if source.get('target_resource_bytes', 0) or source.get('resource_mode') == 'file':
                        self._scan_sharded(sid, source, directory, key, summary)
                        continue
                    result = self._snapshot(source, directory)
                    if not result:
                        with self.store.transaction() as db:
                            old = db.execute("SELECT * FROM resources WHERE source_id=? AND resource_key=? AND state NOT IN ('ACKED','ACK_PENDING','HOLD','SUPERSEDED')", (sid,key)).fetchone()
                        marker_missing = source.get('completion_marker') and not (directory / source['completion_marker']).exists()
                        if old and (not self._intact(dict(old)) or marker_missing):
                            self._hold(old['resource_id'], 'Source files changed')
                            summary['held'] += 1
                        summary['incomplete'] += 1
                        continue
                    snapshot, cleanup = result
                    version = hash_obj({'source_id': sid, 'key': key, 'snapshot': snapshot})
                    with self.store.transaction() as db:
                        old = db.execute("SELECT * FROM resources WHERE source_id=? AND resource_key=? AND state NOT IN ('ACKED','SUPERSEDED')", (sid, key)).fetchone()
                        if old and old['resource_id'] != version:
                            if old['state'] != 'ACK_PENDING':
                                if not self._intact(dict(old)):
                                    db.execute("UPDATE resources SET state='HOLD',reason='source version changed' WHERE resource_id=?", (old['resource_id'],))
                                    summary['held'] += 1
                                else:
                                    summary['ready'] += 1  # Append-only growth waits for this snapshot's ACK.
                            continue
                        if old and old['state'] == 'HOLD':
                            summary['held'] += 1
                            continue
                        if not old:
                            db.execute("INSERT OR IGNORE INTO resources(resource_id,source_id,resource_key,directory,snapshot,state,total_bytes,file_count,created) VALUES(?,?,?,?,?,'READY',?,?,?)",
                                (version, sid, key, str(directory), _json(snapshot), sum(x['stat']['size'] for x in snapshot['files']), len(snapshot['files']), self._clock()))
                    summary['ready'] += 1
                    # Optional cleanup is small and explicit, never a generic rmtree.
                    private = directory / ('.aas-clean-' + version[:24])
                    if source.get('cleanup', {}).get('enabled') and (cleanup or private.exists()):
                        with self._resource_lock(version):
                            current_snapshot = self._snapshot(source, directory)
                            if not current_snapshot or current_snapshot[0] != snapshot:
                                raise ResourceError('SOURCE_CHANGED', 'Source changed before early-checkpoint cleanup')
                            for path, signature in cleanup:
                                if _ordinary(path) != signature:
                                    raise ResourceError('SOURCE_CHANGED', 'Early checkpoint changed')
                            # Persist immutable original-name/stat intent BEFORE
                            # any rename; remaining files never renumber on restart.
                            with self.store.transaction() as db:
                                for path, signature in cleanup:
                                    db.execute('INSERT OR IGNORE INTO early_cleanup(resource_id,name,signature) VALUES(?,?,?)',
                                        (version, path.name, _json(signature)))
                            cleaned, issues = self._finish_early_cleanup(self.store.one(version))
                            summary['cleaned'] += cleaned
                            summary['cleanup_issues'] += issues
                except (ResourceError, OSError):
                    summary['held'] += 1
        return summary

    def _scan_sharded(self, sid, source, directory, key, summary):
        """Publish disjoint immutable file groups; an existing lease always wins.

        The directory-wide completion gate runs before the FIRST publication.
        Once siblings are ACKed, the remaining persisted groups no longer need
        that full file set to exist. Every claim/ACK still fences its own files.
        """
        result = self._snapshot(source, directory)
        snapshot, cleanup = result if result else (None, [])
        individual = source.get('resource_mode') == 'file'
        target = source.get('target_resource_bytes', 0)
        owner = None
        with self.store.transaction() as db:
            rows = [dict(r) for r in db.execute(
                'SELECT * FROM resources WHERE source_id=? AND resource_key=? ORDER BY created,resource_id',
                (sid, key))]
            for row in rows:
                if row['state'] in ('ACKED', 'SUPERSEDED', 'ACK_PENDING', 'HOLD'):
                    continue
                marker_missing = source.get('completion_marker') and not (directory / source['completion_marker']).exists()
                if not self._intact(row) or marker_missing:
                    db.execute("UPDATE resources SET state='HOLD',reason='Source files changed' WHERE resource_id=?", (row['resource_id'],))
                    row['state'] = 'HOLD'
                    continue
                old_partition = json.loads(row['snapshot']).get('partition')
                never_claimed = row['state'] == 'READY' and all(row[field] is None for field in
                    ('manifest', 'consumer_id', 'request_id', 'lease_token', 'lease_expires'))
                needs_split = row['file_count'] > 1 and (individual or row['total_bytes'] > target)
                changed_target = (not individual and old_partition and old_partition['target_resource_bytes'] != target)
                if snapshot and never_claimed and (needs_split or changed_target):
                    db.execute("UPDATE resources SET state='SUPERSEDED',reason='Repartitioned unclaimed snapshot' WHERE resource_id=?", (row['resource_id'],))
                    row['state'] = 'SUPERSEDED'
            # Active snapshots reserve names even after mutation/HOLD. A new
            # version of a leased filename must never acquire a second owner.
            reserved = {item['name'] for row in rows if row['state'] not in ('ACKED', 'SUPERSEDED')
                        for item in json.loads(row['snapshot'])['files']}
            acknowledged = {(item['name'], _json(item['stat'])) for row in rows if row['state'] == 'ACKED'
                            for item in json.loads(row['snapshot'])['files']}
            groups, group, total = [], [], 0
            if snapshot:
                for item in snapshot['files']:
                    if item['name'] in reserved or (item['name'], _json(item['stat'])) in acknowledged:
                        continue
                    size = item['stat']['size']
                    if group and (individual or total + size > target):
                        groups.append(group)
                        group, total = [], 0
                    group.append(item)
                    total += size
                if group:
                    groups.append(group)
                epoch = None if individual else hash_obj(snapshot)
                for files in groups:
                    names = {item['name'] for item in files}
                    shard = {**snapshot, 'files': files}
                    if not individual:
                        shard['partition'] = {'target_resource_bytes': target, 'source_snapshot_sha256': epoch,
                                              'repartitioned_from': sorted(row['resource_id'] for row in rows
                                                  if row['state'] == 'SUPERSEDED' and any(item['name'] in names for item in json.loads(row['snapshot'])['files']))}
                    rid = hash_obj({'source_id': sid, 'key': key, 'snapshot': shard})
                    db.execute("INSERT OR IGNORE INTO resources(resource_id,source_id,resource_key,directory,snapshot,state,total_bytes,file_count,created) VALUES(?,?,?,?,?,'READY',?,?,?)",
                        (rid, sid, key, str(directory), _json(shard), sum(f['stat']['size'] for f in files), len(files), self._clock()))
                    row = dict(db.execute('SELECT * FROM resources WHERE resource_id=?', (rid,)).fetchone())
                    rows.append(row)
            summary['ready'] += sum(row['state'] in ('READY', 'PREPARING', 'LEASED', 'ACK_PENDING') for row in rows)
            summary['held'] += sum(row['state'] == 'HOLD' for row in rows)
            if not snapshot:
                summary['incomplete'] += 1
            if snapshot and cleanup:
                epoch_id = hash_obj({'source_id': sid, 'key': key, 'snapshot': snapshot})
                saved_owner = db.execute('SELECT resource_id FROM cleanup_epochs WHERE epoch_id=?', (epoch_id,)).fetchone()
                if saved_owner:
                    owner = dict(db.execute('SELECT * FROM resources WHERE resource_id=?', (saved_owner[0],)).fetchone())
                else:
                    owner = next((row for row in sorted(rows, key=lambda row: row['resource_id'])
                                  if row['state'] in ('READY', 'PREPARING', 'LEASED')), None)
                    if owner:
                        db.execute('INSERT INTO cleanup_epochs(epoch_id,resource_id) VALUES(?,?)', (epoch_id, owner['resource_id']))
        # A single immutable resource owns the early-file journal for this
        # publication. Early100..500 are outside EVERY shard's published set.
        if owner and cleanup and source.get('cleanup', {}).get('enabled'):
            with self._resource_lock(owner['resource_id']):
                current = self._snapshot(source, directory)
                if not current or current[0] != snapshot:
                    return  # A sibling ACK may legitimately remove main files.
                for path, signature in cleanup:
                    if _ordinary(path) != signature:
                        raise ResourceError('SOURCE_CHANGED', 'Early checkpoint changed')
                with self.store.transaction() as db:
                    for path, signature in cleanup:
                        db.execute('INSERT OR IGNORE INTO early_cleanup(resource_id,name,signature) VALUES(?,?,?)',
                            (owner['resource_id'], path.name, _json(signature)))
                cleaned, issues = self._finish_early_cleanup(owner)
                summary['cleaned'] += cleaned
                summary['cleanup_issues'] += issues

    def _cleanup_enabled(self, row):
        source = self.sources.get(row['source_id'])
        if not source or not source.get('cleanup', {}).get('enabled'):
            return False
        directory = self._directory(source, Path(row['directory']))
        marker = source.get('completion_marker')
        if not marker or not (directory / marker).exists():
            return False
        _ordinary(directory / marker)
        return True

    def _cleanup_record(self, rid, name, state, reason=None):
        with self.store.transaction() as db:
            db.execute('UPDATE early_cleanup SET state=?,reason=? WHERE resource_id=? AND name=?', (state, reason, rid, name))

    def _finish_early_cleanup(self, row):
        """Resume durable, exact early-file intent, even after main weights ACK."""
        if not self._cleanup_enabled(row):
            return 0, 0
        rid = row['resource_id']
        private = Path(row['directory']) / ('.aas-clean-' + rid[:24])
        private.mkdir(mode=0o700, exist_ok=True)
        if private.is_symlink() or private.resolve() != private:
            raise ResourceError('UNSAFE_PATH', 'Unsafe cleanup directory')
        with closing(self.store.connect()) as db:
            records = [dict(r) for r in db.execute('SELECT * FROM early_cleanup WHERE resource_id=?', (rid,))]
        known = {r['name'] for r in records}
        # Old releases used positional slots without a durable name mapping. An
        # unknown capture is never guessed/deleted and cannot block other files.
        for path in private.iterdir():
            if path.name not in known:
                name = '!unmapped:' + path.name
                with self.store.transaction() as db:
                    db.execute("INSERT OR IGNORE INTO early_cleanup(resource_id,name,signature,state,reason) VALUES(?,?,'{}','HOLD','Unmapped legacy capture preserved')", (rid, name))
        cleaned, issues = 0, 0
        for record in records:
            if record['state'] != 'PENDING':
                continue
            name = record['name']
            checkpoint = self._checkpoint(name, row['resource_key'])
            allowed = self.sources[row['source_id']]['cleanup']['checkpoint_steps']
            if not checkpoint or checkpoint[1] not in allowed:
                self._cleanup_record(rid, name, 'HOLD', 'No longer authorized by current cleanup rule')
                issues += 1
                continue
            signature = json.loads(record['signature'])
            try:
                source = self._path(row, name)
                captured = private / _name(name)
                if source.exists() and captured.exists():
                    raise ResourceError('SOURCE_CHANGED', 'Replacement source exists beside captured old checkpoint')
                if source.exists():
                    if _ordinary(source) != signature:
                        raise ResourceError('SOURCE_CHANGED', 'Early checkpoint replacement preserved')
                    self._capture_checked(source, captured, signature)
                if captured.exists():
                    actual = _ordinary(captured)
                    if any(actual[k] != signature[k] for k in ('dev','ino','size','mtime_ns','mode')):
                        raise ResourceError('SOURCE_CHANGED', 'Captured early checkpoint changed')
                    captured.unlink()
                    fsync_dir(private)
                    cleaned += 1
                # Missing source+capture is a replay after a completed unlink.
                self._cleanup_record(rid, name, 'DONE')
            except ResourceError as exc:
                self._cleanup_record(rid, name, 'HOLD', str(exc))
                issues += 1
            except OSError:
                # Leave durable PENDING intent for the next scan; isolate I/O
                # failure from publication and all other source directories.
                issues += 1
        try:
            private.rmdir()
            fsync_dir(Path(row['directory']))
        except OSError:
            if not private.exists():
                raise
            issues += 1  # Unknown legacy capture or pending I/O remains preserved.
        return cleaned, issues

    def _recover_early_cleanup(self):
        with closing(self.store.connect()) as db:
            pending = [r[0] for r in db.execute("SELECT DISTINCT resource_id FROM early_cleanup WHERE state='PENDING'")]
        cleaned, issues = 0, 0
        for rid in pending:
            row = self.store.one(rid)
            if not row:
                continue
            try:
                with self._resource_lock(rid):
                    count, errors = self._finish_early_cleanup(row)
                    cleaned += count
                    issues += errors
            except (ResourceError, OSError):
                issues += 1
        return cleaned, issues

    def _lease(self, db, resource_id, consumer_id, token, *, renew=False, pending=False):
        self._identity(consumer_id)
        row = db.execute('SELECT * FROM resources WHERE resource_id=?', (resource_id,)).fetchone()
        if not row:
            raise ResourceError('NOT_FOUND', 'Unknown resource')
        row = dict(row)
        valid_states = ('LEASED', 'PREPARING') + (('ACK_PENDING', 'ACKED') if pending else ())
        if row['state'] not in valid_states or row['consumer_id'] != consumer_id or not token or row['lease_token'] != token:
            raise ResourceError('LEASE_LOST', 'Lease no longer owns this resource')
        if row['state'] in ('LEASED', 'PREPARING') and (row['lease_expires'] or 0) <= self._clock():
            raise ResourceError('LEASE_EXPIRED', 'Resource lease expired')
        if renew and row['state'] in ('LEASED', 'PREPARING'):
            row['lease_expires'] = self._clock() + self.lease_seconds
            db.execute('UPDATE resources SET lease_expires=? WHERE resource_id=?', (row['lease_expires'], resource_id))
        return row

    def _touch(self, resource_id, consumer_id, token):
        with self.store.transaction() as db:
            return self._lease(db, resource_id, consumer_id, token, renew=True)

    def _intact(self, row):
        try:
            return all(_ordinary(self._path(row, item['name'])) == item['stat'] for item in json.loads(row['snapshot'])['files'])
        except (ResourceError, OSError):
            return False

    def _hash(self, row, item, consumer_id=None, token=None, path=None):
        path = path or self._path(row, item['name'])
        if _ordinary(path) != item['stat']:
            raise ResourceError('SOURCE_CHANGED', 'Source stat changed')
        digest = hashlib.sha256()
        last = self._clock()
        flags = os.O_RDONLY | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0)
        with os.fdopen(os.open(path, flags), 'rb') as stream:
            if _sig(os.fstat(stream.fileno())) != item['stat']:
                raise ResourceError('SOURCE_CHANGED', 'Source changed on open')
            while block := stream.read(1024 * 1024):
                digest.update(block)
                if token and self._clock() - last >= min(5, self.lease_seconds / 3):
                    self._touch(row['resource_id'], consumer_id, token)
                    last = self._clock()
            if _sig(os.fstat(stream.fileno())) != item['stat'] or _ordinary(path) != item['stat']:
                raise ResourceError('SOURCE_CHANGED', 'Source changed during hash')
        return digest.hexdigest()

    def _prepare(self, row, consumer_id, token):
        snapshot = json.loads(row['snapshot'])
        files = []
        for item in snapshot['files']:
            if self.sources[row['source_id']].get('verify_images'):
                from PIL import Image
                try:
                    with Image.open(self._path(row, item['name'])) as image:
                        image.verify()
                except Exception as exc:
                    raise ResourceError('INVALID_IMAGE', 'Image verification failed') from exc
            digest = self._hash(row, item, consumer_id, token)
            files.append({'name': item['name'], 'size': item['stat']['size'], 'sha256': digest})
        self._check_marker(row, snapshot)
        manifest = {'resource_id': row['resource_id'], 'source_id': row['source_id'], 'key': row['resource_key'],
                    'files': files, 'total_bytes': row['total_bytes']}
        manifest['manifest_sha256'] = hash_obj(manifest)
        return manifest

    def claim(self, resource_id, consumer_id, request_id):
        self._identity(consumer_id)
        try:
            validate_id(request_id)
        except AASError as exc:
            raise ResourceError('BAD_REQUEST', 'Unsafe request id') from exc
        with self.store.transaction() as db:
            raw = db.execute('SELECT * FROM resources WHERE resource_id=?', (resource_id,)).fetchone()
            if not raw:
                raise ResourceError('NOT_FOUND', 'Unknown resource')
            row = dict(raw)
            if row['state'] == 'ACKED':
                return {'status': 'acked', 'resource_id': resource_id}
            if row['state'] == 'SUPERSEDED':
                return {'status': 'superseded', 'resource_id': resource_id}
            if row['state'] == 'HOLD':
                raise ResourceError('HOLD', 'Source version is held')
            if row['state'] == 'ACK_PENDING':
                return {'status': 'busy'}
            if not self._intact(row):
                db.execute("UPDATE resources SET state='HOLD',reason='source changed before claim' WHERE resource_id=?", (resource_id,))
                return {'status': 'busy', 'reason': 'source_changed_hold'}
            if row['state'] in ('PREPARING', 'LEASED') and row['lease_expires'] > self._clock():
                if row['consumer_id'] != consumer_id or row['request_id'] != request_id:
                    return {'status': 'busy'}
                if row['state'] == 'PREPARING':
                    return {'status': 'busy', 'lease_token': row['lease_token'], 'lease_expires': row['lease_expires']}
                db.execute('UPDATE resources SET lease_expires=? WHERE resource_id=?', (self._clock() + self.lease_seconds, resource_id))
                return {'status': 'claimed', 'lease_token': row['lease_token'], 'lease_expires': self._clock() + self.lease_seconds, 'manifest': json.loads(row['manifest'])}
            token = secrets.token_hex(32)
            expires = self._clock() + self.lease_seconds
            state = 'LEASED' if row['manifest'] else 'PREPARING'
            db.execute('UPDATE resources SET state=?,consumer_id=?,request_id=?,lease_token=?,lease_expires=? WHERE resource_id=?', (state, consumer_id, request_id, token, expires, resource_id))
        try:
            manifest = json.loads(row['manifest']) if row['manifest'] else self._prepare(row, consumer_id, token)
            with self.store.transaction() as db:
                self._lease(db, resource_id, consumer_id, token, renew=True)
                db.execute("UPDATE resources SET state='LEASED',manifest=? WHERE resource_id=?", (_json(manifest), resource_id))
            return {'status': 'claimed', 'lease_token': token, 'lease_expires': self.store.one(resource_id)['lease_expires'], 'manifest': manifest}
        except (ResourceError, OSError) as exc:
            if isinstance(exc, OSError) or getattr(exc, 'code', None) in ('SOURCE_CHANGED','INVALID_IMAGE'):
                self._hold(resource_id, 'Source changed while preparing manifest')
            raise

    def heartbeat(self, resource_id, consumer_id, lease_token):
        row = self._touch(resource_id, consumer_id, lease_token)
        return {'status': 'ok', 'lease_expires': row['lease_expires']}

    def manifest(self, resource_id, consumer_id, lease_token):
        row = self._touch(resource_id, consumer_id, lease_token)
        if not row['manifest']:
            raise ResourceError('BUSY', 'Manifest is preparing')
        return json.loads(row['manifest'])

    def get_range(self, resource_id, consumer_id, lease_token, name, offset, length):
        _name(name)
        if type(offset) is not int or type(length) is not int or offset < 0 or length < 1 or length > MAX_RANGE:
            raise ResourceError('BAD_RANGE', 'Range must be positive and at most16MiB')
        row = self._touch(resource_id, consumer_id, lease_token)
        if row['state'] != 'LEASED' or not row['manifest']:
            raise ResourceError('BUSY', 'Manifest is preparing')
        snapshot = json.loads(row['snapshot'])
        item = next((x for x in snapshot['files'] if x['name'] == name), None)
        if item is None or offset > item['stat']['size']:
            raise ResourceError('BAD_RANGE', 'File/range not in manifest')
        try:
            path = self._path(row, name)
            if _ordinary(path) != item['stat']:
                raise ResourceError('SOURCE_CHANGED', 'Source changed before range')
            flags = os.O_RDONLY | getattr(os, 'O_BINARY', 0) | getattr(os, 'O_NOFOLLOW', 0)
            with os.fdopen(os.open(path, flags), 'rb') as stream:
                if _sig(os.fstat(stream.fileno())) != item['stat']:
                    raise ResourceError('SOURCE_CHANGED', 'Source changed on open')
                stream.seek(offset)
                data = stream.read(min(length, item['stat']['size'] - offset))
                if _sig(os.fstat(stream.fileno())) != item['stat'] or _ordinary(path) != item['stat']:
                    raise ResourceError('SOURCE_CHANGED', 'Source changed during range')
            self._check_marker(row, snapshot)
            self._touch(resource_id, consumer_id, lease_token)  # Fence before returning bytes.
            return data
        except (ResourceError, OSError) as exc:
            if isinstance(exc, OSError) or getattr(exc, 'code', None) == 'SOURCE_CHANGED':
                current = self.store.one(resource_id)
                if current and current['state'] == 'LEASED' and current['lease_token'] == lease_token:
                    self._hold(resource_id, 'Source changed while serving range')
            raise

    def _check_marker(self, row, snapshot):
        marker = snapshot.get('marker')
        if marker:
            _ordinary(self._path(row, marker['name']))

    def _validate_receipt(self, row, receipt):
        manifest = json.loads(row['manifest'])
        if not isinstance(receipt, dict) or receipt.get('manifest_sha256') != manifest['manifest_sha256']:
            raise ResourceError('BAD_ACK', 'Receipt manifest hash mismatch')
        files = receipt.get('files')
        if not isinstance(files, list) or len(files) != len(manifest['files']):
            raise ResourceError('BAD_ACK', 'Receipt file set mismatch')
        if any(not isinstance(x, dict) or set(x) != {'name', 'size', 'sha256'} for x in files):
            raise ResourceError('BAD_ACK', 'Expected exact name/size/SHA receipt entries')
        if any(not isinstance(x['name'], str) or type(x['size']) is not int or x['size'] < 0 or
               not isinstance(x['sha256'], str) or not re.fullmatch('[0-9a-f]{64}', x['sha256']) for x in files):
            raise ResourceError('BAD_ACK', 'Invalid name/size/SHA receipt entries')
        if sorted(files, key=lambda x:x['name']) != sorted(manifest['files'], key=lambda x:x['name']):
            raise ResourceError('BAD_ACK', 'Receipt file contents mismatch')

    def ack(self, resource_id, consumer_id, lease_token, receipt):
        self._identity(consumer_id)
        with self._resource_lock(resource_id, wait=True):
            with self.store.transaction() as db:
                row = self._lease(db, resource_id, consumer_id, lease_token, pending=True)
            if not row['manifest']:
                raise ResourceError('BUSY', 'Manifest is preparing')
            self._validate_receipt(row, receipt)
            if row['state'] == 'ACKED':
                return {'status': 'already_acked'}
            if row['state'] != 'ACK_PENDING':
                snapshot = json.loads(row['snapshot'])
                try:
                    self._preflight(row, snapshot, consumer_id, lease_token, recovering=False)
                except (ResourceError, OSError):
                    self._hold(resource_id, 'Source changed before ACK; no files deleted')
                    raise
                with self.store.transaction() as db:
                    self._lease(db, resource_id, consumer_id, lease_token, renew=True)
                    db.execute('INSERT INTO ack_journal(resource_id,consumer_id,lease_token,receipt) VALUES(?,?,?,?)', (resource_id, consumer_id, lease_token, _json(receipt)))
                    db.execute("UPDATE resources SET state='ACK_PENDING' WHERE resource_id=?", (resource_id,))
            self._finish_ack(self.store.one(resource_id), preflight_done=row['state'] != 'ACK_PENDING')
            return {'status': 'acked'}

    def _quarantine(self, row):
        return Path(row['directory']) / ('.aas-ack-' + row['resource_id'][:24])

    def _preflight(self, row, snapshot, consumer_id=None, token=None, recovering=False):
        self._check_marker(row, snapshot)
        quarantine = self._quarantine(row)
        if quarantine.exists() and (quarantine.is_symlink() or quarantine.resolve() != quarantine):
            raise ResourceError('UNSAFE_PATH', 'Unsafe ACK quarantine')
        manifest_files = {x['name']: x for x in json.loads(row['manifest'])['files']}
        for index, item in enumerate(snapshot['files']):
            path = self._path(row, item['name'])
            captured = quarantine / str(index)
            if path.exists() and captured.exists():
                raise ResourceError('SOURCE_CHANGED', 'New source appeared during ACK')
            if not path.exists():
                if not recovering:
                    raise ResourceError('SOURCE_CHANGED', 'Source missing before ACK')
                if not captured.exists():
                    continue
                path = captured
            hashing_item = item
            if path == captured:
                signature = _ordinary(captured)
                if any(signature[k] != item['stat'][k] for k in ('dev','ino','size','mtime_ns','mode')):
                    raise ResourceError('SOURCE_CHANGED', 'Captured source changed')
                hashing_item = {**item, 'stat': signature}
            digest = self._hash(row, hashing_item, consumer_id, token, path=path)
            if digest != manifest_files[item['name']]['sha256']:
                raise ResourceError('SOURCE_CHANGED', 'Source hash changed before deletion')

    def _capture_checked(self, source, captured, signature):
        rename_new(source, captured)
        after = _ordinary(captured)
        if any(after[k] != signature[k] for k in ('dev','ino','size','mtime_ns','mode')):
            if not source.exists():
                rename_new(captured, source)
            raise ResourceError('SOURCE_CHANGED', 'Replacement captured; restored without deleting')

    def _finish_ack(self, row, preflight_done=False):
        snapshot = json.loads(row['snapshot'])
        try:
            if not preflight_done:
                self._preflight(row, snapshot, recovering=True)
            if not snapshot.get('delete_after_ack', True):
                with self.store.transaction() as db:
                    db.execute('UPDATE ack_journal SET complete=1 WHERE resource_id=?', (row['resource_id'],))
                    db.execute("UPDATE resources SET state='ACKED' WHERE resource_id=?", (row['resource_id'],))
                return
            quarantine = self._quarantine(row)
            quarantine.mkdir(mode=0o700, exist_ok=True)
            if quarantine.resolve() != quarantine or quarantine.is_symlink():
                raise ResourceError('UNSAFE_PATH', 'Unsafe ACK quarantine')
            with closing(self.store.connect()) as db:
                journal = db.execute('SELECT * FROM ack_journal WHERE resource_id=?', (row['resource_id'],)).fetchone()
            done = set(json.loads(journal['done']))
            for index, item in enumerate(snapshot['files']):
                name = item['name']
                source, captured = self._path(row, name), quarantine / str(index)
                if source.exists():
                    if _ordinary(source) != item['stat']:
                        raise ResourceError('SOURCE_CHANGED', 'Refuse deleting replacement file')
                    self._capture_checked(source, captured, item['stat'])
                if captured.exists():
                    captured.unlink()
                    fsync_dir(quarantine)
                done.add(name)
                with self.store.transaction() as db:
                    db.execute('UPDATE ack_journal SET done=? WHERE resource_id=?', (_json(sorted(done)), row['resource_id']))
            quarantine.rmdir()
            fsync_dir(Path(row['directory']))
            with self.store.transaction() as db:
                db.execute('UPDATE ack_journal SET complete=1 WHERE resource_id=?', (row['resource_id'],))
                db.execute("UPDATE resources SET state='ACKED' WHERE resource_id=?", (row['resource_id'],))
        except ResourceError:
            self._hold(row['resource_id'], 'Source changed during ACK; replacement preserved')
            raise

    def _recover_pending(self):
        for row in self.store.rows():
            if row['state'] == 'ACK_PENDING':
                try:
                    with self._resource_lock(row['resource_id']):
                        self._finish_ack(row)
                except (ResourceError, OSError):
                    pass

    def catalog(self, consumer_id=None, active=None):
        consumer_id = self._identity(consumer_id)
        for lease in active or []:
            self.heartbeat(lease['resource_id'], consumer_id, lease['lease_token'])
        summary = self.scan()
        return {'resources': [{'resource_id': row['resource_id'], 'source_id': row['source_id'],
            'key': row['resource_key'], 'state': row['state'], 'total_bytes': row['total_bytes'], 'file_count': row['file_count']}
            for row in self.store.rows() if row['state'] not in ('ACKED', 'SUPERSEDED')], 'missing_sources': summary['missing_sources']}

    def status(self):
        rows = self.store.rows()
        with closing(self.store.connect()) as db:
            cleanup = {r[0]:r[1] for r in db.execute('SELECT state,COUNT(*) FROM early_cleanup GROUP BY state')}
        return {'producer_id': self.producer_id, 'consumer_id': self.consumer_id,
                'early_cleanup': cleanup,
                'resources': len(rows), 'states': {state:sum(row['state'] == state for row in rows)
                for state in ('READY', 'PREPARING', 'LEASED', 'ACK_PENDING', 'ACKED', 'HOLD', 'SUPERSEDED')}}

    def dispatch(self, request):
        try:
            if not isinstance(request, dict):
                raise ResourceError('BAD_REQUEST', 'Expected a JSON object')
            if request.get('producer_id', self.producer_id) != self.producer_id:
                raise ResourceError('AUTH', 'Producer identity mismatch')
            op = request.get('op')
            consumer = request.get('consumer_id', self.consumer_id)
            self._identity(consumer)
            if op == 'catalog':
                return self.catalog(consumer, request.get('active'))
            if op == 'status':
                return self.status()
            if op == 'claim':
                return self.claim(request['resource_id'], consumer, request['request_id'])
            if op in ('heartbeat', 'manifest', 'ack', 'get'):
                args = (request['resource_id'], consumer, request['lease_token'])
                if op == 'get':
                    return self.get_range(*args, request['name'], request['offset'], request['length'])
                if op == 'ack':
                    return self.ack(*args, request['receipt'])
                return getattr(self, op)(*args)
            raise ResourceError('BAD_REQUEST', 'Unknown resource operation')
        except ResourceError as exc:
            return {'status': 'error', 'code': exc.code, 'message': str(exc)}
        except (KeyError, TypeError, ValueError):
            return {'status': 'error', 'code': 'BAD_REQUEST', 'message': 'Missing or invalid request fields'}
        except OSError:
            return {'status': 'error', 'code': 'IO_ERROR', 'message': 'Resource file or state storage unavailable'}
