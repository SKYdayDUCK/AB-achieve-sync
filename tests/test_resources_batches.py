"""Durable poll batches: real local A/B dispatch, synthetic files, no network."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, redirect_stdout
from copy import deepcopy
import json
from io import StringIO
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from aas.resources.client import ResourceClient
from aas.resources.config import load_config
from aas.resources.server import ResourceServer
from aas.resources import cli
from test_resources_client import FakeTransport

LEGACY_SCHEMA = '''CREATE TABLE resources(
 resource_id TEXT PRIMARY KEY,source_id TEXT NOT NULL,resource_key TEXT NOT NULL,
 state TEXT NOT NULL,request_id TEXT NOT NULL,lease_token TEXT,manifest TEXT,
 output_dir TEXT,attempts INTEGER NOT NULL DEFAULT 0,next_attempt REAL NOT NULL DEFAULT 0,
 error TEXT,created REAL NOT NULL,updated REAL NOT NULL)'''


class ResourceBatchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='aas-batches-test-')
        self.root = Path(self.temp.name).resolve()
        self.input = self.root/'source'
        self.input.mkdir()
        self.config = {'node': {'producer_id': 'A', 'consumer_id': 'B'},
            'paths': {'root_dir': str(self.root/'B'), 'output_dirs': [str(self.root/'output')],
                      'include_source_directory': True},
            'capacity': {'min_free_bytes': 0},
            'transfer': {'max_download_queue': 4, 'chunk_bytes': 7},
            'scheduler': {'strategy': 'fixed_interval', 'fixed_interval': {'interval_seconds': 600}}}
        self.server = ResourceServer(load_config({
            'node': {'producer_id': 'A', 'consumer_id': 'B'},
            'paths': {'root_dir': str(self.root/'A-state')},
            'sources': [{'id': 'exports', 'root': str(self.input), 'subdir_glob': '*',
                         'suffixes': ['.bin'], 'min_age_seconds': 0}]}, 'files', 'a'))

    def tearDown(self):
        self.assertTrue(self.root.name.startswith('aas-batches-test-'))
        self.temp.cleanup()

    def source(self, key):
        directory = self.input/key
        directory.mkdir()
        path = directory/'payload.bin'
        path.write_bytes(b'0123456789'*4)
        return path

    def client(self):
        transport = FakeTransport(self.server)
        return ResourceClient(deepcopy(self.config), transport=transport), transport

    def fill(self, client, counts=(3, 3, 3, 3)):
        results = []
        for batch, count in enumerate(counts):
            for n in range(count): self.source(f'batch{batch}-role{n}')
            result = client.poll()
            self.assertEqual((result['enqueued'], result['enqueued_batches']), (count, 1))
            results.append(result)
        return results

    def legacy_database(self, rows):
        state = self.root/'B'/'state'
        state.mkdir(parents=True)
        with closing(sqlite3.connect(state/'resources.sqlite3')) as db:
            with db:
                db.execute(LEGACY_SCHEMA)
                for row in rows:
                    db.execute('INSERT INTO resources('+','.join(row)+') VALUES('+
                               ','.join('?' for _ in row)+')', list(row.values()))
        return state

    def legacy_row(self, rid='old', state='RETRY', **changes):
        return {'resource_id': rid, 'source_id': 'exports', 'resource_key': 'hero',
            'state': state, 'request_id': 'unchanged-request-'+rid,
            'lease_token': 'unchanged-lease-'+rid, 'manifest': '{"kept":"verbatim"}',
            'output_dir': str(self.root/'output'/'exports'/'hero'), 'attempts': 3,
            'next_attempt': 12345.75, 'error': 'keep retry reason', 'created': 123.5,
            'updated': 456.75, **changes}

    def test_one_poll_more_than_four_roles_share_one_durable_batch(self):
        for n in range(9): self.source('role'+str(n))
        client, _ = self.client()
        result = client.poll()
        self.assertEqual((result['enqueued'], result['enqueued_batches']), (9, 1))
        self.assertEqual(client.pending_count(), 9)
        self.assertEqual(client.pending_batch_count(), 1)
        self.assertEqual({r['batch_id'] for r in client.rows()}, {result['batch_id']})
        batch = client.batch_rows()[0]
        self.assertEqual((batch['origin'], batch['resource_count'], batch['pending_resource_count']), ('poll', 9, 9))
        restarted, _ = self.client()
        self.assertEqual(restarted.batch_rows(), client.batch_rows())
        self.assertEqual(restarted.rows(), client.rows())

    def test_four_batches_skip_catalog_but_existing_worker_progresses(self):
        client, transport = self.client()
        self.fill(client)
        self.assertEqual(client.pending_count(), 12)
        self.assertEqual(client.pending_batch_count(), 4)
        self.source('not-yet-discovered')
        catalogs = transport.count('catalog')
        result = client.poll()
        self.assertTrue(result['queue_full'])
        self.assertEqual((result['discovered'], result['enqueued'], result['enqueued_batches']), (0, 0, 0))
        self.assertIsNone(result['batch_id'])
        self.assertEqual(transport.count('catalog'), catalogs)
        self.assertEqual(client.policy.interval_seconds, 600)
        self.assertTrue(client.work_once())
        self.assertEqual(client.pending_count(), 11)
        self.assertEqual(client.pending_batch_count(), 4)
        self.assertTrue(client.poll()['queue_full'])
        self.assertEqual(transport.count('catalog'), catalogs)
        self.assertGreater(transport.count('get'), 0)

    def test_partial_completion_keeps_slot_whole_batch_completion_releases_it(self):
        client, _ = self.client()
        first = self.fill(client)[0]['batch_id']
        for _ in range(2):
            self.assertTrue(client.work_once())
            self.assertEqual(client.pending_batch_count(), 4)
        self.assertTrue(client.work_once())
        self.assertTrue(all(r['state']=='DONE' for r in client.rows() if r['batch_id']==first))
        self.assertEqual(client.pending_batch_count(), 3)
        for n in range(7): self.source('new-role'+str(n))
        result = client.poll()
        self.assertEqual((result['enqueued'], result['enqueued_batches']), (7, 1))
        self.assertEqual(client.pending_batch_count(), 4)

    def test_hold_keeps_batch_occupied_done_and_skipped_release_it(self):
        client, _ = self.client()
        self.fill(client, (2,))
        a, b = client.rows()
        client._set(a['resource_id'], 'HOLD', error='manual investigation')
        client._set(b['resource_id'], 'SKIPPED')
        self.assertEqual(client.pending_batch_count(), 1)
        self.assertEqual(client.batch_rows()[0]['pending_resource_count'], 1)
        client._set(a['resource_id'], 'SKIPPED')
        self.assertEqual(client.pending_batch_count(), 0)
        self.assertEqual(len(client.batch_rows()), 1)

    def test_empty_and_known_only_polls_create_no_batch(self):
        client, _ = self.client()
        for _ in range(2):
            result = client.poll()
            self.assertEqual((result['enqueued'], result['enqueued_batches']), (0, 0))
            self.assertIsNone(result['batch_id'])
        self.assertEqual(client.batch_rows(), [])
        self.source('one')
        client.poll()
        before = client.batch_rows()
        result = client.poll()
        self.assertEqual((result['enqueued'], result['enqueued_batches']), (0, 0))
        self.assertEqual(client.batch_rows(), before)

    def test_status_reports_batch_unit_and_cli_exposes_id_without_tokens(self):
        for n in range(6): self.source('role'+str(n))
        client, _ = self.client()
        result = client.poll()
        row = client.rows()[0]
        client._set(row['resource_id'], 'RETRY', lease_token='private-lease-token')
        client._status('batch-test')
        status = json.loads((client.state/'status.json').read_bytes())
        self.assertEqual(status['queue_unit'], 'batches')
        self.assertEqual(status['queue_limit'], 4)
        self.assertEqual((status['pending_batches'], status['pending_resources']), (1, 6))
        self.assertEqual(status['batch_counts'], {'pending': 1, 'complete': 0})
        output = StringIO()
        with patch('aas.resources.client.ResourceClient', return_value=client), redirect_stdout(output):
            cli.main_b(['--config', 'unused.yaml', '--preset', 'files', 'status'])
        rows = json.loads(output.getvalue())
        self.assertEqual(len(rows), 6)
        self.assertEqual({r['batch_id'] for r in rows}, {result['batch_id']})
        for public in rows:
            self.assertNotIn('lease_token', public)
            self.assertNotIn('request_id', public)
        self.assertNotIn('private-lease-token', output.getvalue())
        self.assertNotIn(row['request_id'], output.getvalue())

    def concurrent_poll(self, clients):
        start = threading.Barrier(len(clients))
        def poll(client):
            start.wait(timeout=10)
            return client.poll()
        with ThreadPoolExecutor(max_workers=len(clients)) as executor:
            return list(executor.map(poll, clients))

    def test_concurrent_instances_dedupe_discovery_into_one_batch(self):
        for n in range(10): self.source('role'+str(n))
        clients = [self.client()[0] for _ in range(6)]
        results = self.concurrent_poll(clients)
        self.assertEqual(sum(r['enqueued'] for r in results), 10)
        self.assertEqual(sum(r['enqueued_batches'] for r in results), 1)
        self.assertEqual(len(clients[0].rows()), 10)
        self.assertEqual(clients[0].pending_batch_count(), 1)

    def test_concurrent_polls_at_last_slot_never_exceed_four_batches(self):
        original, _ = self.client()
        self.fill(original, (2, 2, 2))
        for n in range(8): self.source('last-slot-role'+str(n))
        clients = [self.client()[0] for _ in range(6)]
        results = self.concurrent_poll(clients)
        self.assertEqual(sum(r['enqueued'] for r in results), 8)
        self.assertEqual(sum(r['enqueued_batches'] for r in results), 1)
        self.assertEqual(original.pending_count(), 14)
        self.assertEqual(original.pending_batch_count(), 4)
        self.assertEqual(len(original.batch_rows()), 4)

    def test_legacy_migrates_once_preserving_every_old_field_and_file(self):
        rows = [self.legacy_row('old-'+state, state) for state in
                ('QUEUED', 'RUNNING', 'RETRY', 'ACK_PENDING', 'HOLD', 'DONE', 'SKIPPED')]
        state = self.legacy_database(rows)
        output = Path(rows[0]['output_dir'])
        output.mkdir(parents=True)
        part = output/'payload.bin.part'
        part.write_bytes(b'preserve these partial bytes')
        receipts = state/'receipts'
        receipts.mkdir()
        receipt = receipts/'old-ACK_PENDING.json'
        receipt.write_bytes(b'{"preserve":"receipt bytes and whitespace"}\n')
        before_files = (part.read_bytes(), receipt.read_bytes())
        client, _ = self.client()
        actual = {r['resource_id']: r for r in client.rows()}
        for row in rows: self.assertEqual({k: actual[row['resource_id']][k] for k in row}, row)
        self.assertEqual(len({r['batch_id'] for r in actual.values()}), 1)
        self.assertEqual(client.pending_count(), 5)
        self.assertEqual(client.pending_batch_count(), 1)
        self.assertEqual(client.batch_rows()[0]['origin'], 'legacy')
        restarted, _ = self.client()
        self.assertEqual(restarted.rows(), client.rows())
        self.assertEqual(len(restarted.batch_rows()), 1)
        self.assertEqual((part.read_bytes(), receipt.read_bytes()), before_files)

    def test_legacy_partial_resumes_same_request_lease_and_byte_offset(self):
        source = self.source('hero')
        transport = FakeTransport(self.server)
        rid = transport.call({'op': 'catalog', 'consumer_id': 'B'})['resources'][0]['resource_id']
        request = 'legacy-real-request'
        claim = transport.call({'op': 'claim', 'consumer_id': 'B', 'resource_id': rid, 'request_id': request})
        row = self.legacy_row(rid, request_id=request, lease_token=claim['lease_token'],
                              manifest=json.dumps(claim['manifest']), next_attempt=0)
        self.legacy_database([row])
        target = Path(row['output_dir'])
        target.mkdir(parents=True)
        expected = source.read_bytes()
        (target/'payload.bin.part').write_bytes(expected[:7])
        client = ResourceClient(self.config, transport=transport)
        self.assertEqual(client.rows()[0]['request_id'], request)
        self.assertEqual(client.rows()[0]['lease_token'], claim['lease_token'])
        start = len(transport.calls)
        self.assertTrue(client.work_once())
        gets = [c for c in transport.calls[start:] if c['op']=='get']
        self.assertEqual(gets[0]['offset'], 7)
        self.assertTrue(all(c['lease_token']==claim['lease_token'] for c in gets))
        self.assertEqual((target/'payload.bin').read_bytes(), expected)
        self.assertEqual(client.rows()[0]['state'], 'DONE')
        self.assertEqual(client.pending_batch_count(), 0)
        self.assertFalse(source.exists())

    def test_done_only_legacy_takes_no_slot_or_redownloads_deleted_output(self):
        self.source('hero')
        rid = FakeTransport(self.server).call({'op': 'catalog', 'consumer_id': 'B'})['resources'][0]['resource_id']
        self.legacy_database([self.legacy_row(rid, 'DONE')])
        client, transport = self.client()
        self.assertEqual(client.pending_batch_count(), 0)
        self.assertEqual(client.poll()['enqueued'], 0)
        self.assertFalse(client.work_once())
        self.assertEqual(transport.count('get'), 0)
        self.assertEqual(transport.count('ack'), 0)
        self.assertFalse(Path(client.rows()[0]['output_dir']).exists())
        self.assertEqual(client.rows()[0]['state'], 'DONE')


if __name__=='__main__': unittest.main()
