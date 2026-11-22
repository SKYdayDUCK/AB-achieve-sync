"""Local A/B integration fixtures. No network/SSH, credentials or real datasets."""
from copy import deepcopy
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from aas.common.core import hash_obj, read_json, write_json
from aas.resources.client import ResourceClient
from aas.resources.config import load_config
from aas.resources.server import ResourceServer
from aas.resources.transport import RemoteError


class FakeTransport:
    """Preserve real dispatch semantics, including error conversion and raw bytes."""
    def __init__(self, server):
        self.server = server
        self.calls = []
        self.closed = False
        self.mutate_claim = None
        self.corrupt_bytes = False
        self.lose_ack = False
        self.interrupt_get_number = None
        self.get_count = 0

    def call(self, request, binary=False):
        self.calls.append(deepcopy(request))
        if request['op'] == 'get':
            self.get_count += 1
            if self.get_count == self.interrupt_get_number:
                raise KeyboardInterrupt('simulated receiver process death')
        answer = self.server.dispatch(request)
        if isinstance(answer, dict) and answer.get('status') == 'error':
            raise RemoteError(answer.get('code', 'REMOTE_ERROR'), answer.get('message', ''))
        if request['op'] == 'claim' and answer.get('status') == 'claimed' and self.mutate_claim:
            answer = deepcopy(answer)
            self.mutate_claim(answer['manifest'])
        if request['op'] == 'ack' and self.lose_ack:
            self.lose_ack = False
            raise ConnectionError('ACK reached A; response was lost')
        if binary:
            if not isinstance(answer, bytes):
                raise AssertionError('get response must stay bytes')
            if self.corrupt_bytes and answer:
                return bytes([answer[0] ^ 1]) + answer[1:]
        return answer

    def close(self):
        self.closed = True

    def count(self, op):
        return sum(row['op'] == op for row in self.calls)


class ResourceClientIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='aas-client-test-')
        self.root = Path(self.temp.name).resolve()
        self.sources = []
        self.bconfig = {'node': {'producer_id':'A','consumer_id':'B'},
            'paths': {'root_dir':str(self.root/'B'), 'output_dirs':[str(self.root/'output')], 'include_source_directory':True},
            'capacity': {'min_free_bytes':0}, 'transfer': {'max_download_queue':4,'chunk_bytes':7},
            'scheduler': {'strategy':'fixed_interval','fixed_interval':{'interval_seconds':600}}}

    def tearDown(self):
        self.assertTrue(Path(self.temp.name).resolve().name.startswith('aas-client-test-'))
        self.temp.cleanup()

    def source(self, sid='s1', key='hero', name='payload.bin', data=b'0123456789' * 8, **options):
        root = self.root/'A-input'/sid
        directory = root/key
        directory.mkdir(parents=True, exist_ok=True)
        (directory/name).write_bytes(data)
        if not any(source['id']==sid for source in self.sources):
            self.sources.append({'id':sid,'root':str(root),'subdir_glob':'*','suffixes':['.bin'],
                                 'min_age_seconds':0,**options})
        return directory/name

    def pair(self, preset='files'):
        cfg = {'node':{'producer_id':'A','consumer_id':'B'},'paths':{'root_dir':str(self.root/'A-state')},'sources':self.sources,'lease':{'seconds':1200}}
        server = ResourceServer(load_config(cfg,preset,'a'))
        transport = FakeTransport(server)
        return server,transport,ResourceClient(self.bconfig,transport=transport,preset=preset)

    def ready_retry(self, client):
        with client.tx() as db:
            db.execute("UPDATE resources SET next_attempt=0 WHERE state IN ('RETRY','ACK_PENDING')")

    def test_file_resources_map_several_roots_and_preserve_nested_directories(self):
        ready = self.source(sid='shots', key='hero/600', name='ready.bin', data=b'ready',
                            subdir_glob='*/*', resource_mode='file', delete_after_ack=True)
        second = self.source(sid='shots', key='hero/600', name='second.bin', data=b'second')
        future = self.source(sid='shots', key='hero/600', name='future.bin', data=b'waiting')
        temporary = self.source(sid='shots', key='hero/600', name='working.tmp.bin')
        timestamp = future.stat().st_mtime + 300
        os.utime(future, (timestamp, timestamp))
        direct = self.source(sid='direct', key='.', name='only.bin', data=b'direct',
                             subdir_glob='.', resource_mode='file', filenames=['only.bin'])
        unrelated = self.source(sid='direct', key='.', name='unrelated.bin')
        self.sources.append({'id':'later', 'root':str(self.root/'A-input'/'later'),
                             'subdir_glob':'.','resource_mode':'file','min_age_seconds':0})
        different = self.root/'different'
        nested = self.root/'nested'
        self.bconfig['paths']['source_output_dirs'] = {
            'shots':[str(nested)], 'direct':[str(different)], 'later':[str(self.root/'later-output')]}
        server, _, client = self.pair()

        self.assertEqual(server.scan()['missing_sources'], ['later'])
        self.assertEqual([(row['file_count'], row['state']) for row in server.store.rows()], [(1,'READY')] * 3)
        self.assertEqual(client.poll()['enqueued'], 3)
        while client.work_once():
            pass

        self.assertEqual((nested/'hero'/'600'/'ready.bin').read_bytes(), b'ready')
        self.assertEqual((nested/'hero'/'600'/'second.bin').read_bytes(), b'second')
        self.assertEqual((different/'only.bin').read_bytes(), b'direct')
        self.assertEqual(client.pending_count(), 0)
        self.assertEqual(server.status()['states']['ACKED'], 3)
        self.assertEqual(client.poll()['enqueued'], 0)
        for deleted in (ready, second):
            self.assertFalse(deleted.exists())
        for retained in (direct, unrelated, temporary, future):
            self.assertTrue(retained.exists())

    def test_directory_mapping_preserves_grouped_lease(self):
        first = self.source(key='hero/final', name='one.bin', data=b'first', subdir_glob='*/*',
                            resource_mode='directory', delete_after_ack=True)
        second = self.source(key='hero/final', name='two.bin', data=b'second')
        target = self.root/'directory-output'
        self.bconfig['paths']['source_output_dirs'] = {'s1':[str(target)]}
        server, _, client = self.pair()

        self.assertEqual(client.poll()['enqueued'], 1)
        self.assertEqual(server.store.rows()[0]['file_count'], 2)
        self.assertTrue(client.work_once())
        self.assertEqual((target/'hero'/'final'/'one.bin').read_bytes(), b'first')
        self.assertEqual((target/'hero'/'final'/'two.bin').read_bytes(), b'second')
        self.assertFalse(first.exists())
        self.assertFalse(second.exists())

    def test_ack_retry_rechecks_mapped_file_before_deletion(self):
        source = self.source(resource_mode='file', delete_after_ack=True)
        target = self.root/'verified-output'
        self.bconfig['paths']['source_output_dirs'] = {'s1':[str(target)]}
        server, transport, client = self.pair()
        self.assertEqual(client.poll()['enqueued'], 1)
        call = transport.call

        def interrupt_ack(request, binary=False):
            if request['op'] == 'ack':
                raise ConnectionError('ACK did not reach A')
            return call(request, binary)

        with patch.object(transport, 'call', side_effect=interrupt_ack):
            self.assertTrue(client.work_once())
        (target/'hero'/'payload.bin').unlink()
        self.ready_retry(client)
        self.assertTrue(client.work_once())
        self.assertEqual(client.rows()[0]['state'], 'HOLD')
        self.assertEqual(server.status()['states']['ACKED'], 0)
        self.assertTrue(source.is_file())

    def _drain_background(self, client):
        original_poll = client.poll
        original_wait = client.stop.wait
        def poll_until_done():
            result = original_poll()
            rows = client.rows()
            if rows and all(row['state'] == 'DONE' for row in rows):
                client.stop.set()
            return result
        timeout = threading.Timer(5, client.stop.set)
        timeout.daemon = True
        timeout.start()
        try:
            with patch.object(client, 'poll', poll_until_done), \
                    patch.object(type(client.policy), 'next_delay_seconds', return_value=0.01), \
                    patch.object(client.stop, 'wait', lambda seconds=None: original_wait(
                        min(seconds, 0.01) if seconds is not None else None)):
                client.run()
        finally:
            timeout.cancel()

    def test_worker_recovers_transient_next_transaction_error(self):
        self.source(key='first')
        self.source(key='second')
        _, transport, client = self.pair()
        client.poll()
        original_next = client._next
        failures = []
        def next_once():
            if not failures:
                failures.append(True)
                raise sqlite3.OperationalError('temporary database locked')
            return original_next()
        with patch.object(client, '_next', next_once):
            self._drain_background(client)
        self.assertEqual(len(failures), 1)
        self.assertEqual([row['state'] for row in client.rows()], ['DONE', 'DONE'])
        self.assertEqual(transport.count('ack'), 2)

    def test_worker_recovers_running_row_after_state_transaction_errors(self):
        self.source(key='first')
        self.source(key='second')
        _, transport, client = self.pair()
        client.poll()
        original_set = client._set
        failed_states = []
        def set_after_failure(rid, state, **values):
            if len(failed_states) < 2 and state in ('RUNNING', 'RETRY'):
                failed_states.append(state)
                self.assertEqual(next(row for row in client.rows() if row['resource_id'] == rid)['state'], 'RUNNING')
                raise sqlite3.OperationalError('temporary state transaction failure')
            return original_set(rid, state, **values)
        with patch.object(client, '_set', set_after_failure):
            self._drain_background(client)
        self.assertEqual(failed_states, ['RUNNING', 'RETRY'])
        self.assertEqual([row['state'] for row in client.rows()], ['DONE', 'DONE'])
        self.assertEqual(transport.count('ack'), 2)

    def test_worker_recovers_transient_status_write_error(self):
        self.source(key='first')
        self.source(key='second')
        _, transport, client = self.pair()
        client.poll()
        failures = []
        def status_write_once(path, value):
            if Path(path) == client.state / 'status.json' and \
                    threading.current_thread() is not threading.main_thread() and not failures:
                failures.append(True)
                raise PermissionError('temporary status-file sharing violation')
            return write_json(path, value)
        with patch('aas.resources.client.write_json', status_write_once):
            self._drain_background(client)
        self.assertEqual(len(failures), 1)
        self.assertEqual([row['state'] for row in client.rows()], ['DONE', 'DONE'])
        self.assertEqual(transport.count('ack'), 2)

    def output(self, sid='s1',key='hero',name='payload.bin',root=None):
        return (root or self.root/'output')/sid/key/name

    def test_one_poll_enqueues_all_roles_as_one_batch_and_worker_still_runs(self):
        for n in range(6):
            self.source(key='hero'+str(n))
        server,transport,client = self.pair()
        self.assertEqual(client.poll()['enqueued'],6)
        self.assertEqual(client.pending_batch_count(),1)
        catalogs = transport.count('catalog')
        self.assertFalse(client.poll()['queue_full'])
        self.assertEqual(transport.count('catalog'),catalogs+1)
        self.assertEqual(client.pending_batch_count(),1)
        self.assertTrue(client.work_once())
        self.assertEqual(sum(row['state']=='DONE' for row in client.rows()),1)
        self.assertGreater(transport.count('get'),0)
        self.assertEqual(client.pending_count(),5)
        self.assertEqual(client.poll()['enqueued'],0)
        self.assertEqual(client.pending_batch_count(),1)

    def test_queue_json_multiple_sources_keys_controls_order_and_output_layout(self):
        self.source('s1','heroA')
        self.source('s2','heroA')
        self.source('s2','heroB')
        queue = self.root/'local-jobs.json'
        self.bconfig['queue_file'] = str(queue)
        server,transport,client = self.pair()
        resources = sorted(server.catalog()['resources'], key=lambda row: row['resource_id'], reverse=True)
        jobs = [{'source_id': row['source_id'], 'key': row['key']} for row in resources]
        queue.write_text(json.dumps({'jobs':jobs}),encoding='utf-8')
        # Coarse clocks can give every row in one poll the same creation time.
        with patch('aas.resources.client.time.time', return_value=1234567890.0):
            self.assertEqual(client.poll()['enqueued'],3)
        client = ResourceClient(self.bconfig, transport=transport)
        expected_order = [(job['source_id'], job['key']) for job in jobs]
        self.assertEqual([(row['source_id'],row['resource_key']) for row in client.rows()], expected_order)
        while client.work_once():
            pass
        claims = [call['resource_id'] for call in transport.calls if call['op'] == 'claim']
        self.assertEqual(claims, [row['resource_id'] for row in resources])
        for sid,key in expected_order:
            self.assertEqual(self.output(sid,key).read_bytes(),b'0123456789'*8)

    def test_future_missing_directory_is_not_created_and_later_discovered(self):
        future = self.root/'A-future'
        self.sources.append({'id':'future','root':str(future),'subdir_glob':'*','suffixes':['*'],'min_age_seconds':0})
        server,transport,client = self.pair()
        self.assertEqual(client.poll()['enqueued'],0)
        self.assertFalse(future.exists())
        role = future/'later'
        role.mkdir(parents=True)
        (role/'later.xyz').write_bytes(b'custom-unrecognized-binary-format')
        self.assertEqual(client.poll()['enqueued'],1)
        self.assertTrue(client.work_once())
        self.assertEqual(self.output('future','later','later.xyz').read_bytes(),b'custom-unrecognized-binary-format')

    def test_resume_partial_and_restart_preserves_request_and_lease(self):
        self.source()
        server,transport,client = self.pair()
        client.poll()
        initial_request = client.rows()[0]['request_id']
        transport.interrupt_get_number = 2
        with self.assertRaises(KeyboardInterrupt):
            client.work_once()
        before = client.rows()[0]
        self.assertEqual(before['state'],'RUNNING')
        part = self.output().with_name('payload.bin.part')
        self.assertEqual(part.stat().st_size,7)
        second_transport = FakeTransport(ResourceServer(server.config))
        restarted = ResourceClient(self.bconfig,transport=second_transport)
        self.assertEqual(restarted.rows()[0]['state'],'RUNNING')  # Constructor does not reset another worker.
        restarted.run(once=True)
        self.assertEqual(restarted.rows()[0]['state'],'DONE')
        self.assertEqual(restarted.rows()[0]['request_id'],initial_request)
        self.assertEqual(restarted.rows()[0]['lease_token'],before['lease_token'])
        first_get = next(row for row in second_transport.calls if row['op']=='get')
        self.assertEqual(first_get['offset'],7)
        self.assertEqual(self.output().read_bytes(),b'0123456789'*8)

    def test_different_existing_output_never_overwritten_and_alternate_root_works(self):
        self.source()
        self.output().parent.mkdir(parents=True)
        self.output().write_bytes(b'user-owned-different-file')
        alternate = self.root/'output-two'
        self.bconfig['paths']['output_dirs'].append(str(alternate))
        server,transport,client = self.pair()
        client.run(once=True)
        self.assertEqual(self.output().read_bytes(),b'user-owned-different-file')
        self.assertEqual(self.output(root=alternate).read_bytes(),b'0123456789'*8)
        self.assertEqual(client.rows()[0]['state'],'DONE')

    def test_restart_rechecks_cached_output_directory_link_before_write_or_ack(self):
        source = self.source()
        server, transport, client = self.pair()
        client.poll()
        transport.interrupt_get_number = 2
        with self.assertRaises(KeyboardInterrupt):
            client.work_once()
        output_dir = self.output().parent
        saved = self.root / 'saved-original-output'
        outside = self.root / 'outside-configured-output'
        for path in (output_dir, saved, outside):
            self.assertTrue(path.resolve().is_relative_to(self.root))
        output_dir.rename(saved)
        outside.mkdir()
        linked = False
        try:
            if os.name == 'nt':
                # Junctions need no symlink privilege and exercise the real
                # Windows deployment boundary skipped by symlink-only tests.
                result = subprocess.run(
                    ['cmd', '/c', 'mklink', '/J', str(output_dir), str(outside)],
                    capture_output=True,
                    creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
                if result.returncode:
                    self.skipTest('Windows junction creation is unavailable')
            else:
                output_dir.symlink_to(outside, target_is_directory=True)
            linked = True
            resumed_transport = FakeTransport(server)
            restarted = ResourceClient(self.bconfig, transport=resumed_transport)
            restarted.run(once=True)
            self.assertEqual(restarted.rows()[0]['state'], 'HOLD')
            self.assertFalse((outside / 'payload.bin').exists())
            self.assertFalse((outside / 'payload.bin.part').exists())
            self.assertTrue(source.is_file())
            self.assertEqual(source.read_bytes(), b'0123456789' * 8)
            self.assertEqual(resumed_transport.count('ack'), 0)
            self.assertEqual((saved / 'payload.bin.part').stat().st_size, 7)
        finally:
            if linked:
                self.assertTrue(output_dir.absolute().is_relative_to(self.root))
                self.assertTrue(outside.resolve().is_relative_to(self.root))
                if os.name == 'nt':
                    os.rmdir(output_dir)  # Remove only the owned junction entry.
                else:
                    output_dir.unlink()

    def test_disk_insufficient_retries_without_range_reads_then_resumes(self):
        self.source()
        self.bconfig['capacity']['min_free_bytes'] = 100
        server,transport,client = self.pair()
        client.poll()
        with patch.object(shutil,'disk_usage',return_value=SimpleNamespace(free=0)):
            client.work_once()
        self.assertEqual(client.rows()[0]['state'],'RETRY')
        self.assertEqual(transport.count('get'),0)
        self.assertFalse(self.output().exists())
        self.assertTrue((self.root/'A-input/s1/hero/payload.bin').exists())
        self.ready_retry(client)
        client.work_once()
        self.assertEqual(client.rows()[0]['state'],'DONE')

    def test_different_existing_file_without_alternate_is_preserved_for_retry(self):
        self.source()
        self.output().parent.mkdir(parents=True)
        self.output().write_bytes(b'different-user-file')
        server,transport,client=self.pair()
        client.run(once=True)
        self.assertEqual(client.rows()[0]['state'],'RETRY')
        self.assertEqual(self.output().read_bytes(),b'different-user-file')
        self.assertEqual(transport.count('get'),0)
        self.assertTrue((self.root/'A-input/s1/hero/payload.bin').is_file())

    def test_remote_source_hold_is_terminal_locally_without_deleting_replacement(self):
        source=self.source()
        server,transport,client=self.pair()
        client.poll()
        source.write_bytes(b'new-producer-version')
        server.scan()
        client.work_once()
        self.assertEqual(client.rows()[0]['state'],'HOLD')
        self.assertEqual(source.read_bytes(),b'new-producer-version')
        self.assertEqual(transport.count('get'),0)

    def test_ack_lost_restart_uses_durable_receipt_and_never_redownloads_deleted_output(self):
        self.source()
        server,transport,client = self.pair()
        client.poll()
        transport.lose_ack = True
        client.work_once()
        self.assertEqual(client.rows()[0]['state'],'RETRY')
        self.assertEqual(server.status()['states']['ACKED'],1)
        rid = client.rows()[0]['resource_id']
        receipt = client.receipts/(rid+'.json')
        self.assertEqual(read_json(receipt)['status'],'DELIVERED')
        self.output().unlink()  # User deliberately removed their already-delivered output.
        second_transport = FakeTransport(ResourceServer(server.config))
        restarted = ResourceClient(self.bconfig,transport=second_transport)
        self.ready_retry(restarted)
        restarted.run(once=True)
        self.assertEqual(restarted.rows()[0]['state'],'DONE')
        self.assertEqual(second_transport.count('get'),0)
        self.assertFalse(self.output().exists())

    def test_expired_lease_at_restart_does_not_block_worker_reclaim(self):
        self.source()
        server,transport,client = self.pair()
        client.poll()
        transport.interrupt_get_number = 2
        with self.assertRaises(KeyboardInterrupt):
            client.work_once()
        old_token = client.rows()[0]['lease_token']
        original_clock = server._clock
        now = original_clock()
        server._clock = lambda:now+1300
        restarted = ResourceClient(self.bconfig,transport=FakeTransport(server))
        restarted.run(once=True)
        self.assertEqual(restarted.rows()[0]['state'],'DONE')
        self.assertNotEqual(restarted.rows()[0]['lease_token'],old_token)

    def test_completed_user_deleted_output_stays_deleted_on_next_poll(self):
        self.source()
        server,transport,client = self.pair()
        client.run(once=True)
        self.output().unlink()
        get_count = transport.count('get')
        restarted = ResourceClient(self.bconfig,transport=transport)
        restarted.run(once=True)
        self.assertFalse(self.output().exists())
        self.assertEqual(transport.count('get'),get_count)
        self.assertEqual(restarted.rows()[0]['state'],'DONE')

    def test_manifest_bad_digest_binding_unsafe_and_case_collision_rejected(self):
        for case in ('digest','binding','unsafe','case_collision','missing_size'):
            with self.subTest(case=case):
                # Independent state and inputs per malformed-manifest case.
                original_root = self.root
                self.root = original_root/case
                self.root.mkdir()
                self.sources = []
                self.bconfig['paths']['root_dir'] = str(self.root/'B')
                self.bconfig['paths']['output_dirs'] = [str(self.root/'output')]
                self.source()
                server,transport,client = self.pair()
                def mutate(manifest):
                    if case=='digest':
                        manifest['manifest_sha256']='0'*64
                        return
                    if case=='binding':
                        manifest['source_id']='wrong-source'
                    elif case=='unsafe':
                        manifest['files'][0]['name']='../outside.bin'
                    elif case=='missing_size':
                        manifest['files'][0].pop('size')
                    else:
                        extra = dict(manifest['files'][0])
                        extra['name']=extra['name'].upper()
                        manifest['files'].append(extra)
                        manifest['total_bytes']*=2
                    manifest['manifest_sha256']=hash_obj({k:v for k,v in manifest.items() if k!='manifest_sha256'})
                transport.mutate_claim = mutate
                client.run(once=True)
                self.assertEqual(client.rows()[0]['state'],'HOLD')
                self.assertEqual(transport.count('get'),0)
                self.assertEqual(transport.count('ack'),0)
                self.assertTrue((self.root/'A-input/s1/hero/payload.bin').exists())
                self.root = original_root

    def test_byte_corruption_retains_partial_and_never_acks_or_publishes(self):
        self.source()
        server,transport,client = self.pair()
        transport.corrupt_bytes = True
        client.run(once=True)
        self.assertEqual(client.rows()[0]['state'],'HOLD')
        self.assertTrue(self.output().with_name('payload.bin.part').exists())
        self.assertFalse(self.output().exists())
        self.assertEqual(transport.count('ack'),0)

    def test_config_bound_producer_is_sent_and_wrong_server_rejected(self):
        self.source()
        server,transport,client = self.pair()
        server.producer_id = 'wrong-producer'
        with self.assertRaises(RemoteError) as caught:
            client.poll()
        self.assertEqual(caught.exception.code,'AUTH')
        self.assertEqual(transport.calls[0].get('producer_id'),'A')

    def test_boolean_size_schema_rejected_before_any_download(self):
        self.source(data=b'x')
        server,transport,client = self.pair()
        def mutate(manifest):
            manifest['files'][0]['size']=True
            manifest['total_bytes']=True
            manifest['manifest_sha256']=hash_obj({k:v for k,v in manifest.items() if k!='manifest_sha256'})
        transport.mutate_claim = mutate
        client.run(once=True)
        self.assertEqual(client.rows()[0]['state'],'HOLD')
        self.assertEqual(transport.count('get'),0)

    def test_zero_byte_custom_file_delivers_as_empty_regular_file(self):
        self.source(data=b'')
        server,transport,client = self.pair()
        client.run(once=True)
        self.assertEqual(client.rows()[0]['state'],'DONE')
        self.assertEqual(self.output().read_bytes(),b'')

    def test_images_preset_and_lora_preset_use_same_manifest_range_protocol(self):
        from PIL import Image
        image_path = self.source(name='picture.png',data=b'placeholder',suffixes=['.png'],idle_flush_seconds=0)
        Image.new('RGB',(4,4),'green').save(image_path)
        server,transport,client = self.pair('images')
        client.run(once=True)
        self.assertEqual(client.rows()[0]['state'],'DONE')
        self.assertTrue(self.output(name='picture.png').exists())
        # Independent A/B states for the LoRA fixture, preserving image receipt.
        self.sources=[]
        self.bconfig['paths']['root_dir']=str(self.root/'B-lora')
        first=self.source(sid='lora',name='hero_model-step00000100.safetensors',data=b'early-100',suffixes=['.safetensors'])
        directory=first.parent
        for step in range(200,1501,100):
            (directory/f'hero_model-step{step:08d}.safetensors').write_bytes(f'weight-{step}'.encode())
        (directory/'hero_model.safetensors').write_bytes(b'final-weight')
        (directory/'TRAIN_COMPLETE.json').write_text('{"max_train_steps":1500}')
        server,transport,client=self.pair('lora')
        client.run(once=True)
        self.assertEqual(client.rows()[0]['state'],'DONE')
        receipt=read_json(next(client.receipts.glob('*.json')))
        self.assertEqual(len(receipt['files']),11)
        self.assertTrue((directory/'hero_model-step00000100.safetensors').exists())
        self.assertFalse((directory/'hero_model-step00000600.safetensors').exists())


if __name__=='__main__':
    unittest.main()
