"""Real filesystem/SQLite concurrency, fencing, crash and exact-deletion tests."""
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
import json
import multiprocessing
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from aas.common.core import hash_obj
from aas.resources.server import MAX_RANGE, ResourceError, ResourceServer


def _claim_worker(config, rid, number):
    return ResourceServer(config).claim(rid, 'B', 'worker_' + str(number))['status']


class ResourceServerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='aas-resource-test-')
        self.root = Path(self.temp.name).resolve()
        self.data = self.root / 'incoming'
        self.role = self.data / 'hero'
        self.role.mkdir(parents=True)
        self.config = {'node': {'producer_id':'A', 'consumer_id':'B'},
            'paths': {'root_dir': str(self.root / 'state')}, 'lease': {'seconds':1200},
            'sources': [{'id':'files', 'root':str(self.data), 'subdir_glob':'*',
                         'suffixes':['.bin'], 'min_age_seconds':0}]}

    def tearDown(self):
        # The exclusively generated fixture root is checked before recursive cleanup.
        self.assertTrue(self.root.name.startswith('aas-resource-test-'))
        self.temp.cleanup()

    def files(self, names=('one.bin','two.bin')):
        for name in names:
            (self.role / name).write_bytes((name + ':').encode() * 20)

    def server_claim(self):
        server = ResourceServer(self.config)
        rid = server.catalog()['resources'][0]['resource_id']
        claim = server.claim(rid, 'B', 'first')
        receipt = {k:deepcopy(claim['manifest'][k]) for k in ('manifest_sha256','files')}
        return server, rid, claim, receipt

    def test_claim_idempotent_and_preparing_does_not_hold_database_lock(self):
        self.files()
        server = ResourceServer(self.config)
        rid = server.catalog()['resources'][0]['resource_id']
        entered, release = threading.Event(), threading.Event()
        original = server._hash

        def blocked(*args, **kwargs):
            entered.set()
            self.assertTrue(release.wait(10))
            return original(*args, **kwargs)

        with patch.object(server, '_hash', side_effect=blocked), ThreadPoolExecutor(1) as pool:
            future = pool.submit(server.claim, rid, 'B', 'same_request')
            self.assertTrue(entered.wait(10))
            other = ResourceServer(self.config)
            self.assertEqual(other.claim(rid, 'B', 'different')['status'], 'busy')
            preparing = other.claim(rid, 'B', 'same_request')
            self.assertEqual(preparing['status'], 'busy')
            self.assertEqual(other.status()['states']['PREPARING'], 1)
            release.set()
            first = future.result(10)
        with patch.object(other, '_hash', side_effect=AssertionError('Manifest must be cached')):
            again = other.claim(rid, 'B', 'same_request')
        self.assertEqual(first['lease_token'], again['lease_token'])
        self.assertEqual(first['lease_token'], preparing['lease_token'])
        manifest = dict(first['manifest'])
        digest = manifest.pop('manifest_sha256')
        self.assertEqual(hash_obj(manifest), digest)

    def test_multiprocess_exactly_one_claim(self):
        self.files()
        server = ResourceServer(self.config)
        rid = server.catalog()['resources'][0]['resource_id']
        context = multiprocessing.get_context('spawn')
        with ProcessPoolExecutor(4, mp_context=context) as pool:
            futures = [pool.submit(_claim_worker, self.config, rid, n) for n in range(8)]
            statuses = [future.result(30) for future in futures]
        self.assertEqual(statuses.count('claimed'), 1)
        self.assertEqual(statuses.count('busy'), 7)

    def test_expiration_fences_old_token_and_heartbeat_renews(self):
        self.files()
        server, rid, first, receipt = self.server_claim()
        now = time.time()
        server._clock = lambda:now + 1300
        second = server.claim(rid, 'B', 'second')
        self.assertNotEqual(first['lease_token'], second['lease_token'])
        with self.assertRaises(ResourceError):
            server.get_range(rid, 'B', first['lease_token'], 'one.bin', 0, 4)
        with self.assertRaises(ResourceError):
            server.ack(rid, 'B', first['lease_token'], receipt)
        expiry = server.heartbeat(rid, 'B', second['lease_token'])['lease_expires']
        server._clock = lambda:now + 2400
        self.assertGreater(server.heartbeat(rid, 'B', second['lease_token'])['lease_expires'], expiry)

    def test_append_during_lease_waits_for_next_resource_and_ack_is_idempotent(self):
        self.files(('one.bin',))
        server, rid, claim, receipt = self.server_claim()
        self.files(('later.bin',))
        self.assertEqual(server.catalog()['resources'][0]['state'], 'LEASED')
        self.assertEqual(server.get_range(rid, 'B', claim['lease_token'], 'one.bin', 0, 4), b'one.')
        self.assertEqual(server.ack(rid, 'B', claim['lease_token'], receipt)['status'], 'acked')
        self.assertEqual(server.ack(rid, 'B', claim['lease_token'], receipt)['status'], 'already_acked')
        self.assertFalse((self.role / 'one.bin').exists())
        self.assertTrue((self.role / 'later.bin').is_file())
        resources = server.catalog()['resources']
        self.assertEqual(len(resources), 1)
        self.assertNotEqual(resources[0]['resource_id'], rid)

    def test_wrong_ack_and_mutation_never_delete_source(self):
        self.files()
        server, rid, claim, receipt = self.server_claim()
        wrong = deepcopy(receipt)
        wrong['files'][0]['sha256'] = '0' * 64
        with self.assertRaises(ResourceError):
            server.ack(rid, 'B', claim['lease_token'], wrong)
        self.assertTrue((self.role / 'one.bin').exists())
        (self.role / 'two.bin').write_bytes(b'NEW-VERSION')
        with self.assertRaises(ResourceError):
            server.ack(rid, 'B', claim['lease_token'], receipt)
        self.assertTrue((self.role / 'one.bin').exists())
        self.assertEqual((self.role / 'two.bin').read_bytes(), b'NEW-VERSION')
        self.assertEqual(server.status()['states']['HOLD'], 1)

    def test_concurrent_ack_is_serialized_and_repeated_ack_succeeds(self):
        self.files()
        server, rid, claim, receipt = self.server_claim()
        other = ResourceServer(self.config)
        barrier = threading.Barrier(2)
        def acknowledge(instance):
            barrier.wait(5)
            return instance.ack(rid,'B',claim['lease_token'],receipt)['status']
        with ThreadPoolExecutor(2) as pool:
            statuses = list(pool.map(acknowledge,(server,other)))
        self.assertEqual(sorted(statuses),['acked','already_acked'])

    def test_full_preflight_detects_second_file_change_before_deleting_any(self):
        self.files()
        server, rid, claim, receipt = self.server_claim()
        original = server._hash
        def change_second(row,item,*args,**kwargs):
            if item['name']=='two.bin':
                (self.role/'two.bin').write_bytes(b'replacement-version')
            return original(row,item,*args,**kwargs)
        with patch.object(server,'_hash',side_effect=change_second):
            with self.assertRaises(ResourceError):
                server.ack(rid,'B',claim['lease_token'],receipt)
        self.assertTrue((self.role/'one.bin').is_file())
        self.assertEqual((self.role/'two.bin').read_bytes(),b'replacement-version')

    def test_crash_after_capture_recovers_ack_without_reclaiming_or_new_copy(self):
        self.files()
        server, rid, claim, receipt = self.server_claim()
        original_unlink = Path.unlink
        fired = []

        def crash(path, *args, **kwargs):
            if path.parent.name.startswith('.aas-ack-') and not fired:
                fired.append(True)
                raise OSError('simulated process crash after journal/capture')
            return original_unlink(path, *args, **kwargs)

        with patch.object(Path, 'unlink', crash):
            with self.assertRaises(OSError):
                server.ack(rid, 'B', claim['lease_token'], receipt)
        self.assertEqual(server.status()['states']['ACK_PENDING'], 1)
        restarted = ResourceServer(self.config)
        restarted.scan()
        self.assertEqual(restarted.status()['states']['ACKED'], 1)
        self.assertFalse((self.role / 'one.bin').exists())
        self.assertFalse((self.role / 'two.bin').exists())
        self.assertEqual(restarted.ack(rid, 'B', claim['lease_token'], receipt)['status'], 'already_acked')
        self.assertEqual(restarted.catalog()['resources'], [])

    def test_new_same_name_after_partial_ack_is_preserved_and_held(self):
        self.files()
        server, rid, claim, receipt = self.server_claim()
        original_unlink = Path.unlink
        def crash(path, *args, **kwargs):
            if path.parent.name.startswith('.aas-ack-'):
                raise OSError('crash')
            return original_unlink(path, *args, **kwargs)
        with patch.object(Path, 'unlink', crash):
            with self.assertRaises(OSError):
                server.ack(rid, 'B', claim['lease_token'], receipt)
        missing = next(name for name in ('one.bin','two.bin') if not (self.role / name).exists())
        (self.role / missing).write_bytes(b'do-not-delete-new-version')
        ResourceServer(self.config).scan()
        self.assertEqual(server.status()['states']['HOLD'], 1)
        self.assertEqual((self.role / missing).read_bytes(), b'do-not-delete-new-version')

    def test_missing_future_sources_multiple_roots_and_range_validation(self):
        self.files()
        future = self.root / 'not-created'
        second = self.root / 'another'
        second.mkdir()
        (second / 'one.bin').write_bytes(b'generic')
        self.config['sources'] += [
            {'id':'future','root':str(future),'subdir_glob':'*','suffixes':['*']},
            {'id':'second','root':str(second),'subdir_glob':'.','suffixes':['*'],'min_age_seconds':0}]
        server = ResourceServer(self.config)
        catalog = server.catalog()
        self.assertEqual(catalog['missing_sources'], ['future'])
        self.assertFalse(future.exists())
        self.assertEqual(len(catalog['resources']), 2)
        rid = next(r['resource_id'] for r in catalog['resources'] if r['source_id']=='files')
        claim = server.claim(rid, 'B', 'request')
        for name, offset, length in [('../outside',0,1), ('one.bin',-1,1), ('one.bin',0,MAX_RANGE+1)]:
            self.assertEqual(server.dispatch({'op':'get','resource_id':rid,'lease_token':claim['lease_token'],'name':name,'offset':offset,'length':length})['status'], 'error')
        self.assertEqual(server.dispatch({'op':'claim','resource_id':rid,'consumer_id':'evil','request_id':'x'})['code'], 'AUTH')
        self.assertEqual(server.dispatch({'op':'ack','resource_id':'../outside','lease_token':'x','receipt':{}})['status'], 'error')

    def lora(self, cleanup=False, omit=None):
        for step in range(100,1501,100):
            if step != omit:
                (self.role / f'hero_model-step{step:08d}.safetensors').write_bytes(f'weight{step}'.encode())
        (self.role / 'hero_model.safetensors').write_bytes(b'final')
        (self.role / 'TRAIN_COMPLETE.json').write_text(json.dumps({'max_train_steps':1500}))
        (self.role / 'other_model-step00000100.safetensors').write_bytes(b'not-this-role')
        (self.role / 'base.safetensors').write_bytes(b'not-a-checkpoint')
        (self.role / 'training.log').write_bytes(b'keep-log')
        source = self.config['sources'][0]
        source.update(suffixes=['.safetensors'], completion_marker='TRAIN_COMPLETE.json', checkpoint_min_step=600,
                      include_final=True, cleanup={'enabled':cleanup,'checkpoint_steps':[100,200,300,400,500]})

    def test_lora_requires_all11_and_cleanup_only_explicit_early_steps(self):
        self.lora(cleanup=True, omit=900)
        server = ResourceServer(self.config)
        self.assertEqual(server.catalog()['resources'], [])
        self.assertTrue((self.role / 'hero_model-step00000100.safetensors').exists())
        (self.role / 'hero_model-step00000900.safetensors').write_bytes(b'weight900')
        summary = server.scan()
        self.assertEqual(summary['cleaned'], 5)
        rid = server.catalog()['resources'][0]['resource_id']
        claim = server.claim(rid,'B','request')
        self.assertEqual(len(claim['manifest']['files']),11)
        self.assertTrue((self.role / 'other_model-step00000100.safetensors').exists())
        self.assertTrue((self.role / 'base.safetensors').exists())
        self.assertTrue((self.role / 'training.log').exists())

    def test_cleanup_default_disabled_and_no_marker_never_deletes(self):
        self.lora()
        server = ResourceServer(self.config)
        self.assertEqual(server.scan()['cleaned'], 0)
        self.config['sources'][0]['cleanup']['enabled'] = True
        (self.role / 'TRAIN_COMPLETE.json').unlink()
        self.assertTrue(all(row['state']=='HOLD' for row in ResourceServer(self.config).catalog()['resources']))
        self.assertTrue((self.role / 'hero_model-step00000100.safetensors').exists())

    def test_cleanup_configuration_rejects_truthy_strings_and_nonexact_steps_without_deletion(self):
        self.lora()
        before={p.name:p.read_bytes() for p in self.role.iterdir() if p.is_file()}
        bad=[None,False,'false',[], {'enabled':'false','checkpoint_steps':[100]},
             {'enabled':0,'checkpoint_steps':[100]}, {'enabled':1,'checkpoint_steps':[100]},
             {'enabled':True,'checkpoint_steps':[True]}, {'enabled':True,'checkpoint_steps':[100.0]},
             {'enabled':True,'checkpoint_steps':['100']}, {'enabled':True,'checkpoint_steps':[600]},
             {'enabled':True,'checkpoint_steps':[100,100]}, {'enabled':True,'checkpoint_steps':None},
             {'enabled':True,'checkpoint_steps':[]}, {'enabled':False,'checkpoint_steps':[False]}]
        for cleanup in bad:
            with self.subTest(cleanup=cleanup):
                config=deepcopy(self.config)
                config['sources'][0]['cleanup']=cleanup
                with self.assertRaises(ValueError):
                    ResourceServer(config)
                self.assertEqual({p.name:p.read_bytes() for p in self.role.iterdir() if p.is_file()},before)
        config=deepcopy(self.config)
        config['sources'][0].update(checkpoint_min_step=100,cleanup={'enabled':True,'checkpoint_steps':[100,200]})
        with self.assertRaises(ValueError):
            ResourceServer(config)
        self.assertEqual({p.name:p.read_bytes() for p in self.role.iterdir() if p.is_file()},before)

    def test_nonfinite_numeric_and_numeric_roots_rejected_before_state_creation(self):
        for value in (float('nan'),float('inf'),float('-inf'),0,-1,True,'nan'):
            with self.subTest(lease=value):
                config=deepcopy(self.config)
                config['lease']['seconds']=value
                with self.assertRaises(ValueError):
                    ResourceServer(config)
        for field in ('min_age_seconds','idle_flush_seconds'):
            for value in (float('nan'),float('inf'),True,-1):
                config=deepcopy(self.config)
                config['sources'][0][field]=value
                with self.assertRaises(ValueError):
                    ResourceServer(config)
        for value in (float('nan'),float('inf'),17,False,''):
            for section in ('state','source'):
                config=deepcopy(self.config)
                if section=='state':
                    config['paths']['root_dir']=value
                else:
                    config['sources'][0]['root']=value
                with self.assertRaises(ValueError):
                    ResourceServer(config)
        self.assertFalse((self.root/'state').exists())

    def test_early_cleanup_capture_crash_restarts_with_stable_name_and_journal(self):
        self.lora(cleanup=True)
        server=ResourceServer(self.config)
        original=Path.unlink
        class Crash(BaseException):
            pass
        def crash_after_capture(path,*args,**kwargs):
            if path.parent.name.startswith('.aas-clean-'):
                raise Crash('process died after capture and before unlink')
            return original(path,*args,**kwargs)
        with patch.object(Path,'unlink',crash_after_capture):
            with self.assertRaises(Crash):
                server.scan()
        private=next(self.role.glob('.aas-clean-*'))
        self.assertEqual([p.name for p in private.iterdir()],['hero_model-step00000100.safetensors'])
        self.assertFalse((self.role/'hero_model-step00000100.safetensors').exists())
        restarted=ResourceServer(self.config)
        self.assertEqual(len(restarted.catalog()['resources']),1)
        self.assertEqual(len(restarted.catalog()['resources']),1)
        self.assertFalse(private.exists())
        self.assertEqual(restarted.status()['early_cleanup'].get('DONE'),5)
        for step in (100,200,300,400,500):
            self.assertFalse((self.role/f'hero_model-step{step:08d}.safetensors').exists())
        self.assertTrue((self.role/'hero_model-step00000600.safetensors').exists())

    def test_early_cleanup_crash_after_unlink_replays_and_recovers_after_main_ack(self):
        self.lora(cleanup=True)
        server=ResourceServer(self.config)
        original=server._cleanup_record
        class Crash(BaseException):
            pass
        def crash_before_done(rid,name,state,reason=None):
            if state=='DONE':
                raise Crash('process died after unlink before journal update')
            return original(rid,name,state,reason)
        with patch.object(server,'_cleanup_record',side_effect=crash_before_done):
            with self.assertRaises(Crash):
                server.scan()
        self.assertFalse((self.role/'hero_model-step00000100.safetensors').exists())
        rid=server.store.rows()[0]['resource_id']
        claim=server.claim(rid,'B','after_crash')
        receipt={k:claim['manifest'][k] for k in ('manifest_sha256','files')}
        server.ack(rid,'B',claim['lease_token'],receipt)
        self.assertFalse((self.role/'hero_model-step00000600.safetensors').exists())
        restarted=ResourceServer(self.config)
        self.assertEqual(restarted.catalog()['resources'],[])
        self.assertEqual(restarted.status()['early_cleanup'].get('DONE'),5)
        self.assertFalse(list(self.role.glob('.aas-clean-*')))
        self.assertTrue((self.role/'base.safetensors').exists())

    def test_legacy_numeric_cleanup_capture_preserved_without_blocking_catalog(self):
        self.lora(cleanup=True)
        server=ResourceServer(self.config)
        source=server.sources['files']
        snapshot,_=server._snapshot(source,self.role)
        rid=hash_obj({'source_id':'files','key':'hero','snapshot':snapshot})
        private=self.role/('.aas-clean-'+rid[:24])
        private.mkdir()
        (self.role/'hero_model-step00000100.safetensors').rename(private/'0')
        for _ in range(2):
            self.assertEqual(len(ResourceServer(self.config).catalog()['resources']),1)
        self.assertEqual((private/'0').read_bytes(),b'weight100')
        self.assertFalse((self.role/'hero_model-step00000200.safetensors').exists())
        self.assertGreaterEqual(server.status()['early_cleanup'].get('HOLD',0),1)

    def test_cleanup_io_failure_isolated_and_same_name_replacement_preserved(self):
        self.lora(cleanup=True)
        second=self.root/'other-source';second.mkdir()
        (second/'safe.bin').write_bytes(b'other-source')
        self.config['sources'].append({'id':'other','root':str(second),'subdir_glob':'.','suffixes':['.bin'],'min_age_seconds':0})
        server=ResourceServer(self.config)
        original=Path.unlink
        def fail(path,*args,**kwargs):
            if path.parent.name.startswith('.aas-clean-') and path.name.endswith('00000100.safetensors'):
                raise OSError('temporary cleanup I/O failure')
            return original(path,*args,**kwargs)
        with patch.object(Path,'unlink',fail):
            catalog=server.catalog()
        self.assertEqual(len(catalog['resources']),2)
        (self.role/'hero_model-step00000100.safetensors').write_bytes(b'new-replacement-must-survive')
        restarted=ResourceServer(self.config)
        self.assertEqual(len(restarted.catalog()['resources']),2)
        self.assertEqual((self.role/'hero_model-step00000100.safetensors').read_bytes(),b'new-replacement-must-survive')
        private=next(self.role.glob('.aas-clean-*'))
        self.assertEqual((private/'hero_model-step00000100.safetensors').read_bytes(),b'weight100')
        self.assertGreaterEqual(restarted.status()['early_cleanup'].get('HOLD',0),1)
        self.assertEqual((second/'safe.bin').read_bytes(),b'other-source')

    def test_image_verify_n_plus_one_and_idle_flush(self):
        from PIL import Image
        source = self.config['sources'][0]
        source.update(root=str(self.role),subdir_glob='.',suffixes=['.png'],verify_images=True,
                      hold_back_newest=1,idle_flush_seconds=300)
        Image.new('RGB',(8,8),'red').save(self.role / 'one.png')
        server = ResourceServer(self.config)
        self.assertEqual(server.catalog()['resources'], [])
        Image.new('RGB',(8,8),'blue').save(self.role / 'two.png')
        rid = server.catalog()['resources'][0]['resource_id']
        claim = server.claim(rid,'B','first')
        self.assertEqual(len(claim['manifest']['files']),1)
        receipt = {k:claim['manifest'][k] for k in ('manifest_sha256','files')}
        server.ack(rid,'B',claim['lease_token'],receipt)
        server._clock = lambda:time.time()+301
        self.assertEqual(len(server.catalog()['resources']),1)

    def test_invalid_image_held_and_symlink_rejected(self):
        self.config['sources'][0].update(suffixes=['.png'],verify_images=True)
        (self.role / 'bad.png').write_bytes(b'not-image')
        server = ResourceServer(self.config)
        rid = server.catalog()['resources'][0]['resource_id']
        with self.assertRaises(ResourceError):
            server.claim(rid,'B','first')
        self.assertEqual(server.status()['states']['HOLD'],1)
        (self.role / 'bad.png').unlink()
        outside = self.root / 'outside.png'
        outside.write_bytes(b'do-not-read')
        try:
            (self.role / 'link.png').symlink_to(outside)
        except OSError:
            self.skipTest('Symlink creation unavailable')
        self.assertGreaterEqual(server.scan()['held'],1)
        self.assertEqual(outside.read_bytes(),b'do-not-read')

    def test_optional_file_shards_are_stable_bounded_and_do_not_hash_during_scan(self):
        for index in range(5):
            (self.role/f'{index}.bin').write_bytes(b'x'*7)
        (self.role/'oversized.bin').write_bytes(b'y'*30)
        self.config['sources'][0]['target_resource_bytes']=14
        server=ResourceServer(self.config)
        with patch.object(server,'_hash',side_effect=AssertionError('scan must not hash file bytes')):
            first=server.catalog()['resources']
            self.assertEqual(len(first),4)
            self.assertEqual(sorted(r['total_bytes'] for r in first),[7,14,14,30])
            for _ in range(3):
                self.assertEqual(server.catalog()['resources'],first)
        restarted=ResourceServer(self.config)
        self.assertEqual(restarted.catalog()['resources'],first)
        names=[]
        for index,row in enumerate(first):
            claim=restarted.claim(row['resource_id'],'B',f'group_{index}')
            self.assertEqual(claim['manifest']['key'],'hero')
            names.extend(f['name'] for f in claim['manifest']['files'])
            self.assertTrue(row['total_bytes']<=14 or row['file_count']==1)
        self.assertEqual(len(names),len(set(names)))
        self.assertEqual(set(names),{f'{i}.bin' for i in range(5)}|{'oversized.bin'})

    def test_shard_config_is_exact_nonnegative_integer_and_zero_preserves_legacy(self):
        self.files()
        for bad in (True,False,-1,1.5,'1024',None,float('nan'),float('inf')):
            config=deepcopy(self.config);config['sources'][0]['target_resource_bytes']=bad
            with self.subTest(value=bad),self.assertRaises(ValueError):
                ResourceServer(config)
        self.config['sources'][0]['target_resource_bytes']=0
        self.assertEqual(len(ResourceServer(self.config).catalog()['resources']),1)

    def test_unclaimed_legacy_resource_is_superseded_without_leasing_old_id(self):
        self.files(('a.bin','b.bin','c.bin','d.bin','e.bin'))
        old=ResourceServer(self.config);old_id=old.catalog()['resources'][0]['resource_id']
        # Recreate the previous on-disk schema's single-active-key constraint.
        with old.store.transaction() as db:
            db.execute("CREATE UNIQUE INDEX one_active_source_key ON resources(source_id,resource_key) WHERE state != 'ACKED'")
        self.config['sources'][0]['target_resource_bytes']=240
        server=ResourceServer(self.config);resources=server.catalog()['resources']
        self.assertEqual(len(resources),3)
        self.assertNotIn(old_id,[r['resource_id'] for r in resources])
        result=server.claim(old_id,'B','old_queued_request')
        self.assertEqual(result['status'],'superseded')
        self.assertNotIn('lease_token',result)
        self.assertEqual(server.store.one(old_id)['state'],'SUPERSEDED')
        self.assertIsNone(server.store.one(old_id)['manifest'])
        self.assertEqual(server.catalog()['resources'],resources)

    def test_switching_root_to_file_mode_does_not_duplicate_the_existing_lease(self):
        source = self.config['sources'][0]
        source.update(subdir_glob='.', delete_after_ack=False)
        path = self.data/'one.bin'
        path.write_bytes(b'leased')
        stamp = time.time() - 1
        os.utime(path, (stamp, stamp))
        _, rid, claim, receipt = self.server_claim()

        source['resource_mode'] = 'file'
        server = ResourceServer(self.config)
        self.assertEqual([row['resource_id'] for row in server.catalog()['resources']], [rid])
        self.assertEqual(server.ack(rid, 'B', claim['lease_token'], receipt)['status'], 'acked')
        self.assertTrue(path.exists())
        self.assertEqual(server.catalog()['resources'], [])

        path.write_bytes(b'an updated version')
        os.utime(path, (stamp, stamp))
        later = server.catalog()['resources']
        self.assertEqual(len(later), 1)
        self.assertNotEqual(later[0]['resource_id'], rid)

    def test_preparing_and_expired_cached_manifest_pin_the_original_whole_resource(self):
        self.files()
        old=ResourceServer(self.config);rid=old.catalog()['resources'][0]['resource_id']
        entered,release=threading.Event(),threading.Event();original=old._hash
        def blocked(*args,**kwargs):
            entered.set();self.assertTrue(release.wait(10));return original(*args,**kwargs)
        config=deepcopy(self.config);config['sources'][0]['target_resource_bytes']=1
        with patch.object(old,'_hash',side_effect=blocked),ThreadPoolExecutor(1) as pool:
            future=pool.submit(old.claim,rid,'B','original_request')
            self.assertTrue(entered.wait(10))
            sharded=ResourceServer(config)
            self.assertEqual([r['resource_id'] for r in sharded.catalog()['resources']],[rid])
            release.set();first=future.result(10)
        # A persisted manifest, even after expiry/state reset, is never split.
        with old.store.transaction() as db:
            db.execute("UPDATE resources SET state='READY',lease_expires=0 WHERE resource_id=?",(rid,))
        self.assertEqual([r['resource_id'] for r in ResourceServer(config).catalog()['resources']],[rid])
        again=sharded.claim(rid,'B','resume_expired')
        self.assertEqual(again['manifest']['files'],first['manifest']['files'])
        self.assertEqual(len(sharded.store.rows()),1)

    def test_concurrent_scans_have_disjoint_groups_and_append_does_not_hold_lease(self):
        for index in range(7):(self.role/f'{index}.bin').write_bytes(bytes([index])*10)
        self.config['sources'][0]['target_resource_bytes']=20
        def catalog(_):return ResourceServer(self.config).catalog()['resources']
        with ThreadPoolExecutor(4) as pool:catalogs=list(pool.map(catalog,range(8)))
        self.assertTrue(all(rows==catalogs[0] for rows in catalogs))
        server=ResourceServer(self.config);rows=catalogs[0];self.assertEqual(len(rows),4)
        first=server.claim(rows[0]['resource_id'],'B','leased')
        (self.role/'appended.bin').write_bytes(b'later')
        after=server.catalog()['resources']
        self.assertEqual(len(after),5)
        self.assertEqual(server.store.one(rows[0]['resource_id'])['state'],'LEASED')
        self.assertEqual(server.manifest(rows[0]['resource_id'],'B',first['lease_token']),first['manifest'])
        ownership=[]
        for row in server.store.rows():
            if row['state'] not in ('ACKED','SUPERSEDED'):
                ownership += [f['name'] for f in json.loads(row['snapshot'])['files']]
        self.assertEqual(len(ownership),len(set(ownership)))
        self.assertEqual(server.catalog()['resources'],after)

    def test_one_shard_mutation_holds_only_that_group_and_ack_deletes_only_sibling(self):
        (self.role/'a.bin').write_bytes(b'a'*10);(self.role/'b.bin').write_bytes(b'b'*10)
        self.config['sources'][0]['target_resource_bytes']=10
        server=ResourceServer(self.config);rows=server.catalog()['resources']
        claims=[server.claim(row['resource_id'],'B',f'request_{i}') for i,row in enumerate(rows)]
        changed=claims[0]['manifest']['files'][0]['name'];safe=claims[1]['manifest']['files'][0]['name']
        (self.role/changed).write_bytes(b'new source must survive')
        server.catalog()
        self.assertEqual(server.store.one(rows[0]['resource_id'])['state'],'HOLD')
        receipt={k:claims[1]['manifest'][k] for k in ('files','manifest_sha256')}
        server.ack(rows[1]['resource_id'],'B',claims[1]['lease_token'],receipt)
        self.assertFalse((self.role/safe).exists())
        self.assertEqual((self.role/changed).read_bytes(),b'new source must survive')
        self.assertEqual(len(server.catalog()['resources']),1)

    def test_lora_shards_gate_full_completion_then_survive_partial_and_concurrent_ack(self):
        self.lora(cleanup=True,omit=900)
        self.config['sources'][0]['target_resource_bytes']=20
        server=ResourceServer(self.config)
        self.assertEqual(server.catalog()['resources'],[])
        self.assertTrue((self.role/'hero_model-step00000100.safetensors').exists())
        (self.role/'hero_model-step00000900.safetensors').write_bytes(b'weight900')
        rows=server.catalog()['resources'];self.assertGreater(len(rows),1)
        self.assertTrue(all(row['total_bytes']<=20 for row in rows))
        self.assertEqual(server.status()['early_cleanup']['DONE'],5)
        with closing(server.store.connect()) as db:
            self.assertEqual(db.execute('SELECT COUNT(*),COUNT(DISTINCT resource_id) FROM early_cleanup').fetchone()[:],(5,1))
        first=server.claim(rows[0]['resource_id'],'B','first_shard')
        first_names={f['name'] for f in first['manifest']['files']}
        receipt={k:first['manifest'][k] for k in ('files','manifest_sha256')}
        server.ack(rows[0]['resource_id'],'B',first['lease_token'],receipt)
        self.assertTrue((self.role/'TRAIN_COMPLETE.json').exists())
        for name in first_names:self.assertFalse((self.role/name).exists())
        # The role's full11 set is now incomplete solely because a shard ACKed.
        remaining=ResourceServer(self.config).catalog()['resources']
        self.assertEqual(len(remaining),len(rows)-1)
        self.assertTrue(all(row['state']=='READY' for row in remaining))
        self.assertEqual(server.status()['states']['HOLD'],0)
        barrier=threading.Barrier(len(remaining))
        def acknowledge(args):
            i,row=args;instance=ResourceServer(self.config)
            claim=instance.claim(row['resource_id'],'B',f'parallel_{i}')
            names={f['name'] for f in claim['manifest']['files']}
            self.assertFalse(names&first_names)
            receipt={k:claim['manifest'][k] for k in ('files','manifest_sha256')}
            barrier.wait(10)
            return instance.ack(row['resource_id'],'B',claim['lease_token'],receipt)['status']
        with ThreadPoolExecutor(len(remaining)) as pool:
            self.assertEqual(list(pool.map(acknowledge,enumerate(remaining))),['acked']*len(remaining))
        for _ in range(3):self.assertEqual(ResourceServer(self.config).catalog()['resources'],[])
        self.assertEqual(server.status()['states']['HOLD'],0)
        self.assertEqual(server.status()['early_cleanup']['DONE'],5)
        self.assertTrue((self.role/'base.safetensors').exists())
        self.assertTrue((self.role/'other_model-step00000100.safetensors').exists())
        self.assertTrue((self.role/'training.log').exists())

    def test_sharded_cleanup_crash_has_one_persistent_owner_and_ack_pending_is_not_split(self):
        self.lora(cleanup=True);self.config['sources'][0]['target_resource_bytes']=20
        server=ResourceServer(self.config);original=Path.unlink
        def fail_cleanup(path,*args,**kwargs):
            if path.parent.name.startswith('.aas-clean-') and path.name.endswith('00000100.safetensors'):
                raise OSError('temporary cleanup failure')
            return original(path,*args,**kwargs)
        fixed=time.time();server._clock=lambda:fixed
        with patch.object(Path,'unlink',fail_cleanup):
            initial=server.catalog()['resources']
            self.assertEqual(server.catalog()['resources'],initial)
        restarted=ResourceServer(self.config);restarted.catalog()
        with closing(restarted.store.connect()) as db:
            self.assertEqual(db.execute('SELECT COUNT(*),COUNT(DISTINCT resource_id) FROM early_cleanup').fetchone()[:],(5,1))
        self.assertEqual(restarted.status()['early_cleanup']['DONE'],5)
        self.assertFalse(list(self.role.glob('.aas-clean-*')))
        # An older whole resource ACK journal remains its sole file owner even
        # when the new grouping feature is enabled before crash recovery.
        other=self.root/'second';other.mkdir();(other/'one.bin').write_bytes(b'a'*20);(other/'two.bin').write_bytes(b'b'*20)
        config=deepcopy(self.config);config['paths']['root_dir']=str(self.root/'second-state');config['sources']=[{'id':'other','root':str(other),'subdir_glob':'.','suffixes':['.bin'],'min_age_seconds':0}]
        legacy=ResourceServer(config);rid=legacy.catalog()['resources'][0]['resource_id'];claim=legacy.claim(rid,'B','legacy_ack')
        receipt={k:claim['manifest'][k] for k in ('files','manifest_sha256')}
        def fail_ack(path,*args,**kwargs):
            if path.parent.name.startswith('.aas-ack-'):raise OSError('ACK crash')
            return original(path,*args,**kwargs)
        with patch.object(Path,'unlink',fail_ack):
            with self.assertRaises(OSError):legacy.ack(rid,'B',claim['lease_token'],receipt)
        config['sources'][0]['target_resource_bytes']=10
        with patch.object(Path,'unlink',fail_ack):
            self.assertEqual([r['resource_id'] for r in ResourceServer(config).catalog()['resources'] if r['source_id']=='other'],[rid])
        final=ResourceServer(config);self.assertEqual([r for r in final.catalog()['resources'] if r['source_id']=='other'],[])
        self.assertEqual(final.store.one(rid)['state'],'ACKED')

    def test_repartition_configuration_revert_uses_new_ids_without_reopening_superseded(self):
        for index in range(4):(self.role/f'{index}.bin').write_bytes(b'x'*6)
        source=self.config['sources'][0];source['target_resource_bytes']=12
        first=ResourceServer(self.config).catalog()['resources'];self.assertEqual(len(first),2)
        source['target_resource_bytes']=24
        second=ResourceServer(self.config).catalog()['resources'];self.assertEqual(len(second),1)
        source['target_resource_bytes']=12
        server=ResourceServer(self.config);third=server.catalog()['resources'];self.assertEqual(len(third),2)
        self.assertFalse({r['resource_id'] for r in first}&{r['resource_id'] for r in third})
        for row in first+second:self.assertEqual(server.claim(row['resource_id'],'B','stale')['status'],'superseded')
        self.assertEqual(server.catalog()['resources'],third)


if __name__ == '__main__':
    unittest.main()
