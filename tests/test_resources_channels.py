"""Byte-bounded batches and independent download channels; synthetic local files only."""
from concurrent.futures import ThreadPoolExecutor
import threading
import unittest

from test_resources_batches import ResourceBatchTests as BatchFixture


class ResourceChannelTests(unittest.TestCase):
    setUp=BatchFixture.setUp
    tearDown=BatchFixture.tearDown
    source=BatchFixture.source
    client=BatchFixture.client

    def test_size_bounded_groups_and_capacity(self):
        self.config['transfer'].update(target_batch_bytes=90, download_workers=4)
        for n in range(11):self.source('role'+str(n))
        client,transport=self.client()
        result=client.poll()
        self.assertEqual((result['enqueued_batches'],result['enqueued']),(4,8))
        self.assertEqual([b['resource_count'] for b in client.batch_rows()],[2]*4)
        catalogs=transport.count('catalog')
        self.assertTrue(client.poll()['queue_full'])
        self.assertEqual(transport.count('catalog'),catalogs)
        self.assertEqual(client.pending_batch_count(),4)

    def test_large_indivisible_resource_has_its_own_batch(self):
        self.config['transfer']['target_batch_bytes']=30
        for n in range(3):self.source('role'+str(n))
        client,_=self.client()
        self.assertEqual(client.poll()['enqueued_batches'],3)
        self.assertEqual([b['resource_count'] for b in client.batch_rows()],[1]*3)

    def test_four_batches_transfer_concurrently_without_duplicate_gets_or_acks(self):
        self.config['transfer'].update(target_batch_bytes=40,download_workers=4)
        for n in range(4):self.source('role'+str(n))
        client,transport=self.client();client.poll()
        original=transport.call;barrier=threading.Barrier(4);seen=set();lock=threading.Lock()
        def overlap(request,binary=False):
            first=False
            if request['op']=='get':
                with lock:
                    if request['resource_id'] not in seen:seen.add(request['resource_id']);first=True
                if first:barrier.wait(timeout=10)
            return original(request,binary)
        transport.call=overlap
        with ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(list(pool.map(lambda _:client.work_once(),range(4))),[True]*4)
        self.assertEqual(len(seen),4)
        self.assertEqual(transport.count('ack'),4)
        self.assertTrue(all(r['state']=='DONE' for r in client.rows()))
        self.assertEqual(len(list((client.state/'progress').glob('*.json'))),4)

    def test_ack_inflight_is_not_selected_by_another_channel(self):
        self.config['transfer'].update(target_batch_bytes=40,download_workers=2)
        for n in range(2):self.source('role'+str(n))
        client,transport=self.client();client.poll()
        original=transport.call;entered=threading.Event();release=threading.Event();first=client.rows()[0]['resource_id']
        def delayed_ack(request,binary=False):
            if request['op']=='ack' and request['resource_id']==first:
                entered.set();self.assertTrue(release.wait(10))
            return original(request,binary)
        transport.call=delayed_ack
        with ThreadPoolExecutor(max_workers=2) as pool:
            future=pool.submit(client.work_once)
            self.assertTrue(entered.wait(10))
            self.assertTrue(client.work_once())
            self.assertEqual(transport.count('claim'),2)
            release.set();self.assertTrue(future.result(timeout=10))
        self.assertEqual(transport.count('ack'),2)

    def test_recovery_does_not_reset_another_live_worker(self):
        self.config['transfer'].update(target_batch_bytes=40,download_workers=2)
        for n in range(2):self.source('role'+str(n))
        client,_=self.client();client.poll()
        first=client._next();second=client._next()
        with client.claim_lock:
            client.inflight.pop(first['resource_id']);client.failed_workers.add(first['resource_id'])
        client._recover_failed_workers()
        states={r['resource_id']:r['state'] for r in client.rows()}
        self.assertEqual(states[first['resource_id']],'RETRY')
        self.assertEqual(states[second['resource_id']],'RUNNING')

    def test_superseded_unclaimed_resource_skips_without_ack(self):
        self.source('hero');client,transport=self.client();client.poll()
        original=transport.call
        def superseded(request,binary=False):
            if request['op']=='claim':return {'status':'superseded'}
            return original(request,binary)
        transport.call=superseded
        self.assertTrue(client.work_once())
        self.assertEqual(client.rows()[0]['state'],'SKIPPED')
        self.assertEqual(transport.count('get'),0)
        self.assertEqual(transport.count('ack'),0)


del BatchFixture


if __name__=='__main__':unittest.main()
