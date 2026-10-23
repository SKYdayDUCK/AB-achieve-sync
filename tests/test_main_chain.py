from __future__ import annotations
from dataclasses import replace
import json
from pathlib import Path
import shutil
import sqlite3
import threading
import sys
import time
import pytest
from aas.common.core import IntegrityError, ProtocolError, TemporaryError, HandoffUncertain, now_utc, process_lock, read_json, write_json, hash_obj, hash_file
from aas.b.core import BService
from aas.b.store import BStore
from aas.b.scheduling.policy import FixedIntervalPoll, PollContext, DiscoveryObservation, make_policy
from aas.a.core import AService
from aas.archive.tar import verify_tar
from aas.a.ssh_ctl import run_forced


def produced(a, generate, n=2, prefix='frame'):
    generate(n, prefix)
    bid = a.once()
    assert bid and a.db.get(bid)['state'] == 'READY'
    return bid


def test_p01_all_pending_first_scan_and_p02_fixed_schedule(node_factory):
    a,b,t,g = node_factory
    bids = [produced(a,g,1,str(i)) for i in range(3)]
    obs = b.discovery.scan()
    assert obs.newly_enqueued_batches == 3
    assert len(b.db.rows()) == 3
    policy = FixedIntervalPoll(300)
    assert policy.next_delay_seconds(PollContext(2, False, False)) == 300
    with pytest.raises(ProtocolError):
        make_policy('adaptive_research', 300)
    assert b.discovery.scan().newly_enqueued_batches == 0
    with b.db.tx() as con:
        kinds = list(con.execute("SELECT kind, observed FROM observations"))
    assert kinds[-1]['kind'] == 'success' and kinds[-1]['observed'] == 0


def test_end_to_end_and_user_modification_after_receipt(node_factory):
    a,b,t,g = node_factory
    bid = produced(a,g,3)
    assert b.once()['jobs'] == 1
    assert a.db.get(bid)['state'] == 'DELETED'
    assert b.db.state('gpuA',bid) == 'ACK_CONFIRMED'
    target = b.output/'gpuA'/bid
    assert len(list(target.iterdir())) == 3
    delivered = read_json(b.receipts/f'{bid}.json')
    assert delivered['receipt_status'] == 'DELIVERED'
    shutil.rmtree(target)
    b.once()
    assert b.db.state('gpuA',bid) == 'ACK_CONFIRMED'
    assert not target.exists()


def test_p04_p05_cursor_and_enqueue_atomically(node_factory,monkeypatch):
    a,b,t,g = node_factory
    bid = produced(a,g)
    page = a.list_ready()
    db = b.db
    orig = db.apply_page
    def before_tx(*args, **kwargs):
        raise KeyboardInterrupt('crash before page commit')
    monkeypatch.setattr(db,'apply_page',before_tx)
    with pytest.raises(KeyboardInterrupt):
        b.discovery.scan()
    assert not db.rows()
    assert db.cursor('gpuA',a.db.epoch())==0
    monkeypatch.setattr(db,'apply_page',orig)
    b.discovery.scan()
    assert len(db.rows())==1
    assert db.cursor('gpuA',a.db.epoch())==page['snapshot_high']
    b_again=BService(b.cfg,t)
    assert b_again.db.rows()[0]['state']=='DISCOVERED'
    assert b_again.db.cursor('gpuA',a.db.epoch())==page['snapshot_high']
    assert b_again.once()['jobs']==1
    assert a.db.get(bid)['state']=='DELETED'


def test_p06_crash_during_source_unlink_recovers_without_audit_key(node_factory):
    a,b,t,g = node_factory
    g(3)
    bid=a.claim()
    calls=[]
    orig_clean=a.clean_sources
    def crash_clean(batch_id):
        return orig_clean(batch_id,after_delete=lambda _: (_ for _ in ()).throw(KeyboardInterrupt('kill -9 simulation')))
    a.clean_sources=crash_clean
    with pytest.raises(KeyboardInterrupt):
        a.package(bid)
    a.clean_sources=orig_clean
    assert a.db.get(bid)['state']=='SOURCE_CLEANING'
    assert not a._audit_key(bid).exists()
    assert len(list((a.staging/bid).iterdir()))==2
    AService(a.cfg).recover()
    assert a.db.get(bid)['state']=='READY'
    assert not (a.staging/bid).exists()


def test_p07_corrupted_bundle_blocks_remaining_source_unlinks(node_factory):
    a,b,t,g=node_factory
    g(3)
    bid=a.claim()
    orig=a.clean_sources
    def crash_clean(batch_id):
        return orig(batch_id,after_delete=lambda _: (_ for _ in ()).throw(KeyboardInterrupt()))
    a.clean_sources=crash_clean
    with pytest.raises(KeyboardInterrupt): a.package(bid)
    a.clean_sources=orig
    remain={p.name for p in (a.staging/bid).iterdir()}
    with (a.ready/bid/'bundle').open('r+b') as file:
        file.seek(10)
        byte=file.read(1)
        file.seek(10)
        file.write(bytes([byte[0]^1]))
    with pytest.raises(IntegrityError): a.clean_sources(bid)
    assert a.db.get(bid)['state']=='VERIFY_RECOVERY_BLOCKED'
    assert {p.name for p in (a.staging/bid).iterdir()}==remain


def test_p08_fake_crypto_success_cannot_bypass_verify_gate(node_factory,monkeypatch):
    a,b,t,g=node_factory
    g(1)
    bid=a.claim()
    from aas.crypto.core import NoneCrypto
    def fake_corrupt(self, source,destination,*,recipients=(),audit_key=None):
        destination.write_bytes(b'fake successful payload')
        return {'scheme':'none','version':1,'success':True}
    monkeypatch.setattr(NoneCrypto,'encrypt_file',fake_corrupt)
    with pytest.raises(IntegrityError): a.package(bid)
    assert len(list((a.staging/bid).iterdir()))==1
    assert a.db.get(bid)['state']=='PACKAGING'


def test_p09_no_valid_ack_no_deletion(node_factory):
    a,b,t,g=node_factory
    bid=produced(a,g)
    record=a.db.get(bid)
    good=dict(producer_id='gpuA',consumer_id='desktopB',batch_id=bid,seq=record['seq'],payload_sha256=record['payload_sha256'],file_count=record['file_count'],committed_bytes=record['source_bytes'],receipt_version=1)
    for field,bad in [('batch_id','other'),('payload_sha256','0'*64),('consumer_id','rogue'),('file_count',record['file_count']+1)]:
        changed=dict(good, **{field:bad})
        with pytest.raises(ProtocolError): a.ack(changed)
        assert a.db.get(bid)['state']=='READY'
        assert (a.ready/bid/'bundle').exists()
    assert not a.db.is_acked(bid)


def test_p10_delivered_receipt_survives_output_deletion_before_ack(node_factory):
    a,b,t,g=node_factory
    bid=produced(a,g)
    orig_ack=t.ack
    def lost_ack(_):
        raise TemporaryError('network failed after delivery')
    t.ack=lost_ack
    b.once()
    assert read_json(b.receipts/f'{bid}.json')['receipt_status']=='DELIVERED'
    shutil.rmtree(b.output/'gpuA'/bid)
    t.ack=orig_ack
    with b.db.tx() as c:
        c.execute("UPDATE transfers SET next_attempt=0 WHERE batch_id=?",(bid,))
    b.once()
    assert a.db.get(bid)['state']=='DELETED'
    assert not (b.output/'gpuA'/bid).exists()


def test_p11_crash_after_rename_then_user_moves_dir_uncertain(node_factory):
    a,b,t,g=node_factory
    bid=produced(a,g)
    b.discovery.scan()
    record=b.db.next_job()
    manifest=t.info(bid)
    b._restore(record,manifest,b._download(record))
    b._make_prepared(record,manifest)
    dest=b._target(bid)
    from aas.common.core import rename_new
    rename_new(b._restore_dir(bid),dest)
    moved=b.output/'moved-by-user'
    dest.rename(moved)
    b.db.recover_running()
    b.once()
    assert b.db.state('gpuA',bid)=='HANDOFF_UNCERTAIN'
    assert not t.ack_requests
    assert moved.exists()


def test_p12_reannounce_old_seq_new_event_id(node_factory):
    a,b,t,g=node_factory
    bid=produced(a,g)
    first=b.discovery.scan()
    seq=a.db.get(bid)['seq']
    prior=a.db.get(bid)['ready_event_id']
    event=a.db.reannounce_ready(bid)
    assert event>prior and a.db.get(bid)['seq']==seq
    second=b.discovery.scan()
    assert second.observed_ready_events==1 and second.newly_enqueued_batches==0
    assert b.db.get('gpuA',bid)['event_id']==event
    assert b.discovery.scan(full_reconcile=True).observed_ready_events==1


def test_p13_network_failure_not_successful_zero(node_factory):
    a,b,t,g=node_factory
    t.force_timeout=True
    with pytest.raises(TemporaryError): b.discovery.scan()
    with b.db.tx() as c:
        assert c.execute("SELECT COUNT(*) FROM observations WHERE kind='success'").fetchone()[0]==0


def test_p14_multi_page_and_cursor_atomicity(node_factory):
    a,b,t,g=node_factory
    bids=[]
    for i in range(106):
        bids.append(produced(a,g,1,f'many{i:04d}'))
    seen=b.discovery.scan()
    assert seen.observed_ready_events==106
    assert len(b.db.rows())==106
    assert len(t.pages)>=2
    assert b.db.cursor('gpuA',a.db.epoch())==a.db.get(bids[-1])['ready_event_id']


def test_p15_partial_bundle_delete_resumes_from_ack_tombstone(node_factory):
    a,b,t,g=node_factory
    bid=produced(a,g)
    row=a.db.get(bid)
    ack=dict(producer_id='gpuA',consumer_id='desktopB',batch_id=bid,seq=row['seq'],payload_sha256=row['payload_sha256'],file_count=row['file_count'],committed_bytes=row['source_bytes'],receipt_version=1)
    a.db.accept_ack(ack,producer_id='gpuA',consumer_id='desktopB')
    with pytest.raises(KeyboardInterrupt):
        a.clean_bundle(bid,after_delete=lambda name: (_ for _ in ()).throw(KeyboardInterrupt()) if name=='bundle' else None)
    assert a.db.get(bid)['state']=='DELETING'
    assert not (a.ready/bid/'bundle').exists()
    assert (a.ready/bid/'manifest.json').exists()
    AService(a.cfg).recover()
    assert a.db.get(bid)['state']=='DELETED'
    assert not (a.ready/bid).exists()
    assert a.db.is_acked(bid)


def test_b_sqlite_lost_receipts_rebuild_without_output(node_factory):
    a,b,t,g=node_factory
    bid=produced(a,g)
    b.once()
    shutil.rmtree(b.output/'gpuA'/bid)
    b.db.path.unlink()
    for suffix in ('-wal','-shm'):
        p=Path(str(b.db.path)+suffix)
        p.unlink(missing_ok=True)
    restored=BService(b.cfg,t)
    restored.recover()
    assert restored.db.state('gpuA',bid)=='ACK_PENDING'
    restored.once()
    assert restored.db.state('gpuA',bid)=='ACK_CONFIRMED'
    assert not (b.output/'gpuA'/bid).exists()


def test_manifest_change_conflict_rejected_atomically(node_factory):
    a,b,t,g=node_factory
    bid=produced(a,g)
    page=a.list_ready()
    b.db.apply_page(page)
    cursor=b.db.cursor('gpuA',a.db.epoch())
    tampered=dict(page)
    tampered['items']=[dict(page['items'][0],payload_sha256='f'*64)]
    with pytest.raises(IntegrityError): b.db.apply_page(tampered)
    assert b.db.cursor('gpuA',a.db.epoch())==cursor
    assert b.db.get('gpuA',bid)['payload_sha256']!= 'f'*64


def test_manual_confirm_must_be_audited(node_factory):
    a,b,t,g=node_factory
    bid=produced(a,g)
    b.discovery.scan()
    record=b.db.next_job()
    manifest=t.info(bid)
    b._restore(record,manifest,b._download(record))
    b._make_prepared(record,manifest)
    shutil.rmtree(b._restore_dir(bid))
    b.db.recover_running()
    b.once()
    assert b.db.state('gpuA',bid)=='HANDOFF_UNCERTAIN'
    with pytest.raises(ProtocolError): b.resolve_uncertain(bid,action='confirm-delivered',reason='x')
    b.resolve_uncertain(bid,action='confirm-delivered',reason='Verified independent recipient copy')
    receipt=read_json(b.receipts/f'{bid}.json')
    assert receipt['confirmation']=='MANUAL_ATTESTATION'
    b.once()
    assert a.db.get(bid)['state']=='DELETED'


def test_force_command_allowlist(node_factory):
    a,b,t,g=node_factory
    import io
    out=io.BytesIO()
    run_forced(a,'aasctl hello',stdout=out)
    assert json.loads(out.getvalue())['producer_id']=='gpuA'
    for bad in ('bash -c id','aasctl get --batch-id ../passwd --offset 0 --length 10','aasctl info --batch-id /etc/passwd'):
        with pytest.raises((ProtocolError,SystemExit)):run_forced(a,bad,stdout=io.BytesIO())


def test_unsafe_legacy_adaptive_config_rejected(tmp_path):
    from aas.common.config import BConfig
    config=tmp_path/'b.yaml'
    config.write_text('''node: {producer_id: gpuA, consumer_id: desktopB}
paths: {root_dir: /tmp/AASB-test}
remote: {host: localhost, user: aas}
scheduler: {mode: adaptive, strategy: fixed_interval}
''')
    with pytest.raises(ProtocolError): BConfig.load(config)


def test_unacked_package_never_removed_on_low_disk(node_factory):
    a,b,t,g=node_factory
    bid=produced(a,g)
    with pytest.raises(IntegrityError): a.clean_bundle(bid)
    assert (a.ready/bid/'bundle').exists()


def test_publishing_recovery_after_cert_persisted_before_rename(node_factory):
    a,b,t,g=node_factory
    g(2)
    bid=a.claim()
    with pytest.raises(KeyboardInterrupt):
        a.package(bid,before_publish=lambda: (_ for _ in ()).throw(KeyboardInterrupt('crash between cert commit and rename')))
    assert a.db.get(bid)['state']=='PUBLISHING'
    assert len(list((a.staging/bid).iterdir()))==2
    a.recover()
    assert a.db.get(bid)['state']=='READY'


def test_certificate_recovery_all_unlinks_done_before_ready(node_factory):
    a,b,t,g=node_factory
    g(2)
    bid=a.claim()
    original=a.db.mark_ready
    def fail(*args,**kwargs): raise KeyboardInterrupt('fail before READY DB')
    a.db.mark_ready=fail
    with pytest.raises(KeyboardInterrupt): a.package(bid)
    a.db.mark_ready=original
    assert a.db.get(bid)['state']=='SOURCE_CLEANING'
    assert not (a.staging/bid).exists()
    a.recover()
    assert a.db.get(bid)['state']=='READY'


def test_operator_retry_blocked_only_after_proof_restored(node_factory):
    a,b,t,g=node_factory
    g(2)
    bid=a.claim()
    clean=a.clean_sources
    def fail_one(bid):
        return clean(bid, after_delete=lambda _: (_ for _ in ()).throw(KeyboardInterrupt()))
    a.clean_sources=fail_one
    with pytest.raises(KeyboardInterrupt): a.package(bid)
    a.clean_sources=clean
    bundle=a.ready/bid/'bundle'
    original=bundle.read_bytes()
    bundle.chmod(0o600)
    bundle.write_bytes(b'corrupt'+original[7:])
    with pytest.raises(IntegrityError): a.clean_sources(bid)
    with pytest.raises(IntegrityError): a.retry_blocked(bid)
    bundle.write_bytes(original)
    a.retry_blocked(bid)
    assert a.db.get(bid)['state']=='READY'


def test_crash_after_bundle_dir_rmdir_before_db_deleted(node_factory):
    a,b,t,g=node_factory
    bid=produced(a,g)
    row=a.db.get(bid)
    ack=dict(producer_id='gpuA',consumer_id='desktopB',batch_id=bid,seq=row['seq'],payload_sha256=row['payload_sha256'],file_count=row['file_count'],committed_bytes=row['source_bytes'],receipt_version=1)
    a.db.accept_ack(ack,producer_id='gpuA',consumer_id='desktopB')
    original_update=a.db.update
    def interrupt(batch_id, prior, state, **kwargs):
        if prior=='DELETING' and state=='DELETED': raise KeyboardInterrupt()
        return original_update(batch_id,prior,state,**kwargs)
    a.db.update=interrupt
    with pytest.raises(KeyboardInterrupt): a.clean_bundle(bid)
    a.db.update=original_update
    assert a.db.get(bid)['state']=='DELETING'
    assert not (a.ready/bid).exists()
    a.recover()
    assert a.db.get(bid)['state']=='DELETED'


def test_forced_emergency_role_separation(node_factory):
    a,b,t,g=node_factory
    import io
    with pytest.raises(ProtocolError): run_forced(a,'aasctl emergency-stream',stdout=io.BytesIO(),role='consumer')
    with pytest.raises(ProtocolError): run_forced(a,'aasctl hello',stdout=io.BytesIO(),role='emergency')


def test_reconciler_full_ready_and_ledger(node_factory):
    a,b,t,g=node_factory
    bid=produced(a,g)
    metrics=b.reconcile()
    assert metrics['ready_seen']==1 and metrics['ledger_audited']==1 and metrics['alerts']==0
    b.once()
    metrics=b.reconcile()
    assert metrics['ledger_audited']==1 and metrics['alerts']==0
    (b.receipts/f'{bid}.json').unlink()
    assert b.reconcile()['alerts']==1


def test_source_unlink_before_audit_crash_replays_absence_safely(node_factory):
    a,b,t,g=node_factory
    g(2)
    bid=a.claim()
    original=a.db.audit
    called={'once':False}
    def crash_between_unlink_and_audit(batch_id, action, detail):
        if action=='SOURCE_UNLINKED' and not called['once']:
            called['once']=True
            raise KeyboardInterrupt('file unlinked and fsynced, log not committed')
        return original(batch_id,action,detail)
    a.db.audit=crash_between_unlink_and_audit
    with pytest.raises(KeyboardInterrupt): a.package(bid)
    a.db.audit=original
    assert a.db.get(bid)['state']=='SOURCE_CLEANING'
    a.recover()
    assert a.db.get(bid)['state']=='READY'
    with a.db.tx() as c:
        assert c.execute("SELECT COUNT(*) FROM audit_log WHERE batch_id=? AND action='SOURCE_ABSENT_CONFIRMED'",(bid,)).fetchone()[0]>=1


def test_binary_ssh_protocol_handler_exact_range(node_factory):
    a,b,t,g=node_factory
    bid=produced(a,g)
    import io
    header=io.BytesIO()
    run_forced(a,'aasctl list-ready --after-event-id 0 --page-size 10',stdout=header)
    assert json.loads(header.getvalue())['items'][0]['batch_id']==bid
    length=123
    raw=io.BytesIO()
    run_forced(a,f'aasctl get --batch-id {bid} --offset 0 --length {length}',stdout=raw)
    assert raw.getvalue()==(a.ready/bid/'bundle').read_bytes()[:length]
    with pytest.raises(ProtocolError):
        run_forced(a,f'aasctl get --batch-id {bid} --offset 0 --length 999999999',stdout=io.BytesIO())


def test_new_input_with_same_basename_is_never_deleted_by_old_batch(node_factory):
    a,b,t,g=node_factory
    g(1,prefix='same')
    bid=a.claim()
    assert bid
    g(1,prefix='same')  # generator created new file with the same basename
    assert (a.input/'same_0.png').exists()
    a.package(bid)
    assert a.db.get(bid)['state']=='READY'
    assert (a.input/'same_0.png').exists()


def test_windows_unsafe_image_names_are_rejected():
    from aas.common.core import safe_image_name
    for name in ('CON.png','a?.png','bad:path.jpg','trailing .png ','image.txt','../foo.png'):
        with pytest.raises(IntegrityError): safe_image_name(name)


def test_inflight_ack_is_not_claimable_by_second_worker(node_factory):
    a,b,t,g=node_factory
    bid=produced(a,g)
    b.discovery.scan()
    seen=[]
    orig=t.ack
    def ack_probe(request):
        seen.append(b.db.next_job())  # another worker polling while this ACK is in flight
        return orig(request)
    t.ack=ack_probe
    b.process_one(b.db.next_job())
    assert seen==[None]
    assert b.db.state('gpuA',bid)=='ACK_CONFIRMED' and len(t.ack_requests)==1


def test_address_family_validated(tmp_path):
    from aas.common.config import BConfig
    base='node: {producer_id: gpuA, consumer_id: desktopB}\npaths: {root_dir: /tmp/x}\nremote: {host: h, user: u, address_family: %s}\n'
    config=tmp_path/'b.yaml'
    config.write_text(base % 'inet')
    assert BConfig.load(config).address_family=='inet'
    config.write_text(base % 'ipx')
    with pytest.raises(ProtocolError): BConfig.load(config)


def test_wait_ready_returns_immediately_when_newer_and_times_out_otherwise(node_factory):
    a,b,t,g=node_factory
    start=time.monotonic()
    quiet=a.wait_ready(0,0.3,tick=0.05)
    assert quiet['changed'] is False and 0.25 <= time.monotonic()-start < 3
    bid=produced(a,g)
    event=a.db.get(bid)['ready_event_id']
    start=time.monotonic()
    hit=a.wait_ready(event-1,30,tick=0.05)
    assert hit['changed'] is True and hit['last_ready_event_id']==event and time.monotonic()-start < 1
    assert a.wait_ready(event,0.2,tick=0.05)['changed'] is False  # nothing newer than what B already saw
    with pytest.raises(ProtocolError): a.wait_ready(-1,1)


def test_wait_ready_is_allowlisted_and_read_only(node_factory):
    a,b,t,g=node_factory
    import io
    out=io.BytesIO()
    run_forced(a,'aasctl wait-ready --after-event-id 0 --timeout-seconds 0.2',stdout=out)
    assert json.loads(out.getvalue())['changed'] is False
    with pytest.raises(ProtocolError): run_forced(a,'aasctl wait-ready --after-event-id 0',stdout=io.BytesIO(),role='emergency')
    assert a.db.rows('READY')==[]


def test_long_poll_wake_discovers_new_batch_far_before_fixed_interval(node_factory):
    from dataclasses import replace
    a,b,t,g=node_factory
    cfg=replace(b.cfg,interval_seconds=100.0,wake_mode='long_poll',wake_timeout_seconds=5.0)
    woken=BService(cfg,t)
    stop=threading.Event(); faults=[]
    def runner():
        try: woken.run(stop)
        except BaseException as exc: faults.append(exc)
    th=threading.Thread(target=runner); th.start()
    try:
        deadline=time.monotonic()+5
        while woken.discovery.last_high is None and time.monotonic()<deadline: time.sleep(0.05)
        assert woken.discovery.last_high==0                    # initial scan done, nothing there
        bid=produced(a,g)                                      # unpredictable arrival, far from the next 100 s tick
        start=time.monotonic()
        while time.monotonic()-start<8:
            if woken.db.rows(): break
            time.sleep(0.05)
        assert len(woken.db.rows())==1, 'wake-up did not trigger a scan'
        assert time.monotonic()-start < 6
        deadline=time.monotonic()+10
        while a.db.get(bid)['state']!='DELETED' and time.monotonic()<deadline: time.sleep(0.1)
        assert a.db.get(bid)['state']=='DELETED'
    finally:
        stop.set(); th.join(timeout=10)
    assert not th.is_alive() and not faults


def test_wake_config_validated(tmp_path):
    from aas.common.config import BConfig
    base='node: {producer_id: gpuA, consumer_id: desktopB}\npaths: {root_dir: /tmp/x}\nremote: {host: h, user: u}\nscheduler: {wake: {mode: %s, timeout_seconds: %s}}\n'
    config=tmp_path/'b.yaml'
    config.write_text(base % ('long_poll',55)); cfg=BConfig.load(config)
    assert cfg.wake_mode=='long_poll' and cfg.interval_seconds==300.0   # fixed_interval stays the safety net
    for mode,secs in (('push',55),('long_poll',1),('long_poll',500)):
        config.write_text(base % (mode,secs))
        with pytest.raises(ProtocolError): BConfig.load(config)


def test_input_summary_reports_files_that_would_otherwise_be_silently_skipped(node_factory):
    a,b,t,g=node_factory
    g(1,'ok')
    (a.input/'half.png.tmp').write_bytes(b'x')
    (a.input/'sub').mkdir()
    linux=sys.platform.startswith('linux')
    if linux: (a.input/'bad:name.png').write_bytes(b'x')   # ':' cannot exist on a Windows filesystem
    (a.input/'notes.txt').write_bytes(b'x')
    s=a.input_summary()
    assert s['images_waiting']==1 and s['in_progress_tmp_files']==1 and s['subdirs_ignored']==1
    assert s['rejected_unsafe_names']==(['bad:name.png'] if linux else []) and s['non_image_ignored']==1
    assert a.status()['input']==s


@pytest.mark.skipif(not sys.platform.startswith('linux'), reason='needs /proc')
def test_file_still_open_by_a_writer_is_not_claimed(node_factory):
    a,b,t,g=node_factory
    from PIL import Image
    path=a.input/'slow.png'
    Image.new('RGB',(16,16),(1,2,3)).save(path)           # complete, valid PNG ...
    writer=path.open('ab')                                # ... but a producer still holds it open
    try:
        assert a._eligible_names()==[] and a.claim() is None
    finally:
        writer.close()
    assert a._eligible_names()==['slow.png']



# ---------------------------------------------------------------- hold back the newest image + idle flush
def _aged_images(a, count, *, newest_age, gap, prefix="h"):
    """count valid PNGs whose mtimes are `gap` seconds apart; the newest is `newest_age` seconds old."""
    import os
    from PIL import Image
    now = time.time()
    names = []
    for i in range(count):
        path = a.input / f"{prefix}_{i:03d}.png"
        Image.new("RGB", (8 + i, 8), (i, 2, 3)).save(path)
        stamp = now - newest_age - gap * (count - 1 - i)
        os.utime(path, (stamp, stamp))
        names.append(path.name)
    return names


def test_n_plus_one_rule_packs_n_and_keeps_the_newest(node_factory):
    from dataclasses import replace
    a, b, t, g = node_factory
    a.cfg = replace(a.cfg, hold_back_newest=1, max_files=3, max_wait_seconds=600)
    names = _aged_images(a, 3, newest_age=10, gap=5)
    assert a.claim() is None, 'n files present is not enough: the newest one is held back'
    assert a.watch_state()['held_back'] == [names[-1]] and a.watch_state()['idle'] is False
    more = _aged_images(a, 1, newest_age=1, gap=0, prefix='z')  # producer has started a 4th file
    bid = a.claim()
    assert bid
    assert sorted(p.name for p in (a.staging / bid).iterdir()) == sorted(names)  # first n, in generation order
    assert [p.name for p in a.input.iterdir()] == more                          # newest stays behind


def test_idle_producer_flushes_the_tail_including_the_newest(node_factory):
    from dataclasses import replace
    a, b, t, g = node_factory
    a.cfg = replace(a.cfg, hold_back_newest=1, max_files=5, max_wait_seconds=10 ** 6)
    names = _aged_images(a, 2, newest_age=600, gap=5)       # nothing new for 600 s > 300 s cold-start ceiling
    assert a.watch_state()['idle'] is True and a.watch_state()['held_back'] == []
    bid = a.claim()
    assert bid and sorted(p.name for p in (a.staging / bid).iterdir()) == sorted(names)
    assert list(a.input.iterdir()) == []


def test_fresh_tmp_file_means_the_producer_is_still_active(node_factory):
    from dataclasses import replace
    a, b, t, g = node_factory
    a.cfg = replace(a.cfg, hold_back_newest=1, max_files=5, max_wait_seconds=10 ** 6)
    _aged_images(a, 1, newest_age=600, gap=0)
    (a.input / 'working.png.tmp').write_bytes(b'being written')   # fresh mtime
    assert a.watch_state()['idle'] is False
    assert a.claim() is None


def test_idle_threshold_adapts_to_the_observed_image_interval(node_factory):
    from dataclasses import replace
    a, b, t, g = node_factory
    assert a.idle_threshold_seconds([]) == a.cfg.idle_flush_max_seconds   # nothing learned yet: be patient
    a.cfg = replace(a.cfg, max_files=6, max_wait_seconds=0)
    _aged_images(a, 6, newest_age=60, gap=10, prefix='hist')            # 10 s between images
    bid = a.claim()                                                     # (hold_back_newest=0 here)
    assert bid
    a.package(bid)                                                      # the claim snapshot (with mtimes) is persisted when packaging
    assert a.idle_threshold_seconds([]) == pytest.approx(40.0, abs=0.5)  # 4 x 10 s
    _aged_images(a, 12, newest_age=5, gap=2, prefix='fast')             # the producer speeds up: recent gaps are 2 s
    entries, _ = a._scan_input()
    assert a.idle_threshold_seconds(entries) == pytest.approx(a.cfg.idle_flush_min_seconds, abs=0.5)  # clamped at the floor


def test_hold_back_zero_keeps_the_old_pack_everything_behavior(node_factory):
    a, b, t, g = node_factory                       # fixture runs with hold_back_newest=0
    names = _aged_images(a, 2, newest_age=1, gap=1)
    bid = a.claim()
    assert bid and sorted(p.name for p in (a.staging / bid).iterdir()) == sorted(names)


def test_watch_config_validated(tmp_path):
    from aas.common.config import AConfig
    base = 'node: {producer_id: gpuA, consumer_id: desktopB}\npaths: {root_dir: %s/a, input_dir: %s/in}\nwatch: %s\n'
    config = tmp_path / 'a.yaml'
    config.write_text(base % (tmp_path.as_posix(), tmp_path.as_posix(), '{hold_back_newest: 1, idle_flush_factor: 4}'))
    cfg = AConfig.load(config)
    assert cfg.hold_back_newest == 1 and cfg.idle_flush_factor == 4.0
    for bad in ('{hold_back_newest: -1}', '{idle_flush_factor: 0.5}', '{idle_flush_min_seconds: 500, idle_flush_max_seconds: 10}'):
        config.write_text(base % (tmp_path.as_posix(), tmp_path.as_posix(), bad))
        with pytest.raises(ProtocolError): AConfig.load(config)
