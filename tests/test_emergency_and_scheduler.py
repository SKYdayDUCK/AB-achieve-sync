from __future__ import annotations
import io
import os
from pathlib import Path
import shutil
import socket
import subprocess
import threading
import time
import pytest
from aas.a.emergency import emergency_stream
from aas.b.emergency import receive_session
from aas.b.core import BService
from aas.common.core import IntegrityError, ProtocolError, TemporaryError, process_lock
from aas.crypto.core import AgeCrypto
from aas.common.config import AConfig


def test_emergency_file_stream_ack_then_unlink(node_factory,tmp_path):
    a,b,t,g=node_factory
    g(2)
    left,right=socket.socketpair()
    errors=[]
    def server():
        try:
            with left.makefile('rwb',buffering=0) as server_io:
                emergency_stream(a,reader=server_io,writer=server_io)
        except Exception as exc:
            errors.append(exc)
    thread=threading.Thread(target=server)
    thread.start()
    with right.makefile('rwb',buffering=0) as client_io:
        count=receive_session(client_io,client_io,b.output/'emergency')
    thread.join(timeout=10)
    assert not thread.is_alive() and not errors
    assert count==2
    assert not list(a.input.iterdir())
    assert not list(a.emergency_claim.iterdir())
    assert len(list((b.output/'emergency').iterdir()))==2
    assert a.db.rows('READY')==[]


def test_emergency_and_normal_claim_exclusive(node_factory):
    a,b,t,g=node_factory
    g(1)
    with process_lock(a.state/'input_claim.lock'):
        with pytest.raises(TemporaryError): a.claim()
    assert len(list(a.input.iterdir()))==1
    assert a.db.rows('READY')==[]


def test_emergency_unconfirmed_claim_recover_no_overwrite(node_factory):
    a,b,t,g=node_factory
    g(1)
    name=next(a.input.iterdir()).name
    claimed=a.emergency_claim/f'0123456789abcdef0123456789abcdef__{name}'
    (a.input/name).rename(claimed)
    g(1)
    a.recover_emergency_claims()
    assert len(list(a.input.iterdir()))==2
    assert not claimed.exists()
    assert any('recovered_' in p.name for p in a.input.iterdir())


def test_b_worker_discovery_not_blocked_by_slow_download(node_factory):
    a,b,t,g=node_factory
    g(1,prefix='first')
    first=a.once()
    started=threading.Event()
    release=threading.Event()
    original=t.get_range
    def blocking_range(batch_id,offset,length):
        if batch_id==first and not started.is_set():
            started.set()
            assert release.wait(timeout=10)
        return original(batch_id,offset,length)
    t.get_range=blocking_range
    stop=threading.Event()
    faults=[]
    def runner():
        try: b.run(stop)
        except BaseException as exc: faults.append(exc)
    th=threading.Thread(target=runner)
    th.start()
    try:
        assert started.wait(timeout=8)
        g(1,prefix='second')
        second=a.once()
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            if len(b.db.rows())>=2:break
            time.sleep(0.05)
        assert len(b.db.rows())==2
        assert b.db.state('gpuA',first)=='RUNNING'
    finally:
        release.set()
        stop.set()
        th.join(timeout=8)
    assert not th.is_alive() and not faults


def test_age_encryption_requires_real_binary(tmp_path):
    """Not a crypto mock: skip when neither age binary is installed in CI."""
    if not shutil.which('age') or not shutil.which('age-keygen'):
        pytest.skip('real age/age-keygen executables unavailable in test environment')
    age=AgeCrypto()
    private=tmp_path/'test.key'
    pub=age.make_audit_key(private)
    content=tmp_path/'raw'
    content.write_bytes(b'age roundtrip test data')
    cipher=tmp_path/'cipher'
    info=age.encrypt_file(content,cipher,recipients=(pub,),audit_key=private)
    clear=tmp_path/'clear'
    age.decrypt_file(cipher,clear,identity=private)
    assert clear.read_bytes()==content.read_bytes()
    assert info['scheme']=='age_x25519'
