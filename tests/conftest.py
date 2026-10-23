from __future__ import annotations
from dataclasses import replace
import io
from pathlib import Path
import pytest
from PIL import Image
from aas.a.core import AService
from aas.b.core import BService
from aas.common.config import AConfig, BConfig
from aas.common.core import now_utc


class InProcessTransport:
    """A service adapter used in tests only; production B always uses restricted SSH."""
    def __init__(self, a):
        self.a = a
        self.pages = []
        self.ack_requests = []
        self.get_calls = []
        self.force_timeout = False

    def hello(self):
        if self.force_timeout:
            from aas.common.core import TemporaryError
            raise TemporaryError("simulation: SSH disconnected")
        return self.a.hello()

    def list_ready(self, after_event_id, page_size, snapshot_high=None):
        self.pages.append((after_event_id, page_size, snapshot_high))
        return self.a.list_ready(after_event_id=after_event_id, page_size=page_size, snapshot_high=snapshot_high)

    def wait_ready(self, after_event_id, timeout_seconds):
        return self.a.wait_ready(after_event_id, timeout_seconds, tick=0.05)

    def info(self, batch_id):
        return self.a.info(batch_id)

    def get_range(self, batch_id, offset, length):
        self.get_calls.append((batch_id, offset, length))
        stream = io.BytesIO()
        self.a.get_range(batch_id, offset, length, stream)
        return stream.getvalue()

    def ack(self, request):
        self.ack_requests.append(request)
        return {"result": self.a.ack(request)}

    def ledger(self, since_seq=0):
        return {"items": self.a.ledger(since_seq)}


@pytest.fixture
def node_factory(tmp_path):
    a_config = AConfig(producer_id="gpuA", consumer_id="desktopB", root=tmp_path / "A", input_dir=tmp_path / "A-input", min_age_seconds=0, max_files=4, max_bytes=100000, max_wait_seconds=0, hold_back_newest=0)
    a = AService(a_config)
    transport = InProcessTransport(a)
    b_config = BConfig(consumer_id="desktopB", producer_id="gpuA", root=tmp_path / "B", remote_host="test.invalid", remote_user="aas", interval_seconds=0.25, chunk_bytes=4096, retry_initial_seconds=0.01, retry_max_seconds=0.05, retry_jitter=False)
    b = BService(b_config, transport)

    def generate(n=2, prefix="image"):
        for i in range(n):
            path = a.input / f"{prefix}_{i}.png"
            Image.new("RGB", (12+i, 8+i), (20+i, 30+i, 40+i)).save(path)

    return a, b, transport, generate
