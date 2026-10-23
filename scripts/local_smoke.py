"""Standalone local in-process transport integration smoke; never used for production."""
from __future__ import annotations
import io
import tempfile
from pathlib import Path
from PIL import Image
from aas.common.config import AConfig,BConfig
from aas.a.core import AService
from aas.b.core import BService


class TestOnlyTransport:
    def __init__(self, a): self.a = a
    def hello(self): return self.a.hello()
    def list_ready(self, after_event_id,page_size,snapshot_high=None):
        return self.a.list_ready(after_event_id=after_event_id,page_size=page_size,snapshot_high=snapshot_high)
    def info(self,batch_id): return self.a.info(batch_id)
    def get_range(self,batch_id,offset,length):
        out=io.BytesIO()
        self.a.get_range(batch_id,offset,length,out)
        return out.getvalue()
    def ack(self,data): return {'result': self.a.ack(data)}
    def ledger(self,since_seq=0): return {'items':self.a.ledger(since_seq)}


def main():
    with tempfile.TemporaryDirectory(prefix='aas-smoke-') as root:
        root = Path(root)
        a = AService(AConfig('demoA','demoB',root/'cloud',root/'generated',max_wait_seconds=0,min_age_seconds=0,hold_back_newest=0))
        for idx in range(3):
            Image.new('RGB',(32,32),(20+idx,90,135)).save(a.input/f'sample-{idx}.png')
        bid = a.once()
        b = BService(BConfig('demoB','demoA',root/'local','localhost','aas',interval_seconds=1),TestOnlyTransport(a))
        result = b.once()
        final = a.db.get(bid)['state']
        out = b.output/'demoA'/bid
        assert final == 'DELETED' and len(list(out.iterdir())) == 3
        print(f'OK {bid}: A={final}, B={b.db.state("demoA",bid)}, output files={len(list(out.iterdir()))}, result={result}')

if __name__ == '__main__': main()
