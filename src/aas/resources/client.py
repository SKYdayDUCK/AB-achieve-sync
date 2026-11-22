"""Durable poll-batch queue; resources keep independent leases and ACK handoff."""
from __future__ import annotations
from contextlib import contextmanager
import fnmatch,json,os,re,sqlite3,sys,threading,time,unicodedata,uuid
from pathlib import Path,PurePosixPath
import yaml
from aas.common.core import ProtocolError,IntegrityError,CapacityError,TemporaryError,hash_file,hash_obj,write_json,read_json,process_lock,rename_new,WIN_RESERVED
from aas.b.scheduling.policy import FixedIntervalPoll,PollContext,RetryPolicy
from .config import load_config
from .transport import ResourceSSHTransport,RemoteError

def safe_name(name):
    if not isinstance(name,str) or not name or len(name.encode('utf-8'))>220 or name in ('.','..') or name[-1] in '. ':raise IntegrityError('invalid file name')
    if unicodedata.normalize('NFC',name)!=name or any(c in '/\\:*?"<>|' or ord(c)<32 for c in name) or name.split('.')[0].upper() in WIN_RESERVED:raise IntegrityError('unsafe file name')
    return name

def safe_key(key, allow_root=False):
    if not isinstance(key, str):raise IntegrityError('unsafe resource key')
    if allow_root and key == '.':return Path('.')
    path=PurePosixPath(key);parts=path.parts
    if not parts or path.is_absolute() or (allow_root and path.as_posix()!=key):raise IntegrityError('unsafe resource key')
    for part in parts:safe_name(part)
    return Path(*parts)

class ResourceClient:
    ACTIVE=('QUEUED','RUNNING','RETRY','ACK_PENDING','HOLD')
    def __init__(self,config,transport=None,preset='files'):
        self.cfg=load_config(config,preset,'b');self.root=Path(self.cfg['paths']['root_dir']).expanduser().resolve();self.root.mkdir(parents=True,exist_ok=True)
        self.state=self.root/'state';self.state.mkdir(exist_ok=True);self.receipts=self.state/'receipts';self.receipts.mkdir(exist_ok=True)
        self.database=self.state/'resources.sqlite3';self.consumer=self.cfg['node']['consumer_id'];self.producer=self.cfg['node']['producer_id']
        self.capacity=int(self.cfg['transfer']['max_download_queue']);self.chunk=int(self.cfg['transfer']['chunk_bytes']);self.floor=int(self.cfg['capacity']['min_free_bytes'])
        if self.capacity<1 or not 1<=self.chunk<=16*1024**2:raise ProtocolError('invalid transfer limits')
        self.target_batch_bytes=self.cfg['transfer']['target_batch_bytes'];workers=self.cfg['transfer']['download_workers']
        if type(self.target_batch_bytes) is not int or self.target_batch_bytes<0 or type(workers) is not int or not 1<=workers<=16:raise ProtocolError('invalid batch size or download worker count')
        self.workers=min(workers,self.capacity)
        self.policy=FixedIntervalPoll(self.cfg['scheduler']['fixed_interval']['interval_seconds']);self.transport=transport or ResourceSSHTransport(self.cfg['remote'])
        self.stop=threading.Event();self.worker_lock=threading.BoundedSemaphore(self.workers)
        self.claim_lock=threading.Lock();self.inflight={};self.failed_workers=set();self.progress_lock=threading.Lock()
        with self.tx() as c:
            c.execute('CREATE TABLE IF NOT EXISTS resources(resource_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, resource_key TEXT NOT NULL,state TEXT NOT NULL,request_id TEXT NOT NULL,lease_token TEXT,manifest TEXT,output_dir TEXT,attempts INTEGER NOT NULL DEFAULT 0,next_attempt REAL NOT NULL DEFAULT 0,error TEXT,created REAL NOT NULL,updated REAL NOT NULL,batch_id TEXT)')
            if 'batch_id' not in {row['name'] for row in c.execute('PRAGMA table_info(resources)')}:
                c.execute('ALTER TABLE resources ADD COLUMN batch_id TEXT')
            c.execute('CREATE TABLE IF NOT EXISTS download_batches(batch_id TEXT PRIMARY KEY,created REAL NOT NULL,origin TEXT NOT NULL)')
            legacy=c.execute('SELECT COUNT(*),MIN(created) FROM resources WHERE batch_id IS NULL').fetchone()
            if legacy[0]:
                batch_id='legacy-'+uuid.uuid4().hex
                c.execute('INSERT INTO download_batches(batch_id,created,origin) VALUES(?,?,?)',(batch_id,legacy[1],'legacy'))
                # Preserve request IDs, tokens, states, retries, parts and receipts.
                c.execute('UPDATE resources SET batch_id=? WHERE batch_id IS NULL',(batch_id,))
            c.execute('CREATE INDEX IF NOT EXISTS resources_batch_state ON resources(batch_id,state)')
    @contextmanager
    def tx(self):
        c=sqlite3.connect(self.database,timeout=30,isolation_level=None);c.row_factory=sqlite3.Row;c.execute('PRAGMA busy_timeout=30000');c.execute('PRAGMA synchronous=FULL');c.execute('PRAGMA journal_mode=WAL')
        try:c.execute('BEGIN IMMEDIATE');yield c;c.commit()
        except BaseException:c.rollback();raise
        finally:c.close()
    def rows(self):
        # SQLite row IDs retain insertion order when discovery timestamps tie.
        with self.tx() as c:return [dict(r) for r in c.execute('SELECT * FROM resources ORDER BY rowid')]
    def pending_count(self):
        with self.tx() as c:return c.execute("SELECT COUNT(*) FROM resources WHERE state NOT IN ('DONE','SKIPPED')").fetchone()[0]
    @staticmethod
    def _pending_batches(c):
        return c.execute("SELECT COUNT(*) FROM (SELECT batch_id FROM resources WHERE state NOT IN ('DONE','SKIPPED') GROUP BY batch_id)").fetchone()[0]
    def pending_batch_count(self):
        with self.tx() as c:return self._pending_batches(c)
    @staticmethod
    def _batch_rows(c):
        return [dict(row) for row in c.execute("SELECT b.batch_id,b.created,b.origin,COUNT(r.resource_id) AS resource_count,SUM(CASE WHEN r.state NOT IN ('DONE','SKIPPED') THEN 1 ELSE 0 END) AS pending_resource_count FROM download_batches b LEFT JOIN resources r ON r.batch_id=b.batch_id GROUP BY b.batch_id ORDER BY b.rowid")]
    def batch_rows(self):
        with self.tx() as c:return self._batch_rows(c)
    def _set(self,rid,state,**values):
        allowed={'lease_token','manifest','output_dir','next_attempt','error'}
        if not values.keys()<=allowed:raise ValueError('invalid ledger field')
        with self.tx() as c:
            existing=c.execute('SELECT state FROM resources WHERE resource_id=?',(rid,)).fetchone()
            if existing[0]=='DONE' and state!='DONE':raise IntegrityError('completed resource cannot regress')
            assignments=['state=?','updated=?']+[k+'=?' for k in values];c.execute('UPDATE resources SET '+','.join(assignments)+' WHERE resource_id=?',[state,time.time(),*values.values(),rid])
    def _request(self,op,**values):return self.transport.call({'op':op,'producer_id':self.producer,'consumer_id':self.consumer,**values},binary=op=='get')
    def _jobs(self):
        path=self.cfg.get('queue_file') or self.cfg['paths'].get('queue_file')
        data=yaml.safe_load(Path(path).read_text(encoding='utf-8-sig')) if path else self.cfg.get('queue',{})
        jobs=data.get('jobs',[]) if isinstance(data,dict) else data
        if not isinstance(jobs,list):raise ProtocolError('local queue must contain jobs list')
        for job in jobs:
            if not isinstance(job,dict) or not set(job)<={'source_id','key','enabled'}:raise ProtocolError('invalid local queue job fields')
            if any(not isinstance(job.get(k),str) or not job[k].strip() for k in ('source_id','key')):raise ProtocolError('queue selectors must be nonempty strings')
            if type(job.get('enabled',True)) is not bool:raise ProtocolError('queue enabled must be boolean')
        return [j for j in jobs if j.get('enabled',True)]
    def poll(self):
        before=self.pending_batch_count()
        if before>=self.capacity:
            self._status('queue_full_wait_next_poll');return {'queue_full':True,'discovered':0,'enqueued':0,'enqueued_batches':0,'batch_id':None,'pending_batches':before}
        rows=self.rows();active=[{'resource_id':r['resource_id'],'lease_token':r['lease_token']} for r in rows if r['lease_token'] and r['state'] in ('RUNNING','ACK_PENDING','RETRY')]
        try:result=self._request('catalog',active=active)
        except RemoteError as error:
            if error.code not in ('LEASE_EXPIRED','LEASE_LOST'):raise
            result=self._request('catalog',active=[])
        items=result.get('resources',[]);jobs=self._jobs();rank={}
        for i,job in enumerate(jobs):
            for item in items:
                if fnmatch.fnmatchcase(item['source_id'],job.get('source_id','*')) and fnmatch.fnmatchcase(item['key'],job.get('key','*')):rank.setdefault(item['resource_id'],i)
        eligible=sorted((x for x in items if x['resource_id'] in rank),key=lambda x:(rank[x['resource_id']],x['resource_id']))
        candidates=[];seen=set()
        for item in eligible:
            safe_name(item['resource_id']);safe_name(item['source_id'])
            safe_key(item['key'], allow_root=item['source_id'] in self.cfg['paths']['source_output_dirs'])
            if item.get('state') in ('ACKED','HOLD','SUPERSEDED') or item['resource_id'] in seen:continue
            seen.add(item['resource_id']);candidates.append(item)
        added=0;batch_id=None;batch_ids=[];full=False
        with self.tx() as c:
            # The batch and every resource row are one commit. Recheck under
            # BEGIN IMMEDIATE so overlapping pollers cannot consume a fifth slot.
            count=self._pending_batches(c)
            full=count>=self.capacity
            if not full:
                fresh=[item for item in candidates if c.execute('SELECT 1 FROM resources WHERE resource_id=?',(item['resource_id'],)).fetchone() is None]
                groups=[];group=[];size=0
                for item in fresh:
                    weight=item.get('total_bytes',0)
                    if self.target_batch_bytes and (type(weight) is not int or weight<0):raise ProtocolError('invalid resource size in catalog')
                    if self.target_batch_bytes and group and size+weight>self.target_batch_bytes:
                        groups.append(group);group=[];size=0
                        if len(groups)>=self.capacity-count:break
                    group.append(item);size+=weight
                if group and len(groups)<self.capacity-count:groups.append(group)
                for group in groups:
                    identifier=uuid.uuid4().hex;batch_ids.append(identifier)
                    c.execute('INSERT INTO download_batches(batch_id,created,origin) VALUES(?,?,?)',(identifier,time.time(),'poll'))
                    for item in group:
                        now=time.time()
                        c.execute('INSERT INTO resources(resource_id,source_id,resource_key,state,request_id,created,updated,batch_id)VALUES(?,?,?,?,?,?,?,?)',
                            (item['resource_id'],item['source_id'],item['key'],'QUEUED',uuid.uuid4().hex,now,now,identifier))
                    added+=len(group)
                batch_id=batch_ids[0] if batch_ids else None
        self._status('queue_full_after_catalog' if full else 'polled')
        return {'queue_full':full,'discovered':len(items),'enqueued':added,'enqueued_batches':len(batch_ids),'batch_id':batch_id,'batch_ids':batch_ids,'pending_batches':self.pending_batch_count()}
    def _next(self):
        with self.claim_lock:
            selected=None
            with self.tx() as c:
                candidates=c.execute("SELECT * FROM resources WHERE state IN ('QUEUED','RETRY','ACK_PENDING') AND next_attempt<=? ORDER BY CASE WHEN state='ACK_PENDING' THEN 0 ELSE 1 END,rowid",(time.time(),))
                for row in candidates:
                    if row['resource_id'] in self.inflight or row['batch_id'] in self.inflight.values():continue
                    c.execute("UPDATE resources SET state='RUNNING',attempts=attempts+1,updated=? WHERE resource_id=?",(time.time(),row['resource_id']))
                    selected=dict(row);break
            if selected:self.inflight[selected['resource_id']]=selected['batch_id']
            return selected
    def _manifest(self,row,manifest):
        if not isinstance(manifest,dict):raise IntegrityError('missing manifest')
        digest=manifest.get('manifest_sha256');content={k:v for k,v in manifest.items() if k!='manifest_sha256'}
        if digest!=hash_obj(content) or manifest.get('resource_id')!=row['resource_id'] or manifest.get('key')!=row['resource_key'] or manifest.get('source_id')!=row['source_id']:raise IntegrityError('manifest binding mismatch')
        files=manifest.get('files');seen=set();total=0
        if not isinstance(files,list) or not files:raise IntegrityError('empty resource')
        for f in files:
            if not isinstance(f,dict) or set(f)!={'name','size','sha256'}:raise IntegrityError('file entry fields are invalid')
            name=safe_name(f['name'])
            if name.casefold() in seen or type(f['size']) is not int or f['size']<0 or not isinstance(f.get('sha256'),str) or not re.fullmatch('[0-9a-f]{64}',f['sha256']):raise IntegrityError('invalid or colliding file entry')
            seen.add(name.casefold());total+=f['size']
        if type(manifest.get('total_bytes')) is not int or total!=manifest['total_bytes']:raise IntegrityError('manifest total mismatch')
        return manifest
    def _output_locations(self, row):
        paths = self.cfg['paths']
        source_id = safe_name(row['source_id'])
        mapped = paths['source_output_dirs'].get(source_id)
        relative = safe_key(row['resource_key'], allow_root=mapped is not None)
        if mapped is None and paths.get('include_source_directory', True):
            relative = Path(source_id) / relative
        for raw in (mapped if mapped is not None else paths['output_dirs']):
            yield Path(raw).expanduser().absolute(), relative

    def _partial(self, row, target, name):
        if row['source_id'] in self.cfg['paths']['source_output_dirs']:
            return target / f".{safe_name(row['resource_id'])}.{hash_obj(name)[:16]}.part"
        return target / (name + '.part')

    def _validate_output(self,row,target,identity=None):
        target=Path(target).absolute()
        allowed=False
        for root, relative in self._output_locations(row):
            if target==root/relative:
                if root.resolve()!=root:raise IntegrityError('configured output root contains a symlink or junction')
                allowed=True;break
        if not allowed or target.resolve()!=target or target.is_symlink() or target.is_relative_to(self.state):raise IntegrityError('output directory escaped configured roots')
        if not target.is_dir():raise IntegrityError('output directory disappeared')
        info=target.stat();current=(info.st_dev,info.st_ino)
        if identity is not None and current!=identity:raise IntegrityError('output directory changed during transfer')
        return current
    def _choose_output(self,row,manifest):
        if row.get('output_dir'):
            target=Path(row['output_dir']);self._validate_output(row,target);return target
        import shutil
        for root, relative in self._output_locations(row):
            target=root/relative
            if target.resolve()!=target or target.is_symlink() or target.is_relative_to(self.state):raise IntegrityError('unsafe output directory')
            target.mkdir(parents=True,exist_ok=True);self._validate_output(row,target);missing=0;collision=False
            for f in manifest['files']:
                p=target/f['name']
                if p.is_symlink():raise IntegrityError('symlink output is forbidden')
                if p.exists():
                    if hash_file(p)!=(f['sha256'],f['size']):collision=True;break
                else:
                    part=self._partial(row,target,f['name'])
                    if part.is_symlink():raise IntegrityError('symlink partial file')
                    already=part.stat().st_size if part.exists() else 0
                    if already>f['size']:raise IntegrityError('oversized partial file')
                    missing+=f['size']-already
            if collision:continue
            if shutil.disk_usage(target).free>=missing+self.floor:return target
        raise CapacityError('No configured output directory has enough space; preserve all existing files')
    def _download_files(self,row,manifest,token,target):
        import shutil
        identity=self._validate_output(row,target);started=time.monotonic();transferred=0
        for f in manifest['files']:
            self._validate_output(row,target,identity)
            final=target/f['name'];part=self._partial(row,target,f['name'])
            if final.is_symlink() or part.is_symlink():raise IntegrityError('symlink output is forbidden')
            if final.exists():
                if hash_file(final)!=(f['sha256'],f['size']):raise IntegrityError('different existing output preserved')
                continue
            if part.is_symlink():raise IntegrityError('symlink partial file')
            offset=part.stat().st_size if part.exists() else 0
            if offset>f['size']:raise IntegrityError('partial file too large')
            with part.open('ab') as stream:
                last_progress=0.0
                while offset<f['size']:
                    self._validate_output(row,target,identity)
                    if self.stop.is_set():raise TemporaryError('receiver stopping')
                    length=min(self.chunk,f['size']-offset)
                    if shutil.disk_usage(target).free<length+self.floor:raise CapacityError('paused_insufficient_disk')
                    data=self._request('get',resource_id=row['resource_id'],lease_token=token,name=f['name'],offset=offset,length=length)
                    if len(data)!=length:raise IntegrityError('range size mismatch')
                    self._validate_output(row,target,identity)
                    stream.write(data);offset+=len(data);transferred+=len(data)
                    if time.monotonic()-last_progress>5:
                        progress={'resource_id':row['resource_id'],'key':row['resource_key'],'file':f['name'],'received':offset,'total':f['size'],'at':time.time(),
                                  'session_bytes':transferred,'bytes_per_second':transferred/max(.001,time.monotonic()-started)}
                        with self.progress_lock:
                            write_json(self.state/'progress'/f"{row['resource_id']}.json",progress)
                            write_json(self.state/'progress.json',progress)
                        last_progress=time.monotonic()
                stream.flush();os.fsync(stream.fileno())
            self._validate_output(row,target,identity)
            if part.is_symlink() or hash_file(part)!=(f['sha256'],f['size']):raise IntegrityError('download SHA mismatch; partial retained for diagnosis')
            if final.exists():raise IntegrityError('output appeared during download; do not overwrite')
            rename_new(part,final)
        self._validate_output(row,target,identity)
    def _process(self,row):
        rid=row['resource_id'];receipt_path=self.receipts/(rid+'.json')
        delivered=read_json(receipt_path) if receipt_path.exists() else None
        claim=self._request('claim',resource_id=rid,request_id=row['request_id'])
        if claim.get('status')=='superseded':
            if row.get('manifest') or row.get('lease_token') or row.get('output_dir') or delivered:raise IntegrityError('Superseded resource already has local transfer state')
            self._set(rid,'SKIPPED',error='unclaimed resource replaced by smaller server resources');return
        if claim.get('status') in ('acked','already_acked'):
            if not delivered:raise IntegrityError('remote ACKED without this local receipt')
            self._set(rid,'DONE',error=None);return
        if claim.get('status')=='busy':raise TemporaryError('resource leased by another request')
        if claim.get('status')!='claimed':raise ProtocolError('unexpected resource claim response')
        token=claim['lease_token'];manifest=self._manifest(row,claim.get('manifest') or self._request('manifest',resource_id=rid,lease_token=token))
        self._set(rid,'RUNNING',lease_token=token,manifest=json.dumps(manifest),error=None)
        if delivered:
            if delivered['manifest_sha256']!=manifest['manifest_sha256']:raise IntegrityError('receipt belongs to a different resource version')
            if row['source_id'] in self.cfg['paths']['source_output_dirs']:
                target=Path(delivered['output_dir']);self._validate_output(row,target)
                for file in manifest['files']:
                    path=target/file['name']
                    if path.is_symlink() or not path.is_file() or hash_file(path)!=(file['sha256'],file['size']):
                        raise IntegrityError('local output changed after receipt; server file preserved')
        else:
            target=self._choose_output(row,manifest);self._set(rid,'RUNNING',output_dir=str(target));self._download_files(row,manifest,token,target)
            delivered={'schema':1,'resource_id':rid,'producer_id':self.producer,'consumer_id':self.consumer,'manifest_sha256':manifest['manifest_sha256'],'files':manifest['files'],'output_dir':str(target),'delivered_at':time.time(),'status':'DELIVERED'}
            write_json(receipt_path,delivered)
        self._set(rid,'ACK_PENDING',lease_token=token,error=None)
        ack=self._request('ack',resource_id=rid,lease_token=token,receipt={'manifest_sha256':manifest['manifest_sha256'],'files':manifest['files'],'local_receipt_sha256':hash_obj(delivered)})
        if ack.get('status') not in ('acked','already_acked'):raise ProtocolError('ACK was not accepted')
        delivered['status']='ACKED';delivered['acked_at']=time.time();write_json(receipt_path,delivered);self._set(rid,'DONE',error=None)
    def work_once(self):
        if not self.worker_lock.acquire(blocking=False):return False
        row=None
        try:
            row=self._next()
            if row is None:return False
            self._status('transferring')
            try:self._process(row)
            except CapacityError as e:self._set(row['resource_id'],'RETRY',error=str(e),next_attempt=time.time()+60)
            except (IntegrityError,ProtocolError) as e:self._set(row['resource_id'],'HOLD',error=str(e),next_attempt=0)
            except Exception as e:
                code=getattr(e,'code','')
                held=code in ('SOURCE_CHANGED','INTEGRITY','UNSAFE_PATH','AUTH','BAD_ACK','BAD_REQUEST','BAD_RANGE','INVALID_IMAGE','HOLD')
                self._set(row['resource_id'],'HOLD' if held else 'RETRY',error=str(e)[:500],next_attempt=time.time()+min(300,10*2**min(row['attempts'],5)))
            self._status('worker');return True
        except Exception:
            if row:
                with self.claim_lock:self.failed_workers.add(row['resource_id'])
            raise
        finally:
            if row:
                with self.claim_lock:self.inflight.pop(row['resource_id'],None)
            self.worker_lock.release()
    def _recover_failed_workers(self):
        with self.claim_lock:
            recovered=[]
            with self.tx() as c:
                for rid in self.failed_workers:
                    if rid in self.inflight:continue
                    c.execute("UPDATE resources SET state='RETRY',next_attempt=0 WHERE resource_id=? AND state='RUNNING'",(rid,))
                    recovered.append(rid)
            self.failed_workers.difference_update(recovered)
    def _status(self,reason):
        with self.tx() as c:
            rows=[dict(row) for row in c.execute('SELECT * FROM resources')]
            batches=self._batch_rows(c)
        counts={s:sum(r['state']==s for r in rows) for s in (*self.ACTIVE,'DONE','SKIPPED')}
        pending_batches=sum(row['pending_resource_count']>0 for row in batches)
        with self.claim_lock:active_channels=len(self.inflight)
        try:write_json(self.state/'status.json',{'at':time.time(),'reason':reason,'queue_limit':self.capacity,'queue_unit':'batches','pending_batches':pending_batches,
            'pending_resources':sum(r['state'] not in ('DONE','SKIPPED') for r in rows),'batch_counts':{'pending':pending_batches,'complete':len(batches)-pending_batches},
            'counts':counts,'poll_seconds':self.policy.interval_seconds,'download_workers':self.workers,'active_channels':active_channels,'target_batch_bytes':self.target_batch_bytes})
        except OSError as error:self._log_error('status_error',error)
    @staticmethod
    def _log_error(event,error):
        print(json.dumps({'event':event,'type':type(error).__name__,'message':str(error)[:400]}),file=sys.stderr,flush=True)
    def run(self,once=False):
        with process_lock(self.state/'b_instance.lock'):
            with self.tx() as c:c.execute("UPDATE resources SET state='RETRY',next_attempt=0 WHERE state='RUNNING'")
            if once:
                self.poll()
                while self.work_once():pass
                return
            def worker():
                recover=False
                while not self.stop.is_set():
                    try:
                        if recover:
                            self._recover_failed_workers()
                            recover=False
                        if not self.work_once():self.stop.wait(1)
                    except Exception as error:
                        self._log_error('worker_error_retry',error);recover=True;self.stop.wait(10)
            threads=[threading.Thread(target=worker,daemon=True,name=f'aas-download-{i+1}') for i in range(self.workers)]
            for thread in threads:thread.start()
            retry=RetryPolicy(10,300,True)
            try:
                while not self.stop.is_set():
                    delay=self.policy.next_delay_seconds(PollContext(time.monotonic(),self.pending_count()>0,False))
                    try:self.poll();retry.reset()
                    except Exception as error:
                        self._log_error('poll_error_retry',error)
                        try:self._status('poll_error:'+str(error)[:200])
                        except Exception as status_error:self._log_error('status_error',status_error)
                        delay=min(delay,retry.next_delay())
                    self.stop.wait(delay)
            finally:
                self.stop.set()
                for thread in threads:thread.join(timeout=200)
                self.transport.close()
