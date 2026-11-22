"""Persistent strict-host-key SSH; fixed forced command, JSON control/range bytes."""
from __future__ import annotations
import json,threading,time
from pathlib import Path
from aas.common.core import ProtocolError, TemporaryError, AuthenticationError, canonical

class RemoteError(TemporaryError):
    def __init__(self,code,message):self.code=code;super().__init__(f'{code}: {message}')

class ResourceSSHTransport:
    def __init__(self,remote):
        self.cfg=remote;self.client=None;self.lock=threading.RLock()
        self.independent=remote.get('independent_connections',False)
        if type(self.independent) is not bool:raise ProtocolError('independent_connections must be boolean')
        self.local=threading.local();self.clients=set()
    def _connect(self):
        import paramiko
        current=getattr(self.local,'client',None) if self.independent else self.client
        if current and current.get_transport() and current.get_transport().is_active():return current
        if current:current.close();self.clients.discard(current)
        client=paramiko.SSHClient();client.load_system_host_keys()
        known=self.cfg.get('known_hosts_file')
        if known:client.load_host_keys(str(Path(known).expanduser()))
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
        try:
            client.connect(self.cfg['host'],port=int(self.cfg.get('port',22)),username=self.cfg['user'],key_filename=str(Path(self.cfg['identity_file']).expanduser()),
                allow_agent=False,look_for_keys=False,timeout=45,banner_timeout=60,auth_timeout=45)
        except (paramiko.AuthenticationException,paramiko.BadHostKeyException) as error:
            client.close();raise AuthenticationError(str(error)) from error
        except (paramiko.SSHException,OSError,EOFError) as error:
            client.close();raise TemporaryError(str(error)) from error
        client.get_transport().set_keepalive(20);self.clients.add(client)
        if self.independent:self.local.client=client
        else:self.client=client
        return client
    def call(self,request,binary=False):
        raw=canonical(request)
        if len(raw)>1024**2:raise ProtocolError('oversized resource request')
        with self.lock:
            client=self._connect();channel=client.get_transport().open_session(window_size=32*1024**2,max_packet_size=131072,timeout=45)
        timeout=600 if request['op'] in ('claim','ack') else 180
        channel.settimeout(timeout);chunks=[];errors=[];size=0
        try:
            channel.exec_command('aas-res');channel.sendall(raw);channel.shutdown_write();deadline=time.monotonic()+timeout
            while True:
                if channel.recv_ready():
                    piece=channel.recv(1024**2);chunks.append(piece);size+=len(piece)
                    if size>20*1024**2:raise ProtocolError('oversized resource response')
                if channel.recv_stderr_ready():errors.append(channel.recv_stderr(4096))
                # SSH exit-status is independent of stream completion. In
                # particular, the server may send it while later stdout packets
                # are still in flight and the local receive buffer is empty.
                ended=channel.eof_received or channel.closed
                if ended and channel.exit_status_ready() and not channel.recv_ready() and not channel.recv_stderr_ready():break
                if time.monotonic()>deadline:
                    expected=request.get('length') if binary else None
                    stage='await-stream-end' if ended or (binary and size==expected) else ('receive-body' if size else 'await-first-byte')
                    raise TemporaryError(f'resource SSH channel timeout: stage={stage} received={size} expected={expected} '
                        f'eof={bool(channel.eof_received)} closed={bool(channel.closed)} '
                        f'exit_status_ready={bool(channel.exit_status_ready())} stderr_bytes={sum(map(len,errors))}')
                time.sleep(.005)
            body=b''.join(chunks);code=channel.recv_exit_status()
            if code:raise TemporaryError(f'resource SSH exit {code}: '+b''.join(errors).decode('utf-8','replace')[:500])
            if binary and len(body)==request['length']:return body
            try:answer=json.loads(body)
            except ValueError as error:
                if binary:raise TemporaryError(f'short range response: expected {request["length"]} bytes, received {len(body)}') from error
                raise ProtocolError('invalid resource response') from error
            if not isinstance(answer,dict):
                if binary:raise TemporaryError(f'short range response: expected {request["length"]} bytes, received {len(body)}')
                raise ProtocolError('resource control response must be object')
            if answer.get('status')=='error':raise RemoteError(answer.get('code','REMOTE_ERROR'),answer.get('message','')[:500])
            if binary:raise TemporaryError(f'short range response: expected {request["length"]} bytes, received {len(body)}')
            return answer
        finally:channel.close()
    def close(self):
        with self.lock:
            for client in self.clients:client.close()
            self.clients.clear();self.client=None;self.local=threading.local()
