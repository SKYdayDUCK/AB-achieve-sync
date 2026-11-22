"""Preset entry points and a fixed-command SSH resource RPC shell."""
from __future__ import annotations
import argparse,json,os,sys,time
from pathlib import Path
from aas.common.core import AASError,ProtocolError,canonical,process_lock
from .config import load_config

def parser(side):
    p=argparse.ArgumentParser(prog='aas-'+side);p.add_argument('--config',required=True);p.add_argument('--preset',choices=('files','images','lora'),default='files')
    p.add_argument('action',choices=('run','once','scan','status','init','dispatch','retry'));p.add_argument('--resource-id')
    return p

def main_a(argv=None):
    from .server import ResourceServer
    args=parser('a').parse_args(argv);cfg=load_config(args.config,args.preset,'a');server=ResourceServer(cfg)
    if args.action=='dispatch':return shell(args.config,args.preset)
    if args.action in ('once','scan','init'):print(json.dumps(server.scan(),ensure_ascii=False));return
    if args.action=='status':print(json.dumps(server.status(),ensure_ascii=False));return
    if args.action!='run':raise ProtocolError('unsupported A action')
    root=Path(cfg['paths']['root_dir']);root.mkdir(parents=True,exist_ok=True)
    with process_lock(root/'a_daemon.lock'):
        while True:
            try:
                latest=load_config(args.config,args.preset,'a')
                if Path(latest['paths']['root_dir']).resolve()!=root.resolve():raise ProtocolError('restart A after changing state root')
                server=ResourceServer(latest);server.scan();cfg=latest
            except Exception as error:print(json.dumps({'event':'scan_error','type':type(error).__name__,'message':str(error)[:400]}),file=sys.stderr,flush=True)
            time.sleep(float(cfg.get('scan_interval_seconds',60)))

def main_b(argv=None):
    from .client import ResourceClient
    args=parser('b').parse_args(argv);client=ResourceClient(args.config,preset=args.preset)
    if args.action=='status':print(json.dumps([{k:r[k] for k in ('resource_id','batch_id','source_id','resource_key','state','attempts','output_dir','error')} for r in client.rows()],ensure_ascii=False));return
    if args.action=='init':client._status('initialized');print(json.dumps({'root':str(client.root)}));return
    if args.action=='retry':
        if not args.resource_id:raise ProtocolError('--resource-id is required')
        client._set(args.resource_id,'RETRY',next_attempt=0,error=None);return
    if args.action not in ('run','once'):raise ProtocolError('unsupported B action')
    try:client.run(once=args.action=='once')
    finally:client.transport.close()

def shell(config,preset='files',input_stream=None,output_stream=None,original_command=None):
    from .server import ResourceServer
    command=os.environ.get('SSH_ORIGINAL_COMMAND','') if original_command is None else original_command
    if command!='aas-res':raise ProtocolError('only the fixed aas-res command is permitted')
    source=input_stream or sys.stdin.buffer;sink=output_stream or sys.stdout.buffer
    request=source.read(1024**2+1)
    if len(request)>1024**2:raise ProtocolError('oversized resource RPC request')
    try:
        obj=json.loads(request)
        if not isinstance(obj,dict):raise ValueError('request is not object')
        response=ResourceServer(load_config(config,preset,'a')).dispatch(obj)
    except (ValueError,AASError) as error:response={'status':'error','code':'PROTOCOL','message':str(error)[:400]}
    sink.write(response if isinstance(response,bytes) else canonical(response));sink.flush()

def main_shell(argv=None):
    p=argparse.ArgumentParser(prog='aas-res-shell');p.add_argument('--config',required=True);p.add_argument('--preset',choices=('files','images','lora'),default='files');args=p.parse_args(argv);shell(args.config,args.preset)
