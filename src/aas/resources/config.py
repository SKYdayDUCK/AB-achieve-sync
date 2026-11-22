"""Local preset defaults; no configuration is accepted from the remote node."""
from __future__ import annotations
import copy
from pathlib import Path
import yaml
from aas.common.core import IMAGE_SUFFIXES, ProtocolError, validate_id

def load_config(path, preset='files', side='a'):
    data=copy.deepcopy(path if isinstance(path,dict) else yaml.safe_load(Path(path).read_text(encoding='utf-8-sig')))
    if not isinstance(data,dict) or preset not in ('files','images','lora'):raise ProtocolError('invalid resource configuration or preset')
    data['preset']=preset
    node=data.setdefault('node',{})
    for field in ('producer_id','consumer_id'):validate_id(node[field])
    paths=data.setdefault('paths',{})
    if not paths.get('root_dir'):raise ProtocolError('paths.root_dir is required')
    if side=='a':
        sources=data.get('sources')
        if sources is None:
            raw=paths.get('input_dirs') or [paths.get('input_dir')]
            if not all(raw):raise ProtocolError('sources or paths.input_dir is required')
            sources=[{'id':f'input-{i+1}','root':str(p),'subdir_glob':'.'} for i,p in enumerate(raw)]
        seen=set()
        for source in sources:
            source_preset=source.get('preset',preset)
            if source_preset not in ('files','images','lora'):raise ProtocolError('invalid source preset')
            validate_id(source['id'])
            if source['id'] in seen:raise ProtocolError('duplicate source id')
            seen.add(source['id'])
            if not source.get('root'):raise ProtocolError('source root is required')
            source.setdefault('subdir_glob','*' if source_preset=='lora' else '.')
            source.setdefault('suffixes',['.safetensors'] if source_preset=='lora' else sorted(IMAGE_SUFFIXES) if source_preset=='images' else ['*'])
            source.setdefault('min_age_seconds',10)
            source.setdefault('completion_marker','TRAIN_COMPLETE.json' if source_preset=='lora' else None)
            if source_preset=='lora':
                source.setdefault('checkpoint_min_step',600);source.setdefault('include_final',True);source.setdefault('max_train_steps',1500)
            if source_preset=='images':
                source.setdefault('verify_images',True);source.setdefault('hold_back_newest',1);source.setdefault('idle_flush_seconds',300)
            source.setdefault('cleanup',{'enabled':False})
        data['sources']=sources;data.setdefault('lease',{}).setdefault('seconds',1200);data.setdefault('scan_interval_seconds',60)
    else:
        scheduler=data.setdefault('scheduler',{})
        if scheduler.get('strategy','fixed_interval')!='fixed_interval':raise ProtocolError('unsupported polling policy')
        scheduler.setdefault('fixed_interval',{}).setdefault('interval_seconds',600)
        # Queue capacity counts unfinished discovery batches, not their resources.
        transfer=data.setdefault('transfer',{});transfer.setdefault('max_download_queue',4);transfer.setdefault('chunk_bytes',8*1024**2)
        transfer.setdefault('target_batch_bytes',0);transfer.setdefault('download_workers',1)
        data.setdefault('capacity',{}).setdefault('min_free_bytes',2*1024**3)
        paths.setdefault('output_dirs',[str(Path(paths['root_dir'])/'output')])
        mapped_outputs = paths.setdefault('source_output_dirs', {})
        if not isinstance(mapped_outputs, dict):
            raise ProtocolError('paths.source_output_dirs must map source IDs to directory lists')
        for source_id, roots in mapped_outputs.items():
            validate_id(source_id)
            if not isinstance(roots, list) or not roots or any(not isinstance(root, str) or not root.strip() for root in roots):
                raise ProtocolError('Mapped source needs a nonempty list of output directories')
        data.setdefault('queue',{'jobs':[{'source_id':'*','key':'*'}]})
    return data
