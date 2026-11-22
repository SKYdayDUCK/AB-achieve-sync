"""Preset routing, strict forced-shell boundary and configuration regressions."""
from contextlib import redirect_stdout
from copy import deepcopy
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from aas import cli as legacy_cli
from aas.common.core import IMAGE_SUFFIXES, ProtocolError, canonical
from aas.resources import cli
from aas.resources.client import ResourceClient
from aas.resources.config import load_config
from aas.resources.server import ResourceServer
from aas.resources.transport import RemoteError


class DirectTransport:
    def __init__(self,server):
        self.server=server
        self.calls=[]
    def call(self,request,binary=False):
        self.calls.append(request)
        result=self.server.dispatch(request)
        if isinstance(result,dict) and result.get('status')=='error':
            raise RemoteError(result['code'],result.get('message',''))
        return result
    def close(self):
        pass


class ResourceCLITests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='aas-cli-test-')
        self.root=Path(self.temp.name).resolve()
        self.source=self.root/'incoming'/'hero'
        self.source.mkdir(parents=True)
        (self.source/'data.bin').write_bytes(bytes(range(64)))
        self.acfg={'node':{'producer_id':'A','consumer_id':'B'},
            'paths':{'root_dir':str(self.root/'A-state')},
            'sources':[{'id':'source','root':str(self.source.parent),'subdir_glob':'*','suffixes':['.bin'],'min_age_seconds':0}]}
        self.bcfg={'node':{'producer_id':'A','consumer_id':'B'},
            'paths':{'root_dir':str(self.root/'B-state'),'output_dirs':[str(self.root/'output')]},
            'capacity':{'min_free_bytes':0},'remote':{'host':'unused','user':'unused','identity_file':'unused'}}

    def tearDown(self):
        self.assertTrue(self.root.name.startswith('aas-cli-test-'))
        self.temp.cleanup()

    def shell(self,obj,**kwargs):
        sink=BytesIO()
        raw=canonical(obj) if not isinstance(obj,bytes) else obj
        cli.shell(self.acfg,input_stream=BytesIO(raw),output_stream=sink,**kwargs)
        return sink.getvalue()

    def test_outer_A_B_route_all_presets_and_equals_forms_without_legacy_service(self):
        for side in ('a','b'):
            for preset in ('images','files','lora'):
                for argument in (['--preset',preset],['--preset='+preset]):
                    with self.subTest(side=side,preset=preset,argument=argument):
                        argv=['aas-'+side,'--config','not-read',*argument,'status']
                        with patch.object(sys,'argv',argv),patch.object(cli,'main_'+side,return_value='resource-route') as routed,patch.object(legacy_cli,'AService' if side=='a' else 'BService') as old:
                            self.assertEqual(getattr(legacy_cli,'main_'+side)(),'resource-route')
                            routed.assert_called_once_with()
                            old.assert_not_called()

    def test_legacy_help_without_preset_still_has_old_commands(self):
        for name,expected in (('a','retry-blocked'),('b','resolve-handoff'),('shell','emergency')):
            output=StringIO()
            with patch.object(sys,'argv',['aas-'+name,'--help']),redirect_stdout(output):
                with self.assertRaises(SystemExit) as exited:
                    getattr(legacy_cli,'main_'+name)()
            self.assertEqual(exited.exception.code,0)
            self.assertIn(expected,output.getvalue())

    def test_default_files_images_and_lora_configurations_do_not_mutate_input(self):
        raw={'node':{'producer_id':'A','consumer_id':'B'},'paths':{'root_dir':str(self.root/'state'),'input_dir':str(self.root/'future')}}
        before=deepcopy(raw)
        files=load_config(raw,'files','a')
        self.assertEqual(files['sources'][0]['suffixes'],['*'])
        self.assertEqual(files['sources'][0]['subdir_glob'],'.')
        self.assertFalse(files['sources'][0]['cleanup']['enabled'])
        images=load_config(raw,'images','a')['sources'][0]
        self.assertEqual(set(images['suffixes']),set(IMAGE_SUFFIXES))
        self.assertTrue(images['verify_images'])
        self.assertEqual(images['hold_back_newest'],1)
        self.assertEqual(images['idle_flush_seconds'],300)
        lora=load_config(raw,'lora','a')['sources'][0]
        self.assertEqual(lora['suffixes'],['.safetensors'])
        self.assertEqual(lora['checkpoint_min_step'],600)
        self.assertEqual(lora['completion_marker'],'TRAIN_COMPLETE.json')
        self.assertTrue(lora['include_final'])
        self.assertEqual(lora['max_train_steps'],1500)
        self.assertFalse(lora['cleanup']['enabled'])
        bcfg=load_config(self.bcfg,side='b')
        self.assertEqual(bcfg['scheduler']['fixed_interval']['interval_seconds'],600)
        self.assertEqual(bcfg['transfer']['max_download_queue'],4)
        self.assertEqual(raw,before)
        self.assertFalse((self.root/'future').exists())
        self.assertEqual(cli.parser('a').parse_args(['--config','x','status']).preset,'files')

    def test_resource_A_scan_and_B_init_use_local_config_without_network(self):
        apath=self.root/'a.json';apath.write_text(json.dumps(self.acfg))
        output=StringIO()
        with redirect_stdout(output):
            cli.main_a(['--config',str(apath),'--preset=files','scan'])
        self.assertEqual(json.loads(output.getvalue())['ready'],1)
        bpath=self.root/'b.json';bpath.write_text(json.dumps(self.bcfg))
        with patch('aas.resources.transport.ResourceSSHTransport._connect',side_effect=AssertionError('No network allowed')),redirect_stdout(StringIO()):
            cli.main_b(['--config',str(bpath),'--preset','files','init'])
        self.assertTrue((self.root/'B-state/state/status.json').exists())

    def test_shell_only_exact_forced_command_and_never_constructs_server_on_rejection(self):
        for command in ('','aas-res extra',' aas-res','aas-res ','aas-res; echo unsafe','aas get','aas-res\n'):
            with self.subTest(command=command),patch('aas.resources.server.ResourceServer') as server,patch.dict(os.environ,{'SSH_ORIGINAL_COMMAND':command}):
                with self.assertRaises(ProtocolError):
                    self.shell({'op':'catalog'})
                server.assert_not_called()

    def test_shell_oversized_input_rejected_before_server_or_filesystem(self):
        with patch('aas.resources.server.ResourceServer') as server:
            with self.assertRaises(ProtocolError):
                self.shell(b'x'*(1024**2+1),original_command='aas-res')
            server.assert_not_called()
        self.assertFalse((self.root/'A-state').exists())

    def test_shell_catalog_claim_and_get_raw_bytes_with_bound_identity(self):
        with patch.dict(os.environ,{'SSH_ORIGINAL_COMMAND':'aas-res'}):
            catalog=json.loads(self.shell({'op':'catalog','producer_id':'A','consumer_id':'B'}))
            rid=catalog['resources'][0]['resource_id']
            claim=json.loads(self.shell({'op':'claim','resource_id':rid,'request_id':'first','consumer_id':'B'}))
            data=self.shell({'op':'get','resource_id':rid,'lease_token':claim['lease_token'],'name':'data.bin','offset':0,'length':8})
            self.assertEqual(data,bytes(range(8)))
            wrong=json.loads(self.shell({'op':'catalog','producer_id':'wrong'}))
            self.assertEqual(wrong['code'],'AUTH')

    def test_shell_malformed_json_arrays_and_extra_objects_are_protocol_errors(self):
        for raw in (b'not-json',b'[]',b'{} {}',b'null'):
            with self.subTest(raw=raw):
                response=json.loads(self.shell(raw,original_command='aas-res'))
                self.assertEqual(response['status'],'error')
                self.assertEqual(response['code'],'PROTOCOL')
        self.assertEqual((self.source/'data.bin').read_bytes(),bytes(range(64)))

    def test_resource_shell_entrypoint_routes_preset_without_connecting(self):
        with patch.object(cli,'shell') as shell:
            cli.main_shell(['--config','not-read','--preset=lora'])
        shell.assert_called_once_with('not-read','lora')

    def test_empty_local_queue_and_explicit_disabled_job_enqueue_nothing(self):
        server=ResourceServer(self.acfg)
        for jobs in ([],[{'source_id':'source','key':'hero','enabled':False}]):
            cfg=deepcopy(self.bcfg);cfg['queue']={'jobs':jobs}
            transport=DirectTransport(server)
            client=ResourceClient(cfg,transport=transport)
            self.assertEqual(client.poll()['enqueued'],0)
            self.assertEqual(client.rows(),[])

    def test_invalid_local_queue_shapes_types_and_unknown_fields_rejected_without_enqueue(self):
        server=ResourceServer(self.acfg)
        bad=['not-list',[None],[5],[{'source_id':5,'key':'hero'}],[{'source_id':'source','key':None}],
             [{'source_id':'source','key':'hero','enabled':'false'}],[{'source_id':'source','ky':'hero'}]]
        for index,jobs in enumerate(bad):
            with self.subTest(jobs=jobs):
                cfg=deepcopy(self.bcfg);cfg['paths']['root_dir']=str(self.root/('bad-B-'+str(index)))
                cfg['queue']={'jobs':jobs}
                client=ResourceClient(cfg,transport=DirectTransport(server))
                with self.assertRaises(ProtocolError):
                    client.poll()
                self.assertEqual(client.rows(),[])
        self.assertEqual((self.source/'data.bin').read_bytes(),bytes(range(64)))


if __name__=='__main__':
    unittest.main()
