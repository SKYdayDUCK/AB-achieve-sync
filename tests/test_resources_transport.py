"""SSH channel framing regressions; scripted byte streams, no SSH or user files."""
import hashlib
import json
from pathlib import Path
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from aas.common.core import TemporaryError, canonical
from aas.resources.transport import RemoteError, ResourceSSHTransport


class ScriptedChannel:
    """Exit status may precede stdout, stderr, EOF and SSH channel close."""
    def __init__(self, events, code=0, status_ready=True):
        self.events = list(events)
        self.stdout = bytearray()
        self.stderr = bytearray()
        self.code = code
        self.status_ready = status_ready
        self.eof_received = False
        self.closed = False
        self.closed_before_eof = False
        self.exit_read_before_eof = False
        self.tick()

    def tick(self, *_):
        if not self.events:
            return
        event = self.events.pop(0)
        self.stdout.extend(event.get('stdout', b''))
        self.stderr.extend(event.get('stderr', b''))
        self.eof_received = event.get('eof', self.eof_received)
        self.status_ready = event.get('status_ready', self.status_ready)
        self.closed = event.get('closed', self.closed)

    def settimeout(self, _): pass
    def exec_command(self, command):
        if command != 'aas-res':
            raise AssertionError('unexpected forced command')
    def sendall(self, _): pass
    def shutdown_write(self): pass
    def recv_ready(self): return bool(self.stdout)
    def recv_stderr_ready(self): return bool(self.stderr)
    def exit_status_ready(self): return self.status_ready or self.closed
    def recv(self, size):
        result = bytes(self.stdout[:size])
        del self.stdout[:size]
        return result
    def recv_stderr(self, size):
        result = bytes(self.stderr[:size])
        del self.stderr[:size]
        return result
    def recv_exit_status(self):
        self.exit_read_before_eof = not (self.eof_received or self.closed)
        return self.code
    def close(self):
        self.closed_before_eof = not (self.eof_received or self.closed)
        self.closed = True


class ResourceTransportTests(unittest.TestCase):
    def test_four_concurrent_channels_reuse_one_strict_host_key_connection(self):
        self.assert_connection_mode(independent=False)

    def test_independent_threads_reuse_own_connections_and_close_all(self):
        self.assert_connection_mode(independent=True)

    def assert_connection_mode(self, independent):
        barrier = threading.Barrier(4, timeout=10)
        channels = []
        clients = []

        class Channel(ScriptedChannel):
            def __init__(self):
                super().__init__([])

            def exec_command(self, command):
                super().exec_command(command)
                # Every RPC must reach its own channel concurrently. A lock
                # spanning the full response would deadlock this barrier.
                barrier.wait()

            def sendall(self, body):
                request = json.loads(body)
                self.stdout.extend(bytes([request['offset'] + 1]) * request['length'])

            def shutdown_write(self):
                self.eof_received = True

        class SSH:
            def __init__(self):
                self.active = True
                self.threads = set()
                self.channels = []
            def is_active(self): return self.active
            def set_keepalive(self, value): self.keepalive = value
            def open_session(self, **kwargs):
                channel = Channel()
                channels.append(channel)
                self.channels.append(channel)
                self.threads.add(threading.get_ident())
                return channel

        class Client:
            def __init__(self):
                self.connects = self.closes = 0
                self.ssh = SSH()
                clients.append(self)
            def load_system_host_keys(self): self.system_keys_loaded = True
            def load_host_keys(self, path): self.known_hosts = path
            def set_missing_host_key_policy(self, policy): self.policy = policy
            def connect(self, host, **kwargs):
                self.connects += 1
                self.connection_options = kwargs
            def get_transport(self): return self.ssh
            def close(self):
                self.closes += 1
                self.ssh.active = False

        class RejectPolicy: pass
        fake_paramiko = SimpleNamespace(SSHClient=Client, RejectPolicy=RejectPolicy,
                                       AuthenticationException=type('AuthenticationException', (Exception,), {}),
                                       BadHostKeyException=type('BadHostKeyException', (Exception,), {}),
                                       SSHException=type('SSHException', (Exception,), {}))
        transport = ResourceSSHTransport({'host': 'example.invalid', 'user': 'test',
                                          'identity_file': 'test-key', 'known_hosts_file': 'known-hosts',
                                          **({'independent_connections': True} if independent else {})})

        def get(index):
            return transport.call({'op': 'get', 'offset': index, 'length': 4096}, binary=True)

        try:
            with patch.dict(sys.modules, {'paramiko': fake_paramiko}), ThreadPoolExecutor(max_workers=4) as pool:
                for _ in range(2):
                    results = list(pool.map(get, range(4)))
                    for index, result in enumerate(results):
                        self.assertEqual(result, bytes([index + 1]) * 4096)
            self.assertEqual(len(clients), 4 if independent else 1)
            self.assertEqual(len(channels), 8)
            self.assertEqual(len({id(channel) for channel in channels}), 8)
            self.assertTrue(all(channel.closed for channel in channels))
            for client in clients:
                self.assertEqual(client.connects, 1)
                self.assertTrue(client.system_keys_loaded)
                self.assertEqual(client.known_hosts, 'known-hosts')
                self.assertIsInstance(client.policy, RejectPolicy)
                self.assertFalse(client.connection_options['allow_agent'])
                self.assertFalse(client.connection_options['look_for_keys'])
                self.assertEqual(client.ssh.keepalive, 20)
                self.assertEqual(len(client.ssh.channels), 2 if independent else 8)
                self.assertEqual(len(client.ssh.threads), 1 if independent else 4)
        finally:
            # Main thread owns no independent client, but must close every
            # worker's persistent connection, exactly once.
            transport.close()
            transport.close()
        self.assertTrue(all(client.closes == 1 and not client.ssh.active for client in clients))

    def call(self, channel, *, binary=False, length=8 * 1024**2, clock=None):
        transport = ResourceSSHTransport({})
        ssh = SimpleNamespace(open_session=lambda **_: channel)
        client = SimpleNamespace(get_transport=lambda: ssh)
        request = {'op': 'get' if binary else 'catalog', 'length': length}
        with patch.object(transport, '_connect', return_value=client), \
                patch('aas.resources.transport.time.sleep', side_effect=channel.tick):
            if clock is None:
                return transport.call(request, binary=binary)
            with patch('aas.resources.transport.time.monotonic', side_effect=clock):
                return transport.call(request, binary=binary)

    def test_eight_mib_range_drains_after_early_exit_status_and_empty_gaps(self):
        expected = b'0123456789abcdef' * (512 * 1024)
        self.assertEqual(len(expected), 8 * 1024**2)
        events = []
        for offset in range(0, len(expected), 128 * 1024):
            events.extend([{'stdout': expected[offset:offset + 128 * 1024]}, {}])
        events.append({'eof': True})
        channel = ScriptedChannel(events, status_ready=True)
        actual = self.call(channel, binary=True)
        self.assertEqual(len(actual), len(expected))
        self.assertEqual(hashlib.sha256(actual).digest(), hashlib.sha256(expected).digest())
        self.assertFalse(channel.closed_before_eof)
        self.assertFalse(channel.exit_read_before_eof)

    def test_control_response_waits_for_eof_and_later_exit_status(self):
        expected = {'status': 'ok', 'resources': []}
        body = canonical(expected)
        channel = ScriptedChannel([{'stdout': body[:8]}, {}, {'stdout': body[8:]},
                                   {'eof': True}, {}, {'status_ready': True}], status_ready=False)
        self.assertEqual(self.call(channel), expected)
        self.assertFalse(channel.exit_read_before_eof)

    def test_nonzero_exit_drains_late_stderr_before_reporting(self):
        channel = ScriptedChannel([{'stdout': b'prefix'}, {}, {'stderr': b'late diagnostic'},
                                   {}, {'eof': True}], code=1)
        with self.assertRaisesRegex(TemporaryError, 'late diagnostic'):
            self.call(channel)
        self.assertFalse(channel.closed_before_eof)

    def test_binary_remote_error_keeps_remote_error_classification(self):
        body = canonical({'status': 'error', 'code': 'LEASE_EXPIRED', 'message': 'lease expired'})
        channel = ScriptedChannel([{'stdout': body}, {'eof': True}])
        with self.assertRaises(RemoteError) as result:
            self.call(channel, binary=True)
        self.assertEqual(result.exception.code, 'LEASE_EXPIRED')

    def test_short_binary_eof_is_temporary_and_reports_only_lengths(self):
        channel = ScriptedChannel([{'stdout': b'not-a-complete-range'}, {'eof': True}])
        with self.assertRaisesRegex(TemporaryError, 'short range response'):
            self.call(channel, binary=True)

    def test_short_binary_json_nonerror_is_temporary(self):
        channel = ScriptedChannel([{'stdout': json.dumps({'status': 'ok'}).encode()}, {'eof': True}])
        with self.assertRaisesRegex(TemporaryError, 'short range response'):
            self.call(channel, binary=True)

    def test_exit_status_without_eof_times_out_instead_of_parsing_prefix(self):
        channel = ScriptedChannel([{'stdout': b'prefix'}], status_ready=True)
        with self.assertRaisesRegex(TemporaryError, 'stage=receive-body received=6 expected=8388608') as result:
            self.call(channel, binary=True, clock=[0, 181])
        self.assertIn('eof=False closed=False exit_status_ready=True', str(result.exception))
        self.assertNotIn('prefix', str(result.exception))

    def test_complete_range_without_eof_times_out_with_stream_end_diagnostic(self):
        channel = ScriptedChannel([{'stdout': b'x' * 4096}], status_ready=True)
        with self.assertRaisesRegex(TemporaryError, 'stage=await-stream-end received=4096 expected=4096'):
            self.call(channel, binary=True, length=4096, clock=[0, 181])
        self.assertTrue(channel.closed)
        self.assertFalse(channel.exit_read_before_eof)

    def test_timeout_before_first_byte_reports_only_metadata(self):
        channel = ScriptedChannel([{'stderr': b'private diagnostic'}], status_ready=False)
        with self.assertRaisesRegex(TemporaryError, 'stage=await-first-byte received=0 expected=8388608') as result:
            self.call(channel, binary=True, clock=[0, 181])
        self.assertIn('exit_status_ready=False stderr_bytes=18', str(result.exception))
        self.assertNotIn('private diagnostic', str(result.exception))


if __name__ == '__main__':
    unittest.main()
