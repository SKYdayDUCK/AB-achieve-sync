"""Read-only config/host diagnostics; never repairs paths or removes data."""
from __future__ import annotations
import argparse
from pathlib import Path
import shutil
from aas.common.config import AConfig,BConfig


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--side',choices=('a','b'),required=True)
    ap.add_argument('--config',required=True)
    args=ap.parse_args()
    config=(AConfig if args.side=='a' else BConfig).load(args.config)
    print(f'Config accepted: {args.side.upper()}')
    print(f'Root: {config.root}')
    print(f'Free bytes: {shutil.disk_usage(config.root if config.root.exists() else config.root.parent).free}')
    if args.side=='a':
        print(f'Input: {config.input_dir}; exists={config.input_dir.exists()}')
        print(f'Archive crypto: {config.crypto_scheme}; age={shutil.which("age")}; age-keygen={shutil.which("age-keygen")}')
    else:
        print(f'SSH binary: {shutil.which("ssh")}, host: {config.remote_host}, port: {config.remote_port}')
        print(f'Crypto: {config.crypto_scheme}; age={shutil.which("age")}')
        print(f'Emergency SSH key separately configured: {bool(config.emergency_identity_file)}')
    print('Doctor is read-only; real SSH/age/Windows/72h tests still required.')

if __name__=='__main__': main()
