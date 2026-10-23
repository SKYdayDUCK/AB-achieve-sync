#!/usr/bin/env bash
set -euo pipefail
python3 -m pip install .
printf 'Installed: aas-a, aasctl-shell\nEdit config/a.example.yaml and follow docs/OPERATIONS.md for SSH forced-command setup.\n'
