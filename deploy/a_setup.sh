#!/usr/bin/env bash
# One-shot A-side setup (idempotent). Run ON the GPU server after copying the project dir there.
#   bash deploy/a_setup.sh <BASE_DIR> <input_dir> <normal_pubkey_file> <emergency_pubkey_file> <b_primary.pub> <b_backup.pub>
# BASE_DIR must be on a big data disk (e.g. /root/autodl-tmp). input_dir MUST be on the same filesystem.
set -euo pipefail
BASE=${1:?base dir}; INPUT=${2:?input dir}; NKEY=${3:?normal ssh pubkey}; EKEY=${4:?emergency ssh pubkey}; PUB1=${5:?age pub}; PUB2=${6:?age backup pub}
SRC=$(cd "$(dirname "$0")/.." && pwd)
PY=${PYTHON:-python3}
"$PY" - <<'PYEOF' || { echo "need Python >= 3.11 (set PYTHON=/path/to/python3.11, e.g. conda create -n aas python=3.11)"; exit 1; }
import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)
PYEOF
mkdir -p "$BASE/aas" "$BASE/aas-keys" "$INPUT"
[ "$(stat -c %d "$BASE")" = "$(stat -c %d "$INPUT")" ] || { echo "input_dir and BASE must be on the same filesystem"; exit 1; }
[ -x "$BASE/aas-venv/bin/python" ] || "$PY" -m venv "$BASE/aas-venv"
"$BASE/aas-venv/bin/python" -m pip install -q --upgrade pip
"$BASE/aas-venv/bin/python" -m pip install -q "$SRC"
command -v age >/dev/null && command -v age-keygen >/dev/null || { apt-get update -qq && apt-get install -y -qq age; }
age --version
cp "$PUB1" "$BASE/aas-keys/b_primary.pub"; cp "$PUB2" "$BASE/aas-keys/b_backup.pub"
CONF="$BASE/aas-keys/a.yaml"
cat > "$CONF" <<YAML
node: {producer_id: gpu-a-01, consumer_id: desktop-b-01}
paths: {root_dir: $BASE/aas, input_dir: $INPUT}
batch: {max_files: 100, max_bytes: 1073741824, max_batch_bytes: 4294967296, max_wait_seconds: ${AAS_MAX_WAIT:-600}, min_age_seconds: 3}
archive: {strategy: tar, verify_final_archive: true, delete_source_after_verified_publish: true}
crypto: {strategy: age_x25519, recipients: [$BASE/aas-keys/b_primary.pub, $BASE/aas-keys/b_backup.pub]}
ack: {delete_bundle_only_after_ack: true, ack_grace_seconds: 0}
watch: {hold_back_newest: 1, idle_flush_factor: 4, idle_flush_min_seconds: 10, idle_flush_max_seconds: ${AAS_IDLE_MAX:-300}}
emergency: {enabled: true, idle_seconds: 60, max_session_seconds: 1800}
capacity: {warn_free_bytes: 5368709120, critical_free_bytes: 2147483648, auto_pause_generator: false, auto_delete_unacked: false}
YAML
# restricted keys: marker lines are replaced, other keys of the account stay untouched
mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys
sed -i '/# aas-managed$/d' ~/.ssh/authorized_keys
SHELLBIN="$BASE/aas-venv/bin/aasctl-shell"
echo "restrict,command=\"$SHELLBIN --config $CONF --role consumer\" $(cut -d' ' -f1,2 "$NKEY") # aas-managed" >> ~/.ssh/authorized_keys
echo "restrict,command=\"$SHELLBIN --config $CONF --role emergency\" $(cut -d' ' -f1,2 "$EKEY") # aas-managed" >> ~/.ssh/authorized_keys
"$BASE/aas-venv/bin/aas-a" --config "$CONF" init
echo "A setup done. Start with: bash $SRC/deploy/aas-a-daemon.sh $BASE start"
