#!/usr/bin/env bash
set -eu
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${APP_DIR}"
source .venv/bin/activate
export CASSANDRA_HOSTS="${CASSANDRA_HOSTS:-cassandra-server}"
echo "=== store_index: Cassandra hosts=${CASSANDRA_HOSTS} ==="
exec python3 store_index.py
