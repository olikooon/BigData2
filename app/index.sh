#!/usr/bin/env bash
set -eu
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "=== index.sh: MapReduce (HDFS) ==="
bash "${DIR}/create_index.sh" "$@"
echo "=== index.sh: Cassandra load ==="
bash "${DIR}/store_index.sh"
echo "=== index.sh finished ==="
