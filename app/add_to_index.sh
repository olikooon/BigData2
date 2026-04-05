#!/usr/bin/env bash
# Optional: add one local UTF-8 document (<doc_id>_<doc_title>.txt) to HDFS /data,
# rebuild /input/data from all of /data, then re-run full index + Cassandra load.
set -eu
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${APP_DIR}"

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <path-to-local-doc.txt>" >&2
  echo "  Filename must be <doc_id>_<doc_title>.txt (spaces as underscores in title)." >&2
  exit 1
fi

FILE="$1"
if [ ! -f "${FILE}" ]; then
  echo "Not a file: ${FILE}" >&2
  exit 1
fi

BASENAME="$(basename "${FILE}")"
case "${BASENAME}" in
  *.txt) ;;
  *) echo "Expected .txt file" >&2; exit 1 ;;
esac

source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON="$(which python)"
unset PYSPARK_PYTHON

echo "Uploading ${FILE} -> hdfs:///data/${BASENAME}"
hdfs dfs -put -f "${FILE}" "/data/${BASENAME}"

spark-submit "${APP_DIR}/rebuild_input_from_hdfs.py"

bash "${APP_DIR}/index.sh"
echo "add_to_index.sh finished."
