#!/usr/bin/env bash
set -eu
cd /app
source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON="$(which python)"
unset PYSPARK_PYTHON

if [ ! -f a.parquet ]; then
  echo "Place a.parquet in /app (mounted from ./app on the host)." >&2
  exit 1
fi

hdfs dfs -put -f a.parquet /
spark-submit prepare_data.py

echo "HDFS /data:"
hdfs dfs -ls /data | head -20 || true
echo "HDFS /input/data:"
hdfs dfs -ls /input/data || true
echo "done data preparation!"
