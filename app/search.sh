#!/usr/bin/env bash

set -eu
cd /app
source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON="$(which python)"
export CASSANDRA_HOSTS="${CASSANDRA_HOSTS:-cassandra-server}"

if [ "$#" -gt 0 ]; then
  export SEARCH_QUERY="$*"
elif ! [ -t 0 ]; then
  export SEARCH_QUERY="$(cat | tr -d '\r')"
else
  echo "Usage: $0 <query words...>" >&2
  echo "   or: echo 'your query' | $0" >&2
  exit 1
fi

if [ -z "${SEARCH_QUERY// }" ]; then
  echo "Empty query." >&2
  exit 1
fi

MASTER="${SPARK_MASTER:-yarn}"

echo "search.sh: master=${MASTER} query=${SEARCH_QUERY}" >&2
date >&2

SUBMIT=(
  spark-submit
  --master "${MASTER}"
  --name bm25-search
  --conf spark.sql.shuffle.partitions=4
  --conf spark.ui.showConsoleProgress=true
  --conf spark.network.timeout=180s
  --conf spark.executor.heartbeatInterval=60s
  --conf spark.rpc.askTimeout=180s
  --conf spark.hadoop.fs.defaultFS=hdfs://cluster-master:9000
)

if [ "${MASTER}" = "yarn" ]; then
  export PYSPARK_PYTHON=./.venv/bin/python
  SUBMIT+=(
    --deploy-mode client
    --archives /app/.venv.tar.gz#.venv
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python
    --conf spark.executorEnv.PYSPARK_PYTHON=./.venv/bin/python
    --conf spark.executor.instances=1
    --num-executors 1
    --executor-memory 768m
    --executor-cores 1
    --conf spark.executor.memoryOverhead=384
    --driver-memory 768m
  )
else
  export PYSPARK_PYTHON="${PYSPARK_DRIVER_PYTHON}"
  SUBMIT+=(
    --driver-memory 1024m
  )
fi

# SEARCH_QUERY is read by query.py (stdin is unreliable through spark-submit on YARN).
"${SUBMIT[@]}" /app/query.py
date >&2
echo "search.sh: finished" >&2
