#!/usr/bin/env bash

set -eu

INPUT_PATH="${1:-/input/data}"

# First Hadoop output part file
hdfs_first_part() {
  local dir="$1"
  hdfs dfs -ls "${dir}" 2>/dev/null | awk '{print $8}' | grep -E '/part-(r-)?[0-9]{5}$' | sort | head -n 1
}

if [ -z "${HADOOP_HOME:-}" ]; then
  echo "HADOOP_HOME is not set" >&2
  exit 1
fi

STREAM_JAR=$(ls "${HADOOP_HOME}/share/hadoop/tools/lib"/hadoop-streaming-*.jar 2>/dev/null | head -n 1)
if [ -z "${STREAM_JAR}" ] || [ ! -f "${STREAM_JAR}" ]; then
  echo "hadoop-streaming jar not found under ${HADOOP_HOME}/share/hadoop/tools/lib" >&2
  exit 1
fi

MR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/mapreduce"

echo "Indexing input: ${INPUT_PATH}"
hdfs dfs -test -d "${INPUT_PATH}" || {
  echo "Input path is not a directory in HDFS: ${INPUT_PATH}" >&2
  exit 1
}

# Clean previous runs
hdfs dfs -rm -r -f /tmp/indexer/job1 /tmp/indexer/job2 >/dev/null 2>&1 || true
hdfs dfs -rm -r -f /indexer/pipeline1 /indexer/pipeline2 /indexer/vocabulary /indexer/index /indexer/stats >/dev/null 2>&1 || true

hdfs dfs -mkdir -p /tmp/indexer /indexer/vocabulary /indexer/index /indexer/stats/doc_lengths /indexer/stats/corpus

echo "=== Pipeline 1: postings + per-doc lengths + vocabulary (df) ==="
hadoop jar "${STREAM_JAR}" \
  -D mapreduce.job.reduces=1 \
  -D mapred.reduce.tasks=1 \
  -input "${INPUT_PATH}" \
  -output /tmp/indexer/job1 \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -file "${MR_DIR}/mapper1.py" \
  -file "${MR_DIR}/reducer1.py"

echo "=== Pipeline 2: corpus N and average dl ==="
hadoop jar "${STREAM_JAR}" \
  -D mapreduce.job.reduces=1 \
  -D mapred.reduce.tasks=1 \
  -input /tmp/indexer/job1 \
  -output /tmp/indexer/job2 \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py" \
  -file "${MR_DIR}/mapper2.py" \
  -file "${MR_DIR}/reducer2.py"

# One partition final layout under /indexer
JOB1_PART="$(hdfs_first_part /tmp/indexer/job1)"
JOB2_PART="$(hdfs_first_part /tmp/indexer/job2)"
if [ -z "${JOB1_PART}" ] || [ -z "${JOB2_PART}" ]; then
  echo "Could not locate MapReduce output part files under /tmp/indexer/job1 or job2" >&2
  exit 1
fi

hdfs dfs -mkdir -p /indexer/pipeline1 /indexer/pipeline2
hdfs dfs -cp -f "${JOB1_PART}" /indexer/pipeline1/part-00000
hdfs dfs -cp -f "${JOB2_PART}" /indexer/pipeline2/part-00000

# Typed views for loaders / BM25
set +e
hdfs dfs -text /indexer/pipeline1/part-00000 | { grep '^VOCAB' || true; } | hdfs dfs -put -f - /indexer/vocabulary/part-00000
hdfs dfs -text /indexer/pipeline1/part-00000 | { grep '^POST' || true; } | hdfs dfs -put -f - /indexer/index/part-00000
hdfs dfs -text /indexer/pipeline1/part-00000 | { grep '^DOC_LEN' || true; } | hdfs dfs -put -f - /indexer/stats/doc_lengths/part-00000
set -e

hdfs dfs -cp -f /indexer/pipeline2/part-00000 /indexer/stats/corpus/part-00000

echo "=== /indexer layout ==="
hdfs dfs -ls -R /indexer

echo "create_index.sh finished."
