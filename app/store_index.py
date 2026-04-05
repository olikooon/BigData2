import os
import subprocess
import sys
import time

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

KEYSPACE = "search_index"
CASSANDRA_HOSTS = [h.strip() for h in os.environ.get("CASSANDRA_HOSTS", "cassandra-server").split(",") if h.strip()]
CONCURRENCY = int(os.environ.get("CASSANDRA_LOAD_CONCURRENCY", "48"))
BATCH = int(os.environ.get("CASSANDRA_LOAD_BATCH", "400"))


def hdfs_ls_parts(directory):
    try:
        out = subprocess.check_output(
            ["hdfs", "dfs", "-ls", directory],
            stderr=subprocess.STDOUT,
            text=True,
        )
    except subprocess.CalledProcessError:
        return []
    paths = []
    for line in out.splitlines():
        line = line.strip()
        if not line or line.startswith("Found "):
            continue
        parts = line.split()
        if len(parts) < 8:
            continue
        p = parts[-1]
        if "/part-" in p:
            paths.append(p)
    paths.sort()
    return paths


def hdfs_first_part(directory):
    ps = hdfs_ls_parts(directory)
    return ps[0] if ps else None


def iter_hdfs_file(hdfs_path):
    proc = subprocess.Popen(
        ["hdfs", "dfs", "-text", hdfs_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        yield line.rstrip("\n\r")
    proc.wait()
    err = proc.stderr.read() if proc.stderr else ""
    if proc.returncode != 0:
        raise RuntimeError(f"hdfs dfs -text {hdfs_path} failed (code {proc.returncode}): {err}")


def connect_cluster():
    last = None
    for attempt in range(72):
        try:
            cluster = Cluster(CASSANDRA_HOSTS, connect_timeout=15)
            session = cluster.connect()
            return cluster, session
        except Exception as e:
            last = e
            print(f"Cassandra not ready ({attempt + 1}/72): {e}", file=sys.stderr, flush=True)
            time.sleep(5)
    raise SystemExit(f"Could not connect to Cassandra at {CASSANDRA_HOSTS}: {last}")


def ensure_schema(session):
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
    )
    session.set_keyspace(KEYSPACE)
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            df int
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS postings (
            term text,
            doc_id text,
            tf int,
            PRIMARY KEY (term, doc_id)
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS doc_stats (
            doc_id text PRIMARY KEY,
            dl int
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS corpus_stats (
            id text PRIMARY KEY,
            n_docs bigint,
            dl_avg double,
            sum_dl bigint
        )
        """
    )
    for tbl in ("vocabulary", "postings", "doc_stats", "corpus_stats"):
        session.execute(f"TRUNCATE {tbl}")


def load_vocabulary(session, hdfs_path):
    prep = session.prepare(f"INSERT INTO {KEYSPACE}.vocabulary (term, df) VALUES (?, ?)")
    batch = []
    n = 0
    for line in iter_hdfs_file(hdfs_path):
        if not line.startswith("VOCAB\t"):
            continue
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        term, df_s = parts[1], parts[2]
        batch.append((term, int(df_s)))
        n += 1
        if len(batch) >= BATCH:
            execute_concurrent_with_args(session, prep, batch, concurrency=CONCURRENCY)
            batch.clear()
    if batch:
        execute_concurrent_with_args(session, prep, batch, concurrency=CONCURRENCY)
    return n


def load_postings(session, hdfs_path):
    prep = session.prepare(
        f"INSERT INTO {KEYSPACE}.postings (term, doc_id, tf) VALUES (?, ?, ?)"
    )
    batch = []
    n = 0
    for line in iter_hdfs_file(hdfs_path):
        if not line.startswith("POST\t"):
            continue
        parts = line.split("\t", 3)
        if len(parts) < 4:
            continue
        term, doc_id, tf_s = parts[1], parts[2], parts[3]
        batch.append((term, doc_id, int(tf_s)))
        n += 1
        if len(batch) >= BATCH:
            execute_concurrent_with_args(session, prep, batch, concurrency=CONCURRENCY)
            batch.clear()
    if batch:
        execute_concurrent_with_args(session, prep, batch, concurrency=CONCURRENCY)
    return n


def load_doc_stats(session, hdfs_path):
    prep = session.prepare(f"INSERT INTO {KEYSPACE}.doc_stats (doc_id, dl) VALUES (?, ?)")
    batch = []
    n = 0
    for line in iter_hdfs_file(hdfs_path):
        if not line.startswith("DOC_LEN\t"):
            continue
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        doc_id, dl_s = parts[1], parts[2]
        batch.append((doc_id, int(dl_s)))
        n += 1
        if len(batch) >= BATCH:
            execute_concurrent_with_args(session, prep, batch, concurrency=CONCURRENCY)
            batch.clear()
    if batch:
        execute_concurrent_with_args(session, prep, batch, concurrency=CONCURRENCY)
    return n


def load_corpus_stats(session, hdfs_path):
    prep = session.prepare(
        "INSERT INTO corpus_stats (id, n_docs, dl_avg, sum_dl) VALUES (?, ?, ?, ?)"
    )
    for line in iter_hdfs_file(hdfs_path):
        if not line.startswith("GLOBAL\t"):
            continue
        parts = line.split("\t", 3)
        if len(parts) < 4:
            continue
        n_docs = int(parts[1])
        dl_avg = float(parts[2])
        sum_dl = int(parts[3])
        session.execute(prep, ("global", n_docs, dl_avg, sum_dl))
        return
    raise RuntimeError("No GLOBAL line found in corpus stats HDFS file")


def main():
    vocab_dir = os.environ.get("HDFS_VOCAB", "/indexer/vocabulary")
    index_dir = os.environ.get("HDFS_INDEX", "/indexer/index")
    doclen_dir = os.environ.get("HDFS_DOC_LEN", "/indexer/stats/doc_lengths")
    corpus_dir = os.environ.get("HDFS_CORPUS_STATS", "/indexer/stats/corpus")

    v_part = hdfs_first_part(vocab_dir)
    p_part = hdfs_first_part(index_dir)
    d_part = hdfs_first_part(doclen_dir)
    c_part = hdfs_first_part(corpus_dir)
    missing = [
        (name, d)
        for name, d, part in (
            ("vocabulary", vocab_dir, v_part),
            ("index", index_dir, p_part),
            ("doc_lengths", doclen_dir, d_part),
            ("corpus", corpus_dir, c_part),
        )
        if part is None
    ]
    if missing:
        for name, d in missing:
            print(f"Missing HDFS part under {d} ({name})", file=sys.stderr)
        raise SystemExit(1)

    print(f"Loading from HDFS:\n  {v_part}\n  {p_part}\n  {d_part}\n  {c_part}", flush=True)

    cluster, session = connect_cluster()
    try:
        ensure_schema(session)
        nv = load_vocabulary(session, v_part)
        np = load_postings(session, p_part)
        nd = load_doc_stats(session, d_part)
        load_corpus_stats(session, c_part)
        print(f"Loaded vocabulary={nv}, postings={np}, doc_stats={nd}, corpus_stats=1", flush=True)
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
