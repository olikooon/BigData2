import math
import os
import re
import subprocess
import sys
from collections import Counter

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

KEYSPACE = "search_index"
CASSANDRA_HOSTS = [
    h.strip()
    for h in os.environ.get("CASSANDRA_HOSTS", "cassandra-server").split(",")
    if h.strip()
]

K1 = float(os.environ.get("BM25_K1", "1.0"))
B = float(os.environ.get("BM25_B", "0.75"))
HDFS_INPUT = os.environ.get("HDFS_INPUT_DOCS", "/input/data")
TOP_K = 10


def tokenize(text):
    return re.findall(r"[a-z0-9]+", (text or "").lower())


def bm25_term_score(n_docs, df, tf, dl, dl_avg, k1, b):
    if df <= 0 or n_docs <= 0 or dl_avg <= 0:
        return 0.0
    idf = math.log(n_docs / float(df))
    denom = k1 * ((1.0 - b) + b * (dl / dl_avg)) + float(tf)
    if denom <= 0.0:
        return 0.0
    return idf * (k1 + 1.0) * float(tf) / denom


def read_query():
    q = os.environ.get("SEARCH_QUERY", "").strip()
    if q:
        return q
    if len(sys.argv) > 1:
        return " ".join(sys.argv[1:]).strip()
    if not sys.stdin.isatty():
        import select

        r, _, _ = select.select([sys.stdin], [], [], 0.5)
        if r:
            data = sys.stdin.read()
            if data.strip():
                return data.strip()
    return ""


def cassandra_session():
    cluster = Cluster(CASSANDRA_HOSTS, connect_timeout=15)
    return cluster, cluster.connect(KEYSPACE)


def load_corpus_stats(session):
    row = session.execute(
        "SELECT n_docs, dl_avg FROM corpus_stats WHERE id = %s", ("global",)
    ).one()
    if row is None:
        raise RuntimeError("corpus_stats missing 'global' row — run index + store_index first")
    return int(row.n_docs), float(row.dl_avg)


def load_df_for_terms(session, terms):
    out = {}
    for t in terms:
        row = session.execute("SELECT df FROM vocabulary WHERE term = %s", (t,)).one()
        if row is not None:
            out[t] = int(row.df)
    return out


def load_postings_for_terms(session, terms):
    rows = []
    for t in terms:
        for r in session.execute(
                "SELECT term, doc_id, tf FROM postings WHERE term = %s", (t,)
        ):
            rows.append((r.term, r.doc_id, int(r.tf)))
    return rows


def load_dl_map(session, doc_ids):
    out = {}
    for did in doc_ids:
        row = session.execute("SELECT dl FROM doc_stats WHERE doc_id = %s", (did,)).one()
        if row is not None:
            out[did] = int(row.dl)
    return out


def _hdfs_list_part_files(directory):
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
        if "/part-" in p and not p.endswith("_SUCCESS"):
            paths.append(p)
    paths.sort()
    return paths


def load_title_map_from_hdfs(base_dir):
    print("Loading titles from HDFS %s (driver)..." % base_dir, file=sys.stderr, flush=True)
    title_map = {}
    for hdfs_path in _hdfs_list_part_files(base_dir):
        proc = subprocess.Popen(
            ["hdfs", "dfs", "-text", hdfs_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.rstrip("\n\r")
            if not line:
                continue
            parts = line.split("\t", 2)
            if len(parts) >= 2:
                title_map[parts[0]] = parts[1]
        proc.wait()
    print("Titles loaded: %d" % len(title_map), file=sys.stderr, flush=True)
    return title_map


def main():
    query = read_query()
    if not query:
        print("Empty query (stdin and argv).", file=sys.stderr)
        sys.exit(1)

    terms = tokenize(query)
    if not terms:
        print("No tokens in query after normalization.", file=sys.stderr)
        sys.exit(1)

    q_weights = Counter(terms)
    unique_terms = list(q_weights.keys())

    print("Cassandra: loading corpus, vocabulary, postings...", file=sys.stderr, flush=True)
    cluster = None
    try:
        cluster, session = cassandra_session()
        n_docs, dl_avg = load_corpus_stats(session)
        df_map = load_df_for_terms(session, unique_terms)
        if not df_map:
            print("No query terms appear in the vocabulary.", file=sys.stderr)
            sys.exit(0)

        posting_rows = load_postings_for_terms(session, list(df_map.keys()))
        doc_ids = {p[1] for p in posting_rows}
        dl_map = load_dl_map(session, doc_ids)
    finally:
        if cluster is not None:
            cluster.shutdown()

    title_map = load_title_map_from_hdfs(HDFS_INPUT)

    print("Starting Spark for BM25 RDD...", file=sys.stderr, flush=True)
    spark = (
        SparkSession.builder.appName("bm25-query")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )
    sc = spark.sparkContext

    idf_inputs = {t: df_map[t] for t in df_map}

    bc_n = sc.broadcast(float(n_docs))
    bc_dl_avg = sc.broadcast(float(dl_avg))
    bc_k1 = sc.broadcast(K1)
    bc_b = sc.broadcast(B)
    bc_df = sc.broadcast(idf_inputs)
    bc_dl = sc.broadcast(dl_map)
    bc_qw = sc.broadcast(dict(q_weights))

    def score_partition(row):
        term, doc_id, tf = row
        n = bc_n.value
        dl_avg = bc_dl_avg.value
        k1 = bc_k1.value
        b = bc_b.value
        df_map_l = bc_df.value
        dl_map_l = bc_dl.value
        qw = bc_qw.value

        df_t = df_map_l.get(term, 0)
        if df_t <= 0:
            return None
        dl = float(dl_map_l.get(doc_id, 0))
        if dl <= 0.0:
            return None
        w = int(qw.get(term, 0))
        if w <= 0:
            return None
        s = bm25_term_score(n, df_t, tf, dl, dl_avg, k1, b) * w
        return doc_id, s

    n_parts = max(1, min(4, len(posting_rows) // 5000 + 1))
    base_rdd = sc.parallelize(posting_rows, n_parts)
    scored = base_rdd.map(score_partition).filter(lambda x: x is not None)
    by_doc = scored.reduceByKey(lambda a, b: a + b)

    top = by_doc.takeOrdered(TOP_K, key=lambda kv: -kv[1])

    for doc_id, _score in top:
        title = title_map.get(doc_id, "")
        print("%s\t%s" % (doc_id, title))

    spark.stop()


if __name__ == "__main__":
    main()
