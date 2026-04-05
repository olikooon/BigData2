import os
import subprocess
import sys

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

N_DOCS = 1000


def run_cmd(args: list[str]) -> None:
    print("RUN:", " ".join(args), flush=True)
    subprocess.check_call(args)


def main():
    spark = (
        SparkSession.builder.appName("data preparation")
        .master("local[*]")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate()
    )
    sc = spark.sparkContext

    df = spark.read.parquet("/a.parquet")
    df = df.select("id", "title", "text").filter(F.col("text").isNotNull())
    cnt = df.count()
    if cnt == 0:
        print("Parquet has no rows with text", file=sys.stderr)
        sys.exit(1)
    frac = min(1.0, (100.0 * N_DOCS) / float(cnt))
    df = df.sample(withReplacement=False, fraction=frac, seed=0).limit(N_DOCS)

    os.makedirs("data", exist_ok=True)
    for name in os.listdir("data"):
        os.remove(os.path.join("data", name))

    written = 0
    for row in df.collect():
        tid = str(row.id)
        title = row.title or ""
        text = (row.text or "").strip()
        if not text:
            continue
        base = sanitize_filename(tid + "_" + title).replace(" ", "_")
        path = os.path.join("data", base + ".txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write(row.text or "")
        written += 1

    if written == 0:
        print("No non-empty documents written", file=sys.stderr)
        sys.exit(1)

    spark.stop()

    run_cmd(["hdfs", "dfs", "-rm", "-r", "-f", "/data"])
    run_cmd(["hdfs", "dfs", "-mkdir", "-p", "/data"])
    run_cmd(["hdfs", "dfs", "-put", "-f", "data", "/"])

    run_cmd(["hdfs", "dfs", "-rm", "-r", "-f", "/input/data"])

    spark = (
        SparkSession.builder.appName("hdfs input bundle")
        .master("local[*]")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate()
    )
    sc = spark.sparkContext

    def parse_whole(kv):
        path, content = kv
        base = path.rsplit("/", 1)[-1]
        if not base.endswith(".txt"):
            return None
        base = base[:-4]
        i = base.find("_")
        if i <= 0:
            return None
        doc_id = base[:i]
        title = base[i + 1 :]
        body = (content or "").strip()
        if not body:
            return None
        title = title.replace("\t", " ")
        body = body.replace("\t", " ").replace("\n", " ")
        return f"{doc_id}\t{title}\t{body}"

    pairs = sc.wholeTextFiles("/data/*.txt")
    lines = pairs.map(parse_whole).filter(lambda x: x is not None)
    lines.coalesce(1).saveAsTextFile("/input/data")
    spark.stop()

    print(f"prepare_data.py finished: {written} local docs, HDFS /data and /input/data updated", flush=True)


if __name__ == "__main__":
    main()
