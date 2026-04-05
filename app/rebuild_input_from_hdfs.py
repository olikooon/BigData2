import subprocess
import sys

from pyspark.sql import SparkSession


def main():
    subprocess.run(
        ["hdfs", "dfs", "-rm", "-r", "-f", "/input/data"],
        check=False,
    )
    spark = SparkSession.builder.appName("rebuild-input-from-hdfs").master("local[*]").getOrCreate()
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
    print("rebuild_input_from_hdfs.py: /input/data updated from /data", flush=True)


if __name__ == "__main__":
    main()
