## Prerequisites

- Docker and Docker Compose
- Place **`a.parquet`** (Wikipedia sample from the course link) in **`app/a.parquet`** before first run

## Run

```bash
docker compose up
```

The master container runs **`app/app.sh`**: starts Hadoop, waits for Cassandra, creates venv, **`prepare_data.sh`** → **`index.sh`** → **`search.sh`**.

By default **`COMPOSE_SPARK_MASTER=local[*]`** so the stack **finishes reliably** (same BM25 RDD code, no YARN executor dependency). For a **YARN** run:

- **CMD:** `set COMPOSE_SPARK_MASTER=yarn&& docker compose up`
- **bash:** `COMPOSE_SPARK_MASTER=yarn docker compose up`

Skip the automatic search step:

```bash
# In app.sh temporarily set SKIP_SEARCH=1, or comment the search line — or use:
docker exec -it cluster-master bash -lc 'export SKIP_SEARCH=1; exec bash /app/app.sh'
```

## Manual search (from host)

```bash
docker exec -it cluster-master bash -lc "cd /app && source .venv/bin/activate && bash search.sh film music wikipedia"
```

Stdin:

```bash
docker exec -it cluster-master bash -lc "cd /app && source .venv/bin/activate && echo 'film music' | bash search.sh"
```

## Optional: add one document

After the cluster has run once (venv exists):

```bash
docker exec -it cluster-master bash -lc "cd /app && bash add_to_index.sh /app/file.txt"
```