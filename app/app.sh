#!/usr/bin/env bash
set -eu
cd /app

service ssh restart

bash start-services.sh

echo "Waiting for Cassandra (9042)..."
python3 -c "
import socket, time
host = 'cassandra-server'
for i in range(90):
    try:
        s = socket.create_connection((host, 9042), 3)
        s.close()
        print('Cassandra port open')
        break
    except OSError:
        time.sleep(2)
else:
    raise SystemExit('Cassandra not reachable on 9042')
"

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt

venv-pack -o .venv.tar.gz

bash prepare_data.sh

bash index.sh

if [ "${SKIP_SEARCH:-0}" = "1" ]; then
  echo "SKIP_SEARCH=1 — skipping search.sh (run manually: docker exec -it cluster-master bash -lc 'cd /app && bash search.sh your words')"
else
  # Default local[*] so docker compose up always completes. Override for YARN demo:
  #   COMPOSE_SPARK_MASTER=yarn docker compose up
  _SM="${COMPOSE_SPARK_MASTER:-local[*]}"
  echo "Running search (SPARK_MASTER=${_SM})..."
  sleep 2
  SPARK_MASTER="${_SM}" bash search.sh "this is a query!"
fi

echo "=== app.sh pipeline finished successfully ==="
