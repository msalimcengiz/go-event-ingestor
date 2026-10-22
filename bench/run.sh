#!/usr/bin/env bash
# Compares WRITE_MODE=direct (one INSERT per event inside the request) with
# WRITE_MODE=buffered (Redis Stream + batched COPY) under the same k6 load.
# Results go to bench/results/. Usage: ./bench/run.sh [duration] [vus]
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION=${1:-30s}
VUS=${2:-100}
RESULTS="$PWD/bench/results"
mkdir -p "$RESULTS"

psql() { docker compose exec -T postgres psql -U ingestor -d ingestor -tAc "$1"; }

for MODE in direct buffered; do
  echo "==> $MODE"
  WRITE_MODE=$MODE docker compose up -d --build --wait api worker >/dev/null
  psql "TRUNCATE events" >/dev/null
  docker compose exec -T redis redis-cli DEL events >/dev/null
  WRITE_MODE=$MODE docker compose restart api worker >/dev/null
  sleep 3
  psql "SELECT pg_stat_reset()" >/dev/null
  xact_before=$(psql "SELECT xact_commit FROM pg_stat_database WHERE datname='ingestor'")

  docker run --rm --network ingestor_default \
    -v "$PWD/bench:/scripts:ro" -v "$RESULTS:/results" \
    -e MODE="$MODE" -e VUS="$VUS" -e DURATION="$DURATION" \
    grafana/k6:1.2.3 run --quiet /scripts/load.js

  # Wait for the workers to drain the stream so we count every row.
  accepted=$(python3 -c "import json;d=json.load(open('$RESULTS/$MODE.json'));print(int(d['metrics']['http_reqs']['values']['count']))")
  expected=$((accepted * 10))
  for _ in $(seq 1 120); do
    rows=$(psql "SELECT count(*) FROM events")
    [ "$rows" -ge "$expected" ] && break
    sleep 1
  done
  xact_after=$(psql "SELECT xact_commit FROM pg_stat_database WHERE datname='ingestor'")

  python3 - "$RESULTS/$MODE.json" "$rows" "$((xact_after - xact_before))" <<'PY'
import json, sys
path, rows, xacts = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
d = json.load(open(path))
d["ingestor"] = {"rows_in_postgres": rows, "postgres_commits": xacts}
json.dump(d, open(path, "w"), indent=2)
PY
done

python3 - "$RESULTS" "$DURATION" "$VUS" <<'PY' | tee "$RESULTS/summary.md"
import json, sys, platform, os
res, duration, vus = sys.argv[1], sys.argv[2], sys.argv[3]
rows = []
for mode in ("direct", "buffered"):
    d = json.load(open(f"{res}/{mode}.json"))
    m = d["metrics"]
    dur = m["http_req_duration"]["values"]
    secs = d["state"]["testRunDurationMs"] / 1000
    events = m["http_reqs"]["values"]["count"] * 10
    rows.append((mode, events / secs, dur["p(50)"], dur["p(95)"], dur["p(99)"],
                 m["checks"]["values"]["rate"] * 100,
                 d["ingestor"]["postgres_commits"], d["ingestor"]["rows_in_postgres"]))
print(f"Load: {vus} VUs, {duration}, 10 events per request.\n")
print("| mode | events/s | p50 ms | p95 ms | p99 ms | 2xx % | Postgres commits | rows stored |")
print("|---|---:|---:|---:|---:|---:|---:|---:|")
for r in rows:
    print(f"| {r[0]} | {r[1]:,.0f} | {r[2]:.1f} | {r[3]:.1f} | {r[4]:.1f} | {r[5]:.2f} | {r[6]:,} | {r[7]:,} |")
d, b = rows
print(f"\nThroughput: {b[1]/d[1]:.1f}x. p95 latency: {d[3]/b[3]:.1f}x lower. "
      f"Postgres commits per stored event: {d[6]/max(d[7],1):.2f} -> {b[6]/max(b[7],1):.4f} "
      f"({(1 - (b[6]/max(b[7],1)) / (d[6]/max(d[7],1))) * 100:.1f}% fewer).")
PY
