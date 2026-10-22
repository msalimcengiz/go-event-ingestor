#!/usr/bin/env bash
# Chaos test: steady load while worker and API containers are killed with SIGKILL.
# Passes only if every event the API acknowledged is in Postgres exactly once, nothing
# is left pending, and nothing was dead-lettered.
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION=${DURATION:-60}
RESULTS="$PWD/bench/results"
mkdir -p "$RESULTS"
export API_KEYS=chaos-key CLAIM_MIN_IDLE=5s

psql() { docker compose exec -T postgres psql -U ingestor -d ingestor -tAc "$1"; }
rcli() { docker compose exec -T redis redis-cli "$@"; }

docker compose up -d --build --wait api worker >/dev/null
psql "TRUNCATE events" >/dev/null
rcli DEL events events:dead >/dev/null
docker compose restart api worker >/dev/null
sleep 3

echo "load for ${DURATION}s, killing containers meanwhile"
docker run --rm --network ingestor_default -v "$PWD/bench:/scripts:ro" -v "$RESULTS:/results" \
  -e API_KEY=chaos-key -e DURATION="${DURATION}s" grafana/k6:1.2.3 run --quiet /scripts/chaos.js \
  2> >(grep -o 'ACK [0-9-]*' | cut -d' ' -f2 > "$RESULTS/acked.txt") &
K6=$!

kills=0
end=$((SECONDS + DURATION - 5))
round=0
while [ $SECONDS -lt $end ]; do
  sleep 6
  round=$((round + 1))
  victims=$(docker compose ps -q worker)
  if [ $((round % 3)) -eq 0 ]; then victims="$victims $(docker compose ps -q api)"; fi
  for c in $victims; do
    docker kill -s KILL "$c" >/dev/null && kills=$((kills + 1))
  done
  sleep 1
  # Bring every victim back; Docker's own restart policy backs off after repeated kills.
  for c in $victims; do
    for _ in $(seq 1 20); do
      [ "$(docker inspect -f '{{.State.Running}}' "$c")" = "true" ] && break
      docker start "$c" >/dev/null 2>&1 || true
      sleep 0.5
    done
  done
done
wait $K6
echo "containers killed: $kills"

# Wait for the workers to reclaim and finish everything.
for _ in $(seq 1 120); do
  pending=$(rcli --raw XINFO GROUPS events | awk 'p{print; exit} /^pending$/{p=1}')
  lag=$(rcli --raw XINFO GROUPS events | awk 'p{print; exit} /^lag$/{p=1}')
  [ "${pending:-1}" = "0" ] && [ "${lag:-1}" = "0" ] && break
  sleep 1
done

sleep 1  # let k6's stderr pipe finish writing acked.txt
docker compose exec -T postgres psql -U ingestor -d ingestor -qc "DROP TABLE IF EXISTS acked; CREATE TABLE acked (k text PRIMARY KEY)"
docker compose exec -T postgres psql -U ingestor -d ingestor -qc "\\copy acked FROM STDIN" < "$RESULTS/acked.txt"

acked=$(psql "SELECT count(*) FROM acked")
rows=$(psql "SELECT count(*) FROM events")
# An acknowledged request is lost if any of its 10 events is missing.
lost=$(psql "SELECT count(*) FROM acked a WHERE (SELECT count(*) FROM events e WHERE e.user_id = a.k) <> 10")
dupes=$(psql "SELECT count(*) - count(DISTINCT id) FROM events")
extra=$(psql "SELECT count(*) FROM events WHERE user_id NOT IN (SELECT k FROM acked)")
dead=$(rcli XLEN events:dead | tr -d '\r')
python3 - "$RESULTS/chaos.json" "$acked" "$rows" "$lost" "$dupes" "$extra" "$dead" "$kills" "$DURATION" <<'PY' | tee "$RESULTS/chaos.md"
import json, sys
path = sys.argv[1]
acked, rows, lost, dupes, extra, dead, kills = map(int, sys.argv[2:9])
duration = sys.argv[9]
m = json.load(open(path))["metrics"]
val = lambda k: int(m.get(k, {}).get("values", {}).get("count", 0))
retries, given_up = val("request_retries"), val("events_given_up")
ok = lost == 0 and dupes == 0 and dead == 0
print(f"Load: 50 VUs for {duration}s, 10 events per request, client retries with the same event ids. "
      f"SIGKILLed {kills} containers: both workers every 6 s, the API every 18 s.\n")
print("| requests acknowledged | events stored | acknowledged but lost | duplicate rows | dead-lettered | client retries | stored after client gave up |")
print("|---:|---:|---:|---:|---:|---:|---:|")
print(f"| {acked:,} | {rows:,} | **{lost}** | **{dupes}** | {dead} | {retries:,} | {extra} |")
print(f"\n**{'PASS' if ok else 'FAIL'}**: every acknowledged request is fully stored, with no duplicates.")
sys.exit(0 if ok else 1)
PY
