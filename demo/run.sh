#!/usr/bin/env bash
# Guided demo: starts the stack, walks through the main behaviours and checks each one.
# Needs only Docker. Takes about two minutes on the first run (image builds).
set -euo pipefail
cd "$(dirname "$0")/.."

STATE="$PWD/.demo-state"   # inside the project: Docker Desktop/Colima only share the home directory
rm -rf "$STATE" && mkdir -p "$STATE"
echo 1 > "$STATE/next_step"
py() {
  docker run --rm --network ingestor_default -v "$PWD/demo:/demo:ro" -v "$STATE:/tmp" -w /demo \
    -e DEMO_STEP="$(cat "$STATE/next_step")" python:3.13-alpine python -u demo.py "$1"
}
quiet() { "$@" >/dev/null 2>&1; }
rcli() { docker compose exec -T redis redis-cli "$@"; }

echo "Starting the stack (API, 2 workers, Postgres, Redis)…"
quiet docker compose up -d --build --wait
py basics

n=$(cat "$STATE/next_step")
echo
echo -e "\033[1m$n. A message that cannot be decoded goes to the dead-letter stream instead of blocking the group\033[0m"
echo $((n + 1)) > "$STATE/next_step"
before=$(rcli XLEN events:dead | tr -d '\r')
rcli XADD events '*' d 'this is not json' >/dev/null
for _ in $(seq 1 20); do
  after=$(rcli XLEN events:dead | tr -d '\r'); [ "$after" -gt "$before" ] && break; sleep 0.5
done
[ "$after" -gt "$before" ] && echo -e "   \033[32m✓\033[0m events:dead grew from $before to $after" || { echo "   ✗ not dead-lettered"; exit 1; }
echo -e "   \033[2m$(rcli XREVRANGE events:dead + - COUNT 1 | tr '\r\n' '  ' | cut -c1-160)\033[0m"

quiet docker compose stop worker
MAX_BACKLOG=50 quiet docker compose up -d --wait api
sleep 1
py backpressure
quiet docker compose up -d --wait worker
py drained
py metrics
quiet docker compose up -d --wait api   # back to the default MAX_BACKLOG
rm -rf "$STATE"
echo "Explore: Grafana dashboard → make observability, then http://localhost:3300 · stop everything → make down"
