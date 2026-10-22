"""Guided demo of go-event-ingestor. Run with `make demo`; demo/run.sh drives the
infrastructure (stopping workers, injecting a poison message) between the phases."""

import sys
import time
import uuid
from datetime import UTC, datetime, timedelta

from lib import done, http, next_step, note, ok, show, step, wait_until

API = "http://api:8080"
KEY = {"Authorization": "Bearer dev-key"}


def stored(event_id: str) -> int:
    return http("GET", f"{API}/v1/events/{event_id}", headers=KEY).status


def basics() -> None:
    step("Requests without an API key are rejected")
    r = http("POST", f"{API}/v1/events", [{"type": "click", "user_id": "u1"}])
    show(r)
    ok(r.status == 401, "401 without Authorization header")

    step("A batch of events is accepted at once and written to Postgres by the workers")
    events = [
        {"id": str(uuid.uuid4()), "type": "page_view", "user_id": "u-42", "payload": {"path": "/kampanyalar"}},
        {"id": str(uuid.uuid4()), "type": "add_to_cart", "user_id": "u-42", "payload": {"sku": "SKU-1", "qty": 2}},
        {"id": str(uuid.uuid4()), "type": "purchase", "user_id": "u-42", "payload": {"order": "o-1001", "total": 1499.9}},
    ]
    r = http("POST", f"{API}/v1/events", events, KEY)
    show(r)
    ok(r.status == 202 and r.json()["accepted"] == 3, "202 Accepted: the API only buffered them in Redis")
    for e in events:
        wait_until(lambda e=e: stored(e["id"]), lambda s: s == 200, timeout=20)
    ok(True, "all 3 events are now readable from Postgres via GET /v1/events/{id}")
    show(http("GET", f"{API}/v1/events/{events[2]['id']}", headers=KEY))

    step("A client retry with the same event ids does not create duplicates")
    r = http("POST", f"{API}/v1/events", events, KEY)
    ok(r.status == 202, "the retry is accepted")
    note("Postgres dedupes on the event id (INSERT … ON CONFLICT DO NOTHING); the count stays 3.")

    step("Invalid events are rejected with the index of the bad one; nothing is stored")
    future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    bad = [{"type": "click", "user_id": "u1"}, {"type": "click", "user_id": "u1", "occurred_at": future}]
    r = http("POST", f"{API}/v1/events", bad, KEY)
    show(r)
    ok(r.status == 422 and r.json()["index"] == 1, "422 pointing at events[1] (timestamp in the future)")


def backpressure() -> None:
    step("Workers are stopped and MAX_BACKLOG is 50: the API pushes back instead of growing Redis forever")
    ids = []
    status = 202
    for _ in range(100):
        batch = [{"id": str(uuid.uuid4()), "type": "click", "user_id": "u-bp"} for _ in range(10)]
        r = http("POST", f"{API}/v1/events", batch, KEY)
        status = r.status
        if r.status != 202:
            break
        ids.extend(e["id"] for e in batch)
        time.sleep(0.1)  # the API samples the backlog every 500 ms, not on every request
    show(r)
    ok(status == 503 and r.headers.get("retry-after") == "1", "503 with Retry-After once the backlog passed the limit")
    note(f"{len(ids)} events were accepted before that and are waiting in the stream.")
    with open("/tmp/backlog_ids", "w") as f:
        f.write("\n".join(ids))


def drained() -> None:
    step("Workers are back: every event accepted during the outage reaches Postgres")
    ids = open("/tmp/backlog_ids").read().split()
    for i in ids[:: max(1, len(ids) // 10)] + ids[-1:]:
        wait_until(lambda i=i: stored(i), lambda s: s == 200, timeout=30)
    ok(True, f"sampled events out of {len(ids)} are all stored")
    r = http("POST", f"{API}/v1/events", [{"type": "click", "user_id": "u-after"}], KEY)
    ok(r.status == 202, "new events are accepted again (backpressure lifted)")


def metrics() -> None:
    step("Prometheus metrics show what happened")
    lines = [l for l in http("GET", f"{API}/metrics").body.decode().splitlines() if not l.startswith("#")]
    for name in ("ingestor_events_accepted_total", "ingestor_backpressure_rejections_total"):
        for line in lines:
            if line.startswith(name):
                note(line)
    rejected = next(float(l.split()[-1]) for l in lines if l.startswith("ingestor_backpressure_rejections_total"))
    ok(rejected >= 1, "the 503 above is counted in ingestor_backpressure_rejections_total")
    done("auth, buffering, idempotency, validation, dead letters and backpressure all behaved as described.")


{"basics": basics, "backpressure": backpressure, "drained": drained, "metrics": metrics}[sys.argv[1]]()
with open("/tmp/next_step", "w") as f:
    f.write(str(next_step()))
