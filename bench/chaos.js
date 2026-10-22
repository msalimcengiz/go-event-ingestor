// k6 client that behaves like a well-written producer: it generates event ids itself and
// retries a failed request with the same ids, so retries are safe. Every request's 10
// events share the user_id "<vu>-<iteration>"; when the API acknowledges a request the
// client logs "ACK <vu>-<iteration>", and the chaos script checks each acknowledged key
// against Postgres.
import http from 'k6/http';
import { Counter } from 'k6/metrics';
import { sleep } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const TARGET = __ENV.TARGET || 'http://api:8080';
const accepted = new Counter('events_accepted');
const givenUp = new Counter('events_given_up');
const retries = new Counter('request_retries');

export const options = {
  vus: parseInt(__ENV.VUS || '50', 10),
  duration: __ENV.DURATION || '60s',
  // Retries can outlive the load phase; let them finish.
  gracefulStop: '70s',
};

export default function () {
  const key = `${__VU}-${__ITER}`;
  const events = Array.from({ length: 10 }, () => ({ id: uuidv4(), type: 'chaos', user_id: key }));
  const body = JSON.stringify(events);
  const params = { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${__ENV.API_KEY}` }, timeout: '5s' };

  const deadline = Date.now() + 50_000;
  for (let attempt = 0; Date.now() < deadline; attempt++) {
    const res = http.post(`${TARGET}/v1/events`, body, params);
    if (res.status === 202) {
      accepted.add(events.length);
      console.log(`ACK ${key}`);
      return;
    }
    retries.add(1);
    sleep(Math.min(0.1 * 2 ** attempt, 2)); // API down or backpressure: back off and retry
  }
  givenUp.add(events.length);
}

export function handleSummary(data) {
  return { '/results/chaos.json': JSON.stringify(data, null, 2) };
}
