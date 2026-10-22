// k6 load test: every virtual user posts small batches of events as fast as it can.
import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const TARGET = __ENV.TARGET || 'http://api:8080';
const EVENTS_PER_REQUEST = parseInt(__ENV.EVENTS_PER_REQUEST || '10', 10);

export const options = {
  vus: parseInt(__ENV.VUS || '100', 10),
  duration: __ENV.DURATION || '30s',
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

const types = ['page_view', 'click', 'add_to_cart', 'purchase', 'ad_impression'];

export default function () {
  const events = [];
  for (let i = 0; i < EVENTS_PER_REQUEST; i++) {
    events.push({
      id: uuidv4(),
      type: types[Math.floor(Math.random() * types.length)],
      user_id: `u${Math.floor(Math.random() * 50000)}`,
      payload: { campaign_id: Math.floor(Math.random() * 500), cost: Math.random() },
    });
  }
  const res = http.post(`${TARGET}/v1/events`, JSON.stringify(events), {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${__ENV.API_KEY || 'dev-key'}` },
  });
  check(res, { 'status is 202': (r) => r.status === 202 });
}

export function handleSummary(data) {
  return { [`/results/${__ENV.MODE || 'run'}.json`]: JSON.stringify(data, null, 2) };
}
