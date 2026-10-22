Load: 100 VUs, 30s, 10 events per request.

| mode | events/s | p50 ms | p95 ms | p99 ms | 2xx % | Postgres commits | rows stored |
|---|---:|---:|---:|---:|---:|---:|---:|
| direct | 18,175 | 54.5 | 63.4 | 73.2 | 100.00 | 528,231 | 539,150 |
| buffered | 50,811 | 13.3 | 45.1 | 66.3 | 100.00 | 1,547 | 1,488,520 |

Throughput: 2.8x. p95 latency: 1.4x lower. Postgres commits per stored event: 0.98 -> 0.0010 (99.9% fewer).
