Load: 50 VUs for 60s, 10 events per request, client retries with the same event ids. SIGKILLed 14 containers: both workers every 6 s, the API every 18 s.

| requests acknowledged | events stored | acknowledged but lost | duplicate rows | dead-lettered | client retries | stored after client gave up |
|---:|---:|---:|---:|---:|---:|---:|
| 64,628 | 646,280 | **0** | **0** | 0 | 100 | 0 |

**PASS**: every acknowledged request is fully stored, with no duplicates.
