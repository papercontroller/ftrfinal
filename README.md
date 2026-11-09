# Resilient University Information System

Final project for Fault Tolerance and Dependable Computing (AITU), Option A: it continues
the midterm [midterm-ftr](https://github.com/papercontroller/midterm-ftr) and the endterm.
Unmodified copies of the midterm (`baseline-midterm/`) and the endterm (`baseline-endterm/`) are the two baselines;
every comparison runs all versions with the same scripts.

New since the endterm: a paid registration workflow with crash recovery (saga), a PostgreSQL
hot standby with automatic failover, scheduled backups, resource limits, two more alerts,
and experiments E8 to E13.

## Structure

| Path | Purpose |
|---|---|
| `cmd/gateway` | API gateway: JWT auth, RBAC, rate limit, health probes, failover |
| `cmd/student-service` | Student profiles, course registration, saga in `registration.go` |
| `cmd/payment-service` | Tuition payments with idempotency keys, void for compensation |
| `cmd/academic-service` | Transcripts, circuit breaker, last-known-good cache |
| `pkg/` | Shared logger, metrics, middleware, breaker and database connection |
| `config/` | Database schema, replication and watchdog scripts, Prometheus, alert rules, Grafana dashboard |
| `k8s/` | Kubernetes manifests (not tested on a cluster) |
| `tests/`, `cmd/*/main_test.go` | Automated tests |
| `scripts/run_experiments.py` | All fault-injection and security experiments |
| `results/` | Measured results, raw request data and the run log |
| `baseline-midterm/` | Midterm at commit 871967a |
| `baseline-endterm/` | Endterm submission, unchanged (one database node, no saga, no backups) |

## Run

Needs Docker Desktop. Python 3 and Go are only needed for experiments and tests.

```bash
cp .env.example .env      # then replace the three values
docker compose up -d --build
```

| URL | What |
|---|---|
| http://127.0.0.1:8080 | API gateway |
| http://127.0.0.1:3000/d/uis-resilience | Grafana dashboard (anonymous view) |
| http://127.0.0.1:9090/alerts | Prometheus alerts |

Synthetic accounts are `student1`, `student2`, `registrar1` and `admin1`. All use `DEMO_PASSWORD` from `.env`.

```bash
TOKEN=$(curl -s 127.0.0.1:8080/auth/login \
  -d '{"username":"student1","password":"YOUR_DEMO_PASSWORD"}' | python -c "import sys,json;print(json.load(sys.stdin)['token'])")
curl -s 127.0.0.1:8080/api/transcripts/1 -H "Authorization: Bearer $TOKEN"
```

## Tests

```bash
go test ./...
```

## Experiments

```bash
python scripts/run_experiments.py                  # everything, about 3 hours
python scripts/run_experiments.py --only E1 E2     # a subset
python scripts/run_experiments.py --target endterm --reps 1
python scripts/summarize_results.py                # print the means used in the report
python scripts/capture_evidence.py                 # demo incident, Grafana screenshot, sample logs
```

All three versions use port 8080, so the script runs one at a time and switches automatically. It resets containers and tables before each repetition. E8 builds a
small local image with `tc`, because the service containers have no NET_ADMIN.

Baseline switches of the final system, used by E9 and E10 and available by hand:

| Setting | Effect |
|---|---|
| `SAGA_RECOVERY=off` | No recovery loop: an interrupted registration stays where it stopped |
| `FAULT_INJECTION=on` | Keys named `fault-<point>-...` crash the student service at that step |
| `touch /tmp/no-failover` in `uis-postgres-standby` | Watchdog stops promoting |

## Demo script

Open Grafana and a terminal side by side. Log in first, as shown above.

**1. Service failure.** Crash a replica while reading in a loop. All requests stay 200 and Docker restarts the replica.

```bash
while true; do curl -s -o /dev/null -w "%{http_code}\n" 127.0.0.1:8080/api/transcripts/1 -H "Authorization: Bearer $TOKEN"; sleep 0.2; done
docker exec academic-service-1 kill -TERM 1      # in a second terminal
docker logs api-gateway 2>&1 | grep "health changed" | tail -2
```

**2. Database freeze and graceful degradation.** Read the transcript twice or more first, so that both replicas cache it.

```bash
docker pause uis-postgres-db
curl -s 127.0.0.1:8080/api/transcripts/1 -H "Authorization: Bearer $TOKEN"   # status DEGRADED_CACHED_MODE
docker unpause uis-postgres-db
```

**3. Security.** Each request is denied and logged.

```bash
curl -i 127.0.0.1:8080/api/transcripts/1                                         # 401
curl -i 127.0.0.1:8080/api/transcripts/2 -H "Authorization: Bearer $TOKEN"       # 403, another student
docker logs api-gateway 2>&1 | grep AUDIT | tail -3
```

**4. Request tracing.** One ID appears in the logs of the gateway and of the replica that served it.

```bash
curl -s 127.0.0.1:8080/api/transcripts/1 -H "Authorization: Bearer $TOKEN" -H "X-Correlation-ID: demo-1"
docker logs api-gateway 2>&1 | grep demo-1
docker logs academic-service-1 2>&1 | grep demo-1; docker logs academic-service-2 2>&1 | grep demo-1
```

**5. Interrupted registration.** Needs `FAULT_INJECTION=on docker compose up -d`. The request
dies with 502 and the recovery loop finishes it about 6 seconds later. Log in as `admin1`.

```bash
curl -s 127.0.0.1:8080/api/registrations -H "Authorization: Bearer $TOKEN" \
  -d '{"idempotency_key":"fault-after_charge-demo","student_id":1,"course_id":"CSE2501","amount":1000}'
sleep 8
curl -s 127.0.0.1:8080/api/registrations/fault-after_charge-demo -H "Authorization: Bearer $TOKEN"   # CONFIRMED
```

**6. Database failover.** Writes return 503 until the standby promotes itself.

```bash
docker kill uis-postgres-db
docker logs -f uis-postgres-standby 2>&1 | grep watchdog
```

Afterwards rebuild both nodes, because joining the old primary back is manual:
`docker compose down -v && docker compose up -d`.

## Known limitations

- The gateway is one container, so it is a single point of failure.
- Failover is one-way and has no quorum. After it, the system has no standby and runs without
  synchronous replication until an operator rebuilds the old primary.
- With synchronous replication, writes stop if only the standby is lost.
- There is no TLS between client and gateway or between services.
- Login uses fixed synthetic accounts with one shared password.
- The transcript cache is per replica and is lost on restart.
- `/metrics` on the gateway is public.
- Backend calls are trusted by network position: the `Service` role is not cryptographically proven.
- The Kubernetes manifests were not run on a cluster and have no database tier.
- Docker does not restart a container that is only unhealthy. Kubernetes liveness probes would.
