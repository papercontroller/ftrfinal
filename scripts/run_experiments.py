"""Repeatable fault-injection and security experiments.

Every number in results/ comes from real requests against real containers. This
script only injects the fault, sends traffic and records what happened.

    python scripts/run_experiments.py                 all experiments, both systems
    python scripts/run_experiments.py --only E1 E2    a subset
    python scripts/run_experiments.py --target final --reps 1

The midterm baseline in baseline-midterm/ and the final system both use port
8080, so only one runs at a time. The script switches between them. It needs only
the Python standard library and Docker.

E1 to E7 are the endterm experiments, repeated on the final system. E8 to E13 are new:
network delay, interrupted workflow, database failover, backup restore, long-running
reliability and gateway crash. Report section 9 maps them to the assignment matrix.
"""
import argparse
import csv
import datetime
import http.client
import json
import os
import platform
import random
import re
import statistics
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(ROOT, "results")
RAW = os.path.join(RESULTS, "raw")
GATEWAY_HOST, GATEWAY_PORT = "127.0.0.1", 8080  # localhost can hit the WSL relay on ::1
PROM = "http://127.0.0.1:9090"
STANDBY = "uis-postgres-standby"
CLIENT_TIMEOUT = 5.0


def sh(cmd, check=False, cwd=ROOT, env=None):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding="utf-8",
                       errors="replace", cwd=cwd, env={**os.environ, **(env or {})})
    if check and r.returncode != 0:
        raise RuntimeError(f"{cmd}\n{r.stdout}\n{r.stderr}")
    return (r.stdout + r.stderr).strip()


def read_env():
    env = {}
    with open(os.path.join(ROOT, ".env")) as f:
        for line in f:
            if "=" in line and not line.startswith("#"):
                k, v = line.strip().split("=", 1)
                env[k] = v
    return env


class Target:
    """What differs between the two systems: URLs, container names, auth."""

    def __init__(self, name):
        self.name = name
        self.env = read_env()
        if name in ("final", "endterm"):
            self.transcript = "/api/transcripts/1"
            self.pay = "/api/payments"
            self.victim = "academic-service-1"
            self.pay_victim = "payment-service-1"
            self.db = "uis-postgres-db"
            self.psql = "psql -U uis_user -d uis_db"
            self.cwd, self.up = ROOT, "docker compose up -d"
            self.down = "docker compose down"
            if name == "endterm":  # same container names and schema, no standby and no saga
                self.cwd = os.path.join(ROOT, "baseline-endterm")
                self.up, self.down = "docker compose -p endterm up -d", "docker compose -p endterm down -v"
        else:
            self.transcript = "/transcript"
            self.pay = "/pay"
            self.victim = "midterm-app-replica-1-1"
            self.pay_victim = "midterm-app-replica-1-1"
            self.db = "midterm-db-1"
            self.psql = "psql -U user -d uis"
            self.cwd = os.path.join(ROOT, "baseline-midterm")
            self.up, self.down = "docker compose -p midterm up -d", "docker compose -p midterm down -v"
        self.token = None
        self.replicas = (["academic-service-1", "academic-service-2", "payment-service-1", "payment-service-2",
                          "student-service-1", "student-service-2"] if name != "midterm"
                         else ["midterm-app-replica-1-1", "midterm-app-replica-2-1"])

    def start(self, env=None, build=False):
        sh(self.up + (" --build" if build else ""), check=True, cwd=self.cwd, env=env)

    def stop(self):
        sh(self.down, cwd=self.cwd)

    def sql(self, q, on=None):
        return sh(f'docker exec {on or self.db} {self.psql} -Atc "{q}"')

    def rebuild(self, env=None):
        """Fresh primary and standby. A failover leaves them swapped, and joining the old
        primary back is a manual step, so experiments that fail over start from scratch."""
        sh("docker compose down -v", cwd=self.cwd)
        self.reset(env)

    def set_failover(self, on):
        sh(f"docker exec {STANDBY} {'rm -f' if on else 'touch'} /tmp/no-failover")

    def seed_students(self):
        self.sql("INSERT INTO students (id, name, email) SELECT g, 'Synthetic ' || g, 'synthetic' || g || '@example.test' "
                 "FROM generate_series(1000, 1999) g ON CONFLICT DO NOTHING; UPDATE courses SET max_capacity = 100000")

    def login(self, user="student1"):
        if self.name == "midterm":
            return None
        status, body, _ = request("POST", "/auth/login", {"username": user, "password": self.env["DEMO_PASSWORD"]})
        return json.loads(body)["token"] if status == 200 else None

    def reset(self, env=None):
        """Known state before every repetition: all containers up, tables empty."""
        sh(f"docker unpause {self.db}")
        # After a pause the health status stays "unhealthy" for a few seconds and compose refuses to start.
        deadline = time.time() + 30
        while time.time() < deadline and sh(f'docker inspect -f "{{{{.State.Health.Status}}}}" {self.db}') == "unhealthy":
            time.sleep(0.5)
        self.start(env)
        self.sql("TRUNCATE payments, idempotency_keys RESTART IDENTITY")
        if self.name == "final":
            # E2, E4 and the rest test a frozen database. Only E10 lets the standby take over.
            self.set_failover(False)
            self.sql("TRUNCATE enrollments, registrations RESTART IDENTITY; UPDATE courses SET enrolled = 0")
        elif self.name == "endterm":
            self.sql("TRUNCATE enrollments RESTART IDENTITY; UPDATE courses SET enrolled = 0")
        deadline = time.time() + 90
        path = "/readyz" if self.name != "midterm" else "/healthz"
        while time.time() < deadline:
            if request("GET", path)[0] == 200 and self.db_ok():
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("system did not become ready")
        time.sleep(3 if self.name == "midterm" else 4)  # token bucket refill, replicas back in rotation
        self.token = self.login()

    def db_ok(self):
        return self.sql("SELECT 1") == "1"

    def fingerprint(self):
        """Hash of the records an attack must not change."""
        tables = "payments" if self.name == "midterm" else "payments, enrollments, transcripts, students, courses"
        out = []
        for t in tables.split(", "):
            out.append(t + "=" + self.sql(f"SELECT count(*), md5(coalesce(string_agg(x::text, ',' ORDER BY x::text), '')) FROM {t} x"))
        return out

    def crash(self, container):
        """Process exit from inside the container. A real crash, so the restart
        policy applies; `docker kill` counts as a manual stop and would not restart."""
        return sh(f"docker exec {container} kill -TERM 1")


def request(method, path, body=None, token=None, headers=None, raw=None, timeout=CLIENT_TIMEOUT, conn=None):
    """Returns (status, text, seconds). status 0 is a transport error or timeout."""
    h = dict(headers or {})
    data = raw
    if body is not None:
        data = json.dumps(body).encode()
    if data is not None:
        h.setdefault("Content-Type", "application/json")
    if token:
        h["Authorization"] = "Bearer " + token
    own = conn is None
    c = conn or http.client.HTTPConnection(GATEWAY_HOST, GATEWAY_PORT, timeout=timeout)
    start = time.perf_counter()
    try:
        c.request(method, path, data, h)
        r = c.getresponse()
        text = r.read().decode("utf-8", "replace")
        return r.status, text, time.perf_counter() - start
    except Exception as e:
        c.close()
        return 0, type(e).__name__, time.perf_counter() - start
    finally:
        if own:
            c.close()


class Load:
    """Open-loop generator: sends at the planned rate, even if the system is slow."""

    def __init__(self, steps, make_request, workers=128, timeout=CLIENT_TIMEOUT):
        self.timeout = timeout
        self.schedule = []
        t = 0.0
        for rate, seconds in steps:
            self.schedule += [t + i / rate for i in range(int(rate * seconds))]
            t += seconds
        self.make_request = make_request
        self.workers = workers
        self.samples = []
        self.lock = threading.Lock()
        self.next = 0
        self.t0 = None
        self.halted = threading.Event()

    def _worker(self):
        conn = http.client.HTTPConnection(GATEWAY_HOST, GATEWAY_PORT, timeout=self.timeout)
        while True:
            with self.lock:
                i = self.next
                self.next += 1
            if i >= len(self.schedule):
                conn.close()
                return
            planned = self.t0 + self.schedule[i]
            delay = planned - time.perf_counter()
            if delay > 0 and self.halted.wait(delay):
                conn.close()
                return
            method, path, body, token = self.make_request(i)
            sent = time.perf_counter()
            status, text, seconds = request(method, path, body, token, conn=conn)
            if status == 0:
                conn = http.client.HTTPConnection(GATEWAY_HOST, GATEWAY_PORT, timeout=self.timeout)
            with self.lock:
                self.samples.append((sent - self.t0, status, seconds, text))

    def start(self):
        self.t0 = time.perf_counter()
        self.threads = [threading.Thread(target=self._worker, daemon=True) for _ in range(self.workers)]
        for t in self.threads:
            t.start()
        return self

    def now(self):
        return time.perf_counter() - self.t0

    def wait(self):
        for t in self.threads:
            t.join()
        self.samples.sort()
        return self.samples


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p / 100 * (len(values) - 1))))]


def summarize(samples, ok=lambda s: s[1] == 200):
    latencies = [s[2] * 1000 for s in samples]
    good = sum(1 for s in samples if ok(s))
    codes = {}
    for s in samples:
        codes[str(s[1])] = codes.get(str(s[1]), 0) + 1
    return {
        "total": len(samples), "success": good, "failed": len(samples) - good,
        "error_rate_pct": round(100 * (len(samples) - good) / len(samples), 2) if samples else None,
        "availability_pct": round(100 * good / len(samples), 2) if samples else None,
        "latency_avg_ms": round(statistics.mean(latencies), 1) if latencies else None,
        "latency_p95_ms": round(percentile(latencies, 95), 1) if latencies else None,
        "latency_max_ms": round(max(latencies), 1) if latencies else None,
        "status_codes": codes,
    }


def save_samples(exp, target, rep, samples):
    os.makedirs(RAW, exist_ok=True)
    path = os.path.join(RAW, f"{exp}_{target}_rep{rep}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["t_sec", "status", "latency_ms", "body_prefix"])
        for t, status, seconds, text in samples:
            w.writerow([round(t, 3), status, round(seconds * 1000, 1), text[:60]])


def firing_alerts():
    try:
        url = PROM + "/api/v1/query?query=" + urllib.parse.quote('ALERTS{alertstate="firing"}')
        data = json.load(urllib.request.urlopen(url, timeout=2))
        return {m["metric"]["alertname"] for m in data["data"]["result"]}
    except Exception:
        return set()


def prom_value(query):
    try:
        data = json.load(urllib.request.urlopen(PROM + "/api/v1/query?query=" + urllib.parse.quote(query), timeout=2))
        result = data["data"]["result"]
        return float(result[0]["value"][1]) if result else None
    except Exception:
        return None


def wait_alerts_clear(limit=90):
    end = time.time() + limit
    while time.time() < end:
        if not firing_alerts():
            return True
        time.sleep(1)
    return False


class Watcher:
    """Polls a condition and records when it first turns true, on the runner's clock."""

    def __init__(self, conditions, interval=0.1):
        self.conditions = conditions
        self.first = {}
        self.interval = interval
        self.stop_flag = False
        self.start_at = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self.stop_flag:
            for name, check in self.conditions.items():
                if name not in self.first and check():
                    self.first[name] = time.perf_counter()
            time.sleep(self.interval)

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.stop_flag = True
        self.thread.join()
        return self.first


def replica_health(replica):
    status, text, _ = request("GET", "/metrics", timeout=1)
    if status != 200:
        return None
    m = re.search(r'upstream_replica_healthy\{replica="%s",service="[^"]+"\} (\d)' % re.escape(replica), text)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------- experiments

def e1_instance_failure(tg, rep):
    """Crash one replica under steady read traffic."""
    tg.reset()
    tok = tg.token
    load = Load([(15, 30)], lambda i: ("GET", tg.transcript, None, tok)).start()
    time.sleep(5)
    conditions = {}
    if tg.name != "midterm":
        conditions = {"removed": lambda: replica_health("academic-service-1:8083") == 0}
    w = Watcher(conditions).start()
    injected = time.perf_counter()
    crash_output = tg.crash(tg.victim)
    if tg.name != "midterm":
        while "removed" not in w.first and time.perf_counter() - injected < 10:
            time.sleep(0.05)
        w.conditions["rejoined"] = lambda: "removed" in w.first and replica_health("academic-service-1:8083") == 1
    samples = load.wait()
    first = w.stop()
    save_samples("E1", tg.name, rep, samples)
    state = sh(f'docker inspect -f "{{{{.State.Status}}}} restarts={{{{.RestartCount}}}}" {tg.victim}')
    return {
        **summarize(samples),
        "crash_output": crash_output,
        "failure_window": summarize([s for s in samples if 5 <= s[0] <= 8]),
        "detection_time_sec": round(first["removed"] - injected, 2) if "removed" in first else None,
        "recovery_time_sec": round(first["rejoined"] - injected, 2) if "rejoined" in first else None,
        "victim_state_after": state,
        "alerts": sorted(firing_alerts()) if tg.name != "midterm" else None,
    }


def e2_dependency_block(tg, rep):
    """Freeze the database for 15 s with docker pause; TCP stays up, nothing answers."""
    tg.reset()
    tok = tg.token
    for _ in range(12):  # both replicas must have the record cached
        request("GET", tg.transcript, token=tok)
    load = Load([(15, 35)], lambda i: ("GET", tg.transcript, None, tok)).start()
    time.sleep(5)
    injected = load.now()
    sh(f"docker pause {tg.db}")
    time.sleep(7)
    breaker_mid = prom_value("max(circuit_breaker_state)") if tg.name != "midterm" else None
    pay_status, pay_text, pay_seconds = request(
        "POST", tg.pay, {"idempotency_key": f"e2-{rep}", "student_id": 1, "amount": 5.0}, tok)
    time.sleep(max(0, 15 - (load.now() - injected)))
    resumed = load.now()
    sh(f"docker unpause {tg.db}")
    samples = load.wait()
    save_samples("E2", tg.name, rep, samples)

    def is_degraded(s):
        return "DEGRADED" in s[3] or "degraded" in s[3]

    during = [s for s in samples if injected <= s[0] <= resumed]
    degraded = [s for s in during if is_degraded(s)]
    fresh_after = [s for s in samples if s[0] > resumed and s[1] == 200 and not is_degraded(s)]
    # recovery = the moment after which every response is fresh again
    last_bad = max([s[0] + s[2] for s in samples if s[0] > injected and (s[1] != 200 or is_degraded(s))], default=None)
    return {
        **summarize(samples),
        "during_fault": summarize(during),
        "degraded_responses": len(degraded),
        "real_data_during_fault_pct": round(100 * sum(1 for s in during if '"gpa"' in s[3]) / len(during), 2) if during else None,
        "first_degraded_after_sec": round(degraded[0][0] - injected, 2) if degraded else None,
        "first_fresh_after_resume_sec": round(fresh_after[0][0] - resumed, 2) if fresh_after else None,
        "recovery_after_resume_sec": round(last_bad - resumed, 2) if last_bad else None,
        "breaker_state_during_fault": breaker_mid,
        "payment_during_fault": {"status": pay_status, "seconds": round(pay_seconds, 2)},
        "payments_in_db_after": tg.sql("SELECT count(*) FROM payments"),
        "degraded_sample": degraded[0][3][:200] if degraded else None,
    }


def e3_load_spike(tg, rep):
    """Stepped spike. The rate limiter is raised because all synthetic clients share
    one IP; the aim here is to measure capacity, not the limiter (see E6)."""
    env = {"RATE_LIMIT_RPS": "100000", "RATE_LIMIT_BURST": "100000"}
    tg.reset(env)
    tok = tg.token

    def make(i):
        if i % 10 == 0:
            return "POST", tg.pay, {"idempotency_key": f"e3-{rep}-{i}", "student_id": 1, "amount": 1.0}, tok
        return "GET", tg.transcript, None, tok

    load = Load([(20, 10), (300, 20), (20, 10)], make, workers=200).start()
    time.sleep(20)
    stats = sh('docker stats --no-stream --format "{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}"')
    samples = load.wait()
    save_samples("E3", tg.name, rep, samples)
    ok = lambda s: s[1] in (200, 201)
    spike = [s for s in samples if 10 <= s[0] < 30]
    posts_ok = sum(1 for i, s in enumerate(samples) if s[1] == 201 or '"SUCCESS"' in s[3])
    prefix = "uis|academic|payment|student|api-gateway" if tg.name != "midterm" else "midterm"
    return {
        **summarize(samples, ok),
        "spike": {**summarize(spike, ok), "achieved_rps": round(len(spike) / 20, 1)},
        "payments_acknowledged": posts_ok,
        "payments_in_db": tg.sql("SELECT count(*) FROM payments"),
        "duplicate_payments": tg.sql("SELECT count(*) - count(DISTINCT id) FROM payments"),
        "resources_during_spike": [line for line in stats.splitlines() if re.match(prefix, line)],
    }


def e4_alerts(tg, rep):
    """One deliberate incident per alert rule, plus a quiet period as negative control."""
    tok_holder = {}

    def scenario(name, expected, inject, restore, make_request, rate, limit=50):
        tg.reset()
        wait_alerts_clear()
        tok_holder["t"] = tg.token
        load = Load([(rate, limit)], make_request).start()
        time.sleep(3)
        injected = time.perf_counter()
        inject()
        fired, seen = None, set()
        while time.perf_counter() - injected < limit - 5:
            now = firing_alerts()
            seen |= now
            if expected in now:
                fired = time.perf_counter() - injected
                break
            time.sleep(0.25)
        restore()
        load.halted.set()
        samples = load.wait()
        save_samples(f"E4-{name}", tg.name, rep, samples)
        time.sleep(1)
        seen |= firing_alerts()
        return {"scenario": name, "expected_alert": expected, "fired": fired is not None,
                "detection_delay_sec": round(fired, 2) if fired else None,
                "all_alerts_seen": sorted(seen), "extra_alerts": sorted(seen - {expected})}

    tg.reset()
    wait_alerts_clear()
    tok = tg.token
    quiet = Load([(5, 60)], lambda i: ("GET", tg.transcript, None, tok)).start()
    seen = set()
    while any(t.is_alive() for t in quiet.threads):
        seen |= firing_alerts()
        time.sleep(1)
    quiet.wait()

    results = [{"scenario": "quiet_period", "expected_alert": None, "fired": bool(seen),
                "detection_delay_sec": None, "all_alerts_seen": sorted(seen), "extra_alerts": sorted(seen)}]
    results.append(scenario(
        "instance_down", "ServiceInstanceDown",
        lambda: sh("docker stop student-service-2"), lambda: sh("docker start student-service-2"),
        lambda i: ("GET", "/api/courses", None, tok_holder["t"]), 5))
    results.append(scenario(
        "breaker_open", "CircuitBreakerOpen",
        lambda: sh(f"docker pause {tg.db}"), lambda: sh(f"docker unpause {tg.db}"),
        lambda i: ("GET", tg.transcript, None, tok_holder["t"]), 10))
    results.append(scenario(
        "error_rate", "HighErrorRate",
        lambda: sh("docker stop payment-service-1 payment-service-2"),
        lambda: sh("docker start payment-service-1 payment-service-2"),
        lambda i: ("POST", tg.pay, {"idempotency_key": f"e4-{rep}-{i}", "student_id": 1, "amount": 1.0}, tok_holder["t"]), 5))
    results.append(scenario(
        "latency", "HighRequestLatencyP95",
        lambda: sh(f"docker pause {tg.db}"), lambda: sh(f"docker unpause {tg.db}"),
        lambda i: ("GET", "/api/courses", None, tok_holder["t"]), 5))
    results.append(scenario(
        "db_unreachable", "DatabaseUnreachable",
        lambda: sh(f"docker pause {tg.db}"), lambda: sh(f"docker unpause {tg.db}"),
        lambda i: ("GET", "/api/courses", None, tok_holder["t"]), 5))

    def stop_payments_and_register():
        sh("docker stop payment-service-1 payment-service-2")
        request("POST", "/api/registrations", {"idempotency_key": f"e4-{rep}", "student_id": 1, "course_id": "CSE2501", "amount": 10.0}, tok_holder["t"])

    results.append(scenario(
        "registration_stuck", "RegistrationStuck",
        stop_payments_and_register, lambda: sh("docker start payment-service-1 payment-service-2"),
        lambda i: ("GET", "/api/courses", None, tok_holder["t"]), 5))
    results.append(scenario(
        "abuse", "AbuseRateLimitTriggered",
        lambda: None, lambda: None,
        lambda i: ("GET", "/api/courses", None, tok_holder["t"]), 45))
    sh(f"docker unpause {tg.db}")
    return {"scenarios": results}


def forged_tokens(tok):
    import base64
    enc = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=").decode()
    none_token = enc(b'{"alg":"none","typ":"JWT"}') + "." + enc(b'{"user_id":"1","role":"Admin","exp":4102444800}') + "."
    head, payload, sig = tok.split(".")
    padded = payload + "=" * (-len(payload) % 4)
    claims = json.loads(base64.urlsafe_b64decode(padded))
    claims["role"] = "Admin"
    role_swapped = f"{head}.{enc(json.dumps(claims).encode())}.{sig}"
    return none_token, role_swapped


def e5_unauthorized(tg, rep):
    """Protected operations without valid authorization."""
    tg.reset()
    before = tg.fingerprint()
    cases = []

    def case(name, method, path, expected, body=None, token=None, headers=None, secret=None):
        status, text, _ = request(method, path, body, token, headers)
        leaked = bool(secret and secret in text)
        cases.append({"case": name, "status": status, "expected": expected, "denied": status in (401, 403),
                      "passed": status == expected and not leaked, "data_exposed": leaked})

    if tg.name == "final":
        tok = tg.token
        none_token, role_swapped = forged_tokens(tok)
        pay = {"idempotency_key": f"e5-{rep}", "student_id": 1, "amount": 99.0}
        audit_before = prom_counter_sum("audit_security_events_total", gateway=True)
        case("transcript without token", "GET", "/api/transcripts/1", 401, secret="gpa")
        case("payment without token", "POST", "/api/payments", 401, body=pay)
        case("tampered signature", "GET", "/api/transcripts/1", 401, token=tok[:-3] + "abc", secret="gpa")
        case("alg=none forged Admin token", "GET", "/api/students", 401, token=none_token, secret="email")
        case("role changed inside token", "GET", "/api/students", 401, token=role_swapped, secret="email")
        case("student lists all students", "GET", "/api/students", 403, token=tok, secret="email")
        case("student reads other transcript", "GET", "/api/transcripts/2", 403, token=tok, secret="gpa")
        case("student reads other profile", "GET", "/api/students/2", 403, token=tok, secret="email")
        case("student pays for other student", "POST", "/api/payments", 403, token=tok,
             body={"idempotency_key": f"e5b-{rep}", "student_id": 2, "amount": 99.0})
        case("student voids a payment", "POST", "/api/payments/void", 403, token=tok, body={"idempotency_key": "reg-x"})
        case("Admin token voids a payment", "POST", "/api/payments/void", 403, token=tg.login("admin1"), body={"idempotency_key": "reg-x"})
        case("forged service role voids a payment", "POST", "/api/payments/void", 403, token=tok,
             headers={"X-User-Role": "Service"}, body={"idempotency_key": "reg-x"})
        case("student registers another student", "POST", "/api/registrations", 403, token=tok,
             body={"idempotency_key": f"e5c-{rep}", "student_id": 2, "course_id": "CSE2501", "amount": 10.0})
        case("forged identity headers", "GET", "/api/transcripts/2", 403, token=tok,
             headers={"X-User-ID": "2", "X-User-Role": "Admin"}, secret="gpa")
        status, body, _ = request("POST", "/auth/login", {"username": "nobody", "password": "wrong"})
        cases.append({"case": "login with bad credentials", "status": status, "expected": 401, "denied": True,
                      "passed": status == 401, "data_exposed": False})
        status, body, _ = request("POST", "/auth/login", {"username": "student1", "password": tg.env["DEMO_PASSWORD"], "role": "Admin"})
        escalated = '"role":"Admin"' in body
        cases.append({"case": "client requests Admin role at login", "status": status, "expected": 200, "denied": not escalated,
                      "passed": not escalated, "data_exposed": False})
        time.sleep(0.2)
        audit_events = prom_counter_sum("audit_security_events_total", gateway=True) - audit_before
        logs = sh("docker logs --since 30s api-gateway")
        with open(os.path.join(RAW, f"E5_audit_rep{rep}.log"), "w", encoding="utf-8") as f:
            f.writelines(line + "\n" for line in logs.splitlines() if '"AUDIT"' in line)
    else:
        case("transcript without token", "GET", tg.transcript, 401, secret="gpa")
        case("payment without token", "POST", tg.pay, 401,
             body={"idempotency_key": f"e5-{rep}", "student_id": 1, "amount": 99.0})
        case("server-side request forgery via /external", "GET",
             "/external?url=http://app-replica-2:8080/healthz", 403, secret="server_id")
        audit_events = None

    after = tg.fingerprint()
    return {"cases": cases, "denied": sum(c["passed"] for c in cases), "total": len(cases),
            "data_unchanged": before == after, "audit_events_gateway": audit_events}


def prom_counter_sum(name, gateway=False):
    status, text, _ = request("GET", "/metrics", timeout=2)
    return sum(float(m) for m in re.findall(r"^%s\{[^}]*\} ([0-9.e+]+)$" % name, text, re.M)) if status == 200 else 0


def e6_abuse(tg, rep):
    """Malformed input, then a burst, then proof that normal users still get through."""
    tg.reset()
    tok = tg.token
    before = tg.fingerprint()
    pay = tg.pay
    cases = []

    def bad(name, method, path, raw=None, body=None, headers=None):
        status, text, _ = request(method, path, body, tok, headers, raw=raw)
        cases.append({"case": name, "status": status, "rejected": 400 <= status < 500, "server_error": status >= 500 or status == 0})

    base = {"idempotency_key": f"e6-{rep}", "student_id": 1, "amount": 10.0}
    bad("broken JSON", "POST", pay, raw=b'{"amount": ')
    bad("negative amount", "POST", pay, body={**base, "amount": -100})
    bad("zero amount", "POST", pay, body={**base, "amount": 0})
    bad("amount too large", "POST", pay, body={**base, "amount": 1e12})
    bad("missing idempotency key", "POST", pay, body={"student_id": 1, "amount": 10})
    bad("idempotency key 300 chars", "POST", pay, body={**base, "idempotency_key": "k" * 300})
    bad("SQL injection in key field", "POST", pay, body={**base, "idempotency_key": "x'; DROP TABLE payments;--", "amount": "1 OR 1=1"})
    bad("body of 2 MB", "POST", pay, raw=b'{"pad":"' + b"a" * (2 << 20) + b'"}')
    if tg.name == "final":
        bad("wrong type for student_id", "POST", pay, body={**base, "student_id": "1 OR 1=1"})
        bad("unknown currency format", "POST", pay, body={**base, "currency": "'; DROP--"})
        bad("SQL injection in course id", "POST", "/api/students/1/courses", body={"course_id": "x' OR '1'='1"})
        reg = {"idempotency_key": f"e6r-{rep}", "student_id": 1, "course_id": "CSE2501", "amount": 10.0}
        bad("registration with negative amount", "POST", "/api/registrations", body={**reg, "amount": -5})
        bad("registration with SQL in course id", "POST", "/api/registrations", body={**reg, "course_id": "x'; DROP TABLE courses;--"})
        bad("registration without key", "POST", "/api/registrations", body={"student_id": 1, "course_id": "CSE2501", "amount": 10.0})
        bad("non-numeric transcript id", "GET", "/api/transcripts/1%27%20OR%201=1")
    time.sleep(4)

    # Burst from one client. 300 requests as fast as 32 connections can send them.
    read_path = "/api/courses" if tg.name == "final" else tg.transcript
    spoof = Load([(1000, 0.3)], lambda i: ("GET", read_path, None, tok), workers=32).start().wait()
    burst = summarize(spoof)
    time.sleep(4)
    # The same burst with a new X-Forwarded-For per request tries to escape the limiter.
    results = []
    lock = threading.Lock()

    def spoofed(start):
        conn = http.client.HTTPConnection(GATEWAY_HOST, GATEWAY_PORT, timeout=CLIENT_TIMEOUT)
        for i in range(25):
            s = request("GET", read_path, None, tok, {"X-Forwarded-For": f"10.9.{start}.{i}"}, conn=conn)
            with lock:
                results.append((0, s[0], s[2], s[1]))
        conn.close()

    threads = [threading.Thread(target=spoofed, args=(n,)) for n in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    spoofed_summary = summarize(results)
    # Recovery of normal service
    time.sleep(3)
    good, _, seconds = request("GET", read_path, None, tok)
    health = request("GET", "/healthz")[0]
    after = tg.fingerprint()
    stability = sh('docker ps --format "{{.Names}} {{.Status}}"')
    prefix = "api-gateway|academic|payment|student|uis-postgres" if tg.name == "final" else "midterm"
    return {
        "malformed": cases,
        "malformed_rejected": sum(c["rejected"] for c in cases), "malformed_total": len(cases),
        "server_errors": sum(c["server_error"] for c in cases),
        "burst": burst, "burst_with_spoofed_xff": spoofed_summary,
        "normal_request_after": good, "normal_latency_ms": round(seconds * 1000, 1), "health_after": health,
        "data_unchanged": before == after,
        "containers": [line for line in stability.splitlines() if re.match(prefix, line)],
    }


def e7_recovery_consistency(tg, rep, payments=300, workers=8):
    """Crash a payment instance during a batch. Every 4th payment is sent twice at the
    same moment, as a client would on a lost response. Then count what is in the database.
    The limiter is raised as in E3: the midterm has none, so 429s would hide the crash."""
    tg.reset({"RATE_LIMIT_RPS": "100000", "RATE_LIMIT_BURST": "100000"})
    tok = tg.token
    acked = {}
    attempts = {"total": 0, "failed": 0, "server_500": 0}
    failure_codes = {}
    lock = threading.Lock()
    index = {"next": 0}

    def send(key, amount):
        body = {"idempotency_key": key, "student_id": 1, "amount": amount}
        backoff = 0.2
        for _ in range(10):
            status, text, _ = request("POST", tg.pay, body, tok)
            with lock:
                attempts["total"] += 1
                if status not in (200, 201):
                    attempts["failed"] += 1
                    failure_codes[str(status)] = failure_codes.get(str(status), 0) + 1
                if status == 500:
                    attempts["server_500"] += 1
            if status in (200, 201):
                return True
            time.sleep(backoff)
            backoff = min(backoff * 2, 2)
        return False

    def worker():
        while True:
            with lock:
                i = index["next"]
                index["next"] += 1
            if i >= payments:
                return
            key, amount = f"e7-{rep}-{i}", round(1000 + i / 100, 2)
            if i % 4 == 0:
                twin = threading.Thread(target=send, args=(key, amount))
                twin.start()
                ok = send(key, amount)
                twin.join()
            else:
                ok = send(key, amount)
            if ok:
                with lock:
                    acked[key] = amount
            time.sleep(0.02)

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    start = time.perf_counter()
    for t in threads:
        t.start()
    while index["next"] < payments / 3:
        time.sleep(0.01)
    tg.crash(tg.pay_victim)
    crashed_at = time.perf_counter() - start
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start
    time.sleep(1)
    rows = tg.sql("SELECT amount, count(*) FROM payments GROUP BY amount").splitlines()
    in_db = {float(r.split("|")[0]): int(r.split("|")[1]) for r in rows if "|" in r}
    return {
        "payments_planned": payments, "acknowledged": len(acked), "rows_in_db": sum(in_db.values()),
        "distinct_in_db": len(in_db),
        "duplicate_rows": sum(n - 1 for n in in_db.values() if n > 1),
        "lost_acknowledged": sum(1 for v in acked.values() if v not in in_db),
        "not_acknowledged_but_stored": sum(1 for v in in_db if v not in set(acked.values())),
        "http_attempts": attempts["total"], "failed_attempts": attempts["failed"], "server_500": attempts["server_500"],
        "failure_codes": failure_codes,
        "crash_at_sec": round(crashed_at, 2), "duration_sec": round(elapsed, 2),
        "orphan_payments_without_key": int(tg.sql("SELECT (SELECT count(*) FROM payments) - (SELECT count(*) FROM idempotency_keys)") or 0),
        "victim_state_after": sh(f'docker inspect -f "{{{{.State.Status}}}} restarts={{{{.RestartCount}}}}" {tg.pay_victim}'),
    }


def ensure_netem_image():
    if "Error" in sh("docker image inspect uis-netem"):
        subprocess.run("docker build -t uis-netem -", shell=True, check=True, text=True, capture_output=True,
                       input='FROM alpine:3.19\nRUN apk add --no-cache iproute2\nENTRYPOINT ["tc"]\n')


def netem(container, delay_ms=None):
    """Adds delay to everything leaving the container. The application containers have no
    NET_ADMIN, so a helper joins their network namespace with that capability."""
    args = f"qdisc replace dev eth0 root netem delay {delay_ms}ms" if delay_ms else "qdisc del dev eth0 root"
    return sh(f"docker run --rm --network container:{container} --cap-add NET_ADMIN uis-netem {args}")


def gateway_metric(name):
    return prom_counter_sum(name)


def e8_network_delay(tg, rep):
    """Delay on the link to the backends, in three sizes. Traffic starts first, the delay
    runs from second 5 to second 20, then the system has 10 s to return to normal."""
    ensure_netem_image()
    replicas = tg.replicas[:2]
    out = {}
    for name, delay, victims, rate, timeout in (
            ("tolerable_200ms_all", 200, replicas, 10, CLIENT_TIMEOUT),
            ("slow_4s_one_replica", 4000, replicas[:1], 10, CLIENT_TIMEOUT),
            ("slow_4s_all", 4000, replicas, 3, 12.0)):
        tg.reset({"RATE_LIMIT_RPS": "100000", "RATE_LIMIT_BURST": "100000"})
        tok = tg.token
        for _ in range(12):  # cache filled, as in E2
            request("GET", tg.transcript, token=tok)
        retries_before = gateway_metric("upstream_retries_total") if tg.name == "final" else None
        load = Load([(rate, 30)], lambda i: ("GET", tg.transcript, None, tok), timeout=timeout).start()
        time.sleep(5)
        injected = load.now()
        for c in victims:
            netem(c, delay)
        time.sleep(15)
        removed_health = replica_health("academic-service-1:8083") if tg.name != "midterm" else None
        for c in victims:
            netem(c)
        restored = load.now()
        samples = load.wait()
        save_samples(f"E8-{name}", tg.name, rep, samples)
        during = [s for s in samples if injected <= s[0] <= restored]
        after = [s for s in samples if s[0] > restored + 3]
        last_bad = max([s[0] + s[2] for s in samples if s[0] > injected and (s[1] != 200 or s[2] > 1)], default=None)
        out[name] = {
            **summarize(samples),
            "during_fault": summarize(during),
            "after_fault": summarize(after),
            "degraded_responses": sum(1 for s in samples if "DEGRADED" in s[3] or "degraded" in s[3]),
            "gateway_retries": gateway_metric("upstream_retries_total") - retries_before if tg.name == "final" else None,
            "replica_healthy_flag_during_fault": removed_health,
            "normal_after_restore_sec": max(0.0, round(last_bad - restored, 2)) if last_bad else 0.0,
        }
    return out


CRASH_POINTS = ("after_start", "after_seat", "after_charge", "after_void")
DECLINED_AMOUNT = 99999999  # above the payment limit, so the charge is refused and the saga compensates


def saga_invariants(tg):
    q = lambda text: int(tg.sql(text) or 0)
    return {
        "states": dict(line.split("|") for line in tg.sql("SELECT status, count(*) FROM registrations GROUP BY status").splitlines() if "|" in line),
        "not_final": q("SELECT count(*) FROM registrations WHERE status IN ('STARTED','SEAT_RESERVED','CANCELLING')"),
        "confirmed_without_payment": q("SELECT count(*) FROM registrations r LEFT JOIN payments p ON p.id = r.payment_id AND p.status = 'COMPLETED' WHERE r.status = 'CONFIRMED' AND p.id IS NULL"),
        "confirmed_without_seat": q("SELECT count(*) FROM registrations r LEFT JOIN enrollments e ON e.student_id = r.student_id AND e.course_id = r.course_id WHERE r.status = 'CONFIRMED' AND e.id IS NULL"),
        "seat_without_confirmation": q("SELECT count(*) FROM enrollments e JOIN registrations r ON e.student_id = r.student_id AND e.course_id = r.course_id WHERE r.status <> 'CONFIRMED'"),
        "charged_without_confirmation": q("SELECT count(*) FROM payments p JOIN registrations r ON r.student_id = p.student_id WHERE p.status = 'COMPLETED' AND r.status <> 'CONFIRMED'"),
        "duplicate_charges": q("SELECT coalesce(sum(n - 1), 0) FROM (SELECT count(*) n FROM payments WHERE status = 'COMPLETED' GROUP BY student_id) x"),
        "seat_counter_drift": q("SELECT coalesce(sum(abs(c.enrolled - (SELECT count(*) FROM enrollments e WHERE e.course_id = c.id))), 0) FROM courses c"),
    }


def wait_saga_final(tg, limit):
    start = time.perf_counter()
    while time.perf_counter() - start < limit:
        if tg.sql("SELECT count(*) FROM registrations WHERE status IN ('STARTED','SEAT_RESERVED','CANCELLING')") == "0":
            return round(time.perf_counter() - start, 2)
        time.sleep(0.5)
    return None


def wait_replicas(names, limit=15):
    end = time.time() + limit
    while time.time() < end and not all(replica_health(n) == 1 for n in names):
        time.sleep(0.2)


def saga_crashes(tg, rep, recovery, per_point):
    """Kill the student service between two steps of the workflow. Half of the clients
    repeat the request with the same key, as a client does after a 502."""
    tg.reset({"FAULT_INJECTION": "on", "SAGA_RECOVERY": recovery, "RATE_LIMIT_RPS": "100000", "RATE_LIMIT_BURST": "100000"})
    tg.seed_students()
    admin = tg.login("admin1")
    student = 1000
    answers = {}
    for point in CRASH_POINTS:
        for i in range(per_point):
            student += 1
            key = f"fault-{point}-{rep}-{i}"
            amount = DECLINED_AMOUNT if point == "after_void" else 1000.0
            body = {"idempotency_key": key, "student_id": student, "course_id": "CSE2501", "amount": amount}
            status = request("POST", "/api/registrations", body, admin)[0]
            answers[str(status)] = answers.get(str(status), 0) + 1
            wait_replicas(["student-service-1:8081", "student-service-2:8081"])
            if i % 2 == 0:
                request("POST", "/api/registrations", body, admin)
    for i in range(per_point):  # control group: the same workflow without a crash
        student += 1
        request("POST", "/api/registrations", {"idempotency_key": f"plain-{rep}-{i}", "student_id": student, "course_id": "CSE2501", "amount": 1000.0}, admin)
    settled = wait_saga_final(tg, 40 if recovery == "on" else 12)
    recovered = tg.sql("SELECT round(avg(extract(epoch FROM updated_at - created_at))::numeric, 2), round(max(extract(epoch FROM updated_at - created_at))::numeric, 2) FROM registrations WHERE attempts > 0")
    avg_s, _, max_s = recovered.partition("|")
    return {
        "registrations": student - 1000, "crashes_injected": per_point * len(CRASH_POINTS), "first_answer_codes": answers,
        "seconds_until_all_final": settled, "recovery_avg_sec": float(avg_s) if avg_s else None,
        "recovery_max_sec": float(max_s) if max_s else None, **saga_invariants(tg),
    }


def saga_outage(tg, rep, outage):
    """Payment service gone while registrations arrive, shorter and longer than the retry budget."""
    tg.reset({"RATE_LIMIT_RPS": "100000", "RATE_LIMIT_BURST": "100000"})
    tg.seed_students()
    admin = tg.login("admin1")
    sh("docker stop payment-service-1 payment-service-2")
    codes = {}
    for i in range(5):
        status = request("POST", "/api/registrations", {"idempotency_key": f"out-{outage}-{rep}-{i}", "student_id": 1100 + i, "course_id": "CSE2501", "amount": 1000.0}, admin)[0]
        codes[str(status)] = codes.get(str(status), 0) + 1
    time.sleep(outage)
    sh("docker start payment-service-1 payment-service-2")
    settled = wait_saga_final(tg, 45)
    return {"outage_sec": outage, "first_answer_codes": codes, "seconds_until_all_final": settled, **saga_invariants(tg)}


def e9_interrupted_workflow(tg, rep):
    return {
        "crashes_recovery_off": saga_crashes(tg, rep, "off", 8),
        "crashes_recovery_on": saga_crashes(tg, rep, "on", 8),
        "payment_outage_short": saga_outage(tg, rep, 10),
        "payment_outage_long": saga_outage(tg, rep, 45),
    }


def commit_latency(tg, n=150):
    """Sequential writes, so the number is the cost of one commit path and not of queueing."""
    tok = tg.token
    seconds = [request("POST", tg.pay, {"idempotency_key": f"lat-{time.time_ns()}", "student_id": 1, "amount": 2.0}, tok)[2] * 1000 for _ in range(n)]
    tg.sql("TRUNCATE payments, idempotency_keys RESTART IDENTITY")
    return {"mean_ms": round(statistics.mean(seconds), 2), "p95_ms": round(percentile(seconds, 95), 2)}


def write_outage(tg, rep, variant, kill):
    """Primary lost under 60 writes/s and 20 reads/s. Failed writes are not retried, so the
    acknowledged set is exact. Every acknowledged payment must exist afterwards."""
    env = {"RATE_LIMIT_RPS": "100000", "RATE_LIMIT_BURST": "100000"}
    tg.reset(env)
    tok = tg.token
    for _ in range(12):
        request("GET", tg.transcript, token=tok)
    if tg.name == "final":
        if variant == "async":
            tg.sql("ALTER SYSTEM SET synchronous_standby_names = ''")
            tg.sql("SELECT pg_reload_conf()")
            time.sleep(1)
        tg.set_failover(True)
    latency = commit_latency(tg) if tg.name == "final" else None

    amount = lambda i: round(1000 + i / 100, 2)
    writes = Load([(60, 40)], lambda i: ("POST", tg.pay, {"idempotency_key": f"e10-{rep}-{variant}-{i}", "student_id": 1, "amount": amount(i)}, tok))
    reads = Load([(20, 40)], lambda i: ("GET", tg.transcript, None, tok))
    writes.start()
    reads.start()
    time.sleep(10)
    injected = writes.now()
    t_kill = time.perf_counter()
    sh(kill)
    w = None
    if tg.name == "final":
        w = Watcher({"promoted": lambda: tg.sql("SELECT pg_is_in_recovery()", STANDBY) == "f"}, interval=0.25).start()
    write_samples = writes.wait()
    read_samples = reads.wait()
    first = w.stop() if w else {}
    save_samples(f"E10-writes-{variant}", tg.name, rep, write_samples)
    save_samples(f"E10-reads-{variant}", tg.name, rep, read_samples)

    good = lambda s: s[1] in (200, 201)
    after = [s for s in write_samples if s[0] >= injected]
    ok_after = [s for s in after if good(s)]
    # The window ends when writes work again for good, not at a lucky single success.
    last_bad = max([s[0] + s[2] for s in after if not good(s)], default=None)
    acked = {amount(i) for i, s in enumerate(sorted(writes.samples)) if good(s)}
    outage_reads = [s for s in read_samples if injected <= s[0] <= (last_bad or injected)]

    if tg.name != "final":  # no failover: the operator brings the same node back
        sh(f"docker start {tg.db}")
        time.sleep(4)
    live = STANDBY if tg.name == "final" else tg.db
    stored_raw = tg.sql("SELECT amount FROM payments", live).split()
    stored = {float(x) for x in stored_raw}
    result = {
        **summarize(write_samples, good),
        "writes_acknowledged": len(acked), "rows_in_db": len(stored_raw),
        "lost_acknowledged": len(acked - stored), "duplicate_rows": len(stored_raw) - len(stored),
        "stored_but_not_acknowledged": len(stored - acked),
        "write_outage_sec": round(last_bad - injected, 2) if last_bad else None,
        "write_outage_censored": bool(last_bad and last_bad > write_samples[-1][0] - 1),
        "promoted_after_sec": round(first["promoted"] - t_kill, 2) if "promoted" in first else None,
        "reads": summarize(read_samples), "reads_during_outage": summarize(outage_reads),
        "commit_latency": latency,
    }
    if tg.name == "final":  # fencing: the old primary returns and must get no traffic
        sh(f"docker start {tg.db}")
        deadline = time.time() + 20
        while time.time() < deadline and not tg.db_ok():
            time.sleep(0.5)
        time.sleep(3)
        before = tg.sql("SELECT count(*) FROM payments")
        status = request("POST", tg.pay, {"idempotency_key": f"e10-fence-{rep}-{variant}", "student_id": 1, "amount": 3.33}, tok)[0]
        result["write_after_old_primary_returned"] = status
        result["old_primary_rows_unchanged"] = tg.sql("SELECT count(*) FROM payments") == before
        result["write_reached_new_primary"] = tg.sql("SELECT count(*) FROM payments WHERE amount = 3.33", STANDBY) == "1"
    return result


def standby_lost(tg, rep):
    """The cost of synchronous commit: without the standby a commit cannot be acknowledged."""
    tg.reset({"RATE_LIMIT_RPS": "100000", "RATE_LIMIT_BURST": "100000"})
    tok = tg.token
    for _ in range(12):
        request("GET", tg.transcript, token=tok)
    sh(f"docker stop {STANDBY}")
    keys = [f"e10-standby-{rep}-{i}" for i in range(10)]
    attempts = []
    for i, key in enumerate(keys):
        attempts.append(request("POST", tg.pay, {"idempotency_key": key, "student_id": 1, "amount": 10 + i}, tok))
    read = request("GET", tg.transcript, token=tok)
    sh(f"docker start {STANDBY}")
    deadline = time.time() + 30
    while time.time() < deadline and tg.sql("SELECT count(*) FROM pg_stat_replication WHERE sync_state = 'sync'") != "1":
        time.sleep(0.5)
    time.sleep(2)
    retried = [request("POST", tg.pay, {"idempotency_key": key, "student_id": 1, "amount": 10 + i}, tok)[0] for i, key in enumerate(keys)]
    rows = int(tg.sql("SELECT count(*) FROM payments"))
    return {
        "writes_sent": len(keys), "write_codes": {str(c): sum(1 for a in attempts if a[0] == c) for c in {a[0] for a in attempts}},
        "write_max_ms": round(max(a[2] for a in attempts) * 1000), "read_status": read[0],
        "retry_codes": {str(c): retried.count(c) for c in set(retried)}, "rows_after_retry": rows, "duplicates": rows - len(keys),
    }


def e10_database_failover(tg, rep):
    if tg.name != "final":
        return write_outage(tg, rep, tg.name, f"docker kill {tg.db}")
    results = {}
    for variant in ("sync", "async"):
        tg.rebuild({"RATE_LIMIT_RPS": "100000", "RATE_LIMIT_BURST": "100000"})
        results[variant] = write_outage(tg, rep, variant, f"docker kill {tg.db}")
    tg.rebuild()
    results["standby_lost"] = standby_lost(tg, rep)
    return results


def pay_many(tg, prefix, n, base):
    return sum(request("POST", tg.pay, {"idempotency_key": f"{prefix}-{i}", "student_id": 1, "amount": base + i}, tg.token)[0] == 201 for i in range(n))


def e11_backup_restore(tg, rep):
    """An operator error reaches the standby at once. Only the backup brings the data back,
    and writes made after the backup are the recovery point."""
    tg.reset()
    pay_many(tg, f"e11a-{rep}", 40, 100)
    start = time.perf_counter()
    dump = sh(f"docker exec {tg.db} pg_dump -Fc -U uis_user -d uis_db -f /backups/e11.dump")
    dump_sec = time.perf_counter() - start
    before = tg.sql("SELECT count(*), md5(string_agg(x::text, ',' ORDER BY id)) FROM payments x")
    time.sleep(4)
    pay_many(tg, f"e11b-{rep}", 8, 500)
    tg.sql("TRUNCATE payments, idempotency_keys")
    time.sleep(1)
    standby_rows_after_error = tg.sql("SELECT count(*) FROM payments", STANDBY)
    start = time.perf_counter()
    restore = sh(f"docker exec {tg.db} pg_restore --clean --if-exists -U uis_user -d uis_db /backups/e11.dump")
    restore_sec = time.perf_counter() - start
    after = tg.sql("SELECT count(*), md5(string_agg(x::text, ',' ORDER BY id)) FROM payments x")
    time.sleep(1)
    payment = request("POST", tg.pay, {"idempotency_key": f"e11c-{rep}", "student_id": 1, "amount": 9.0}, tg.token)[0]
    return {
        "rows_at_backup": int(before.split("|")[0]), "writes_after_backup": 8, "rows_on_standby_after_error": int(standby_rows_after_error),
        "rows_after_restore": int(after.split("|")[0]), "checksum_matches_backup": before == after,
        "writes_lost_after_backup": 8, "dump_sec": round(dump_sec, 2), "restore_sec": round(restore_sec, 2),
        "payment_after_restore": payment, "restore_errors": restore[:300],
    }


def e12_reliability_run(tg, rep, seconds=300, mean_gap=40, operator_delay=30):
    """Random replica crashes, one at a time, at exponential gaps under steady traffic. This
    is an accelerated test: the numbers describe recovery, not how often the system fails
    in normal life. The midterm cannot restart itself, so an operator delay is assumed."""
    tg.reset({"RATE_LIMIT_RPS": "100000", "RATE_LIMIT_BURST": "100000"})
    tok = tg.token
    rng = random.Random(1000 + rep)  # same crash plan on both systems
    amount = lambda i: round(1 + i / 100, 2)

    def make(i):
        if i % 10 == 0:
            return "POST", tg.pay, {"idempotency_key": f"e12-{rep}-{i}", "student_id": 1, "amount": amount(i)}, tok
        return "GET", tg.transcript, None, tok

    load = Load([(10, seconds)], make).start()
    incidents = []
    while True:
        gap = rng.expovariate(1 / mean_gap)
        if load.now() + gap > seconds - 20:
            break
        time.sleep(gap)
        victim = rng.choice(tg.replicas)
        injected = load.now()
        t0 = time.perf_counter()
        tg.crash(victim)
        if tg.name != "midterm":
            port = {"student": 8081, "payment": 8082, "academic": 8083}[victim.split("-")[0]]
            target = f"{victim}:{port}"
            end = t0 + 5
            while time.perf_counter() < end and replica_health(target) != 0:
                time.sleep(0.05)
            detected = time.perf_counter() - t0
            end = t0 + 30
            while time.perf_counter() < end and replica_health(target) != 1:
                time.sleep(0.05)
            repaired = time.perf_counter() - t0
        else:
            detected = None
            time.sleep(operator_delay)
            sh(f"docker start {victim}")
            repaired = time.perf_counter() - t0
        incidents.append({"t_sec": round(injected, 1), "victim": victim, "gap_sec": round(gap, 1),
                          "detect_sec": round(detected, 2) if detected else None, "repair_sec": round(repaired, 2)})
    samples = load.wait()
    save_samples("E12", tg.name, rep, samples)
    good = lambda s: s[1] in (200, 201)
    second = {}
    for s in samples:
        second.setdefault(int(s[0]), []).append(good(s))
    ack = {amount(i) for i, s in enumerate(samples) if i % 10 == 0 and s[1] in (200, 201)}  # order of samples is by send time
    stored_raw = tg.sql("SELECT amount FROM payments").split()
    return {
        **summarize(samples, good), "duration_sec": seconds, "replicas": len(tg.replicas), "incidents": incidents,
        "seconds_with_failures": sum(1 for v in second.values() if not all(v)), "seconds_observed": len(second),
        "payment_rows": len(stored_raw), "duplicate_rows": len(stored_raw) - len({float(x) for x in stored_raw}),
    }


def e13_gateway_crash(tg, rep):
    """The gateway is one container, so this measures the size of a known weak point."""
    tg.reset()
    tok = tg.token
    load = Load([(15, 25)], lambda i: ("GET", tg.transcript, None, tok)).start()
    time.sleep(5)
    injected = load.now()
    tg.crash("api-gateway")
    samples = load.wait()
    save_samples("E13", tg.name, rep, samples)
    bad = [s for s in samples if s[1] != 200]
    return {**summarize(samples), "outage_sec": round(bad[-1][0] + bad[-1][2] - injected, 2) if bad else 0.0,
            "state_after": sh('docker inspect -f "{{.State.Status}} restarts={{.RestartCount}}" api-gateway')}


EXPERIMENTS = {
    "E1": ("Service instance failure", e1_instance_failure, 5, ("midterm", "endterm", "final")),
    "E2": ("Dependency timeout (database frozen)", e2_dependency_block, 3, ("midterm", "endterm", "final")),
    "E3": ("Load spike", e3_load_spike, 3, ("midterm", "endterm", "final")),
    "E4": ("Monitoring and alert test", e4_alerts, 3, ("final",)),
    "E5": ("Unauthorized access", e5_unauthorized, 3, ("midterm", "final")),
    "E6": ("Abusive and malformed requests", e6_abuse, 3, ("midterm", "final")),
    "E7": ("Recovery and consistency", e7_recovery_consistency, 5, ("midterm", "endterm", "final")),
    "E8": ("Network delay and timeouts", e8_network_delay, 3, ("midterm", "endterm", "final")),
    "E9": ("Interrupted multi-step workflow", e9_interrupted_workflow, 3, ("final",)),
    "E10": ("Database failover", e10_database_failover, 3, ("midterm", "endterm", "final")),
    "E11": ("Backup and restore", e11_backup_restore, 3, ("final",)),
    "E12": ("Reliability run with random crashes", e12_reliability_run, 3, ("midterm", "endterm", "final")),
    "E13": ("Gateway crash", e13_gateway_crash, 3, ("endterm", "final")),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="*", default=list(EXPERIMENTS))
    ap.add_argument("--target", choices=["midterm", "endterm", "final"])
    ap.add_argument("--reps", type=int, help="override the repetition count")
    args = ap.parse_args()

    os.makedirs(RAW, exist_ok=True)
    out_path = os.path.join(RESULTS, "experiment_results.json")
    output = {"experiments": {}}
    if os.path.exists(out_path):
        with open(out_path, encoding="utf-8") as f:
            output = json.load(f)
    output["environment"] = {
        "date": datetime.datetime.now().isoformat(timespec="seconds"), "host_os": platform.platform(),
        "cpu_count": os.cpu_count(), "docker": sh('docker version --format "{{.Server.Version}}"'),
        "note": "All services, the database and the load generator share one Windows host running Docker Desktop.",
        "baseline_commit": "871967a (github.com/papercontroller/midterm-ftr)",
    }

    for target_name in ("midterm", "endterm", "final"):
        if args.target and args.target != target_name:
            continue
        chosen = [e for e in args.only if target_name in EXPERIMENTS[e][3]]
        if not chosen:
            continue
        tg = Target(target_name)
        for other in {"midterm", "endterm", "final"} - {target_name}:
            Target(other).stop()
        tg.start(build=True)
        for exp in chosen:
            title, fn, reps, _ = EXPERIMENTS[exp]
            reps = args.reps or reps
            print(f"== {exp} {title} / {target_name} / {reps} reps", flush=True)
            runs = []
            for rep in range(1, reps + 1):
                t0 = time.time()
                r = fn(tg, rep)
                r["rep"] = rep
                runs.append(r)
                print(f"  rep {rep}: {round(time.time() - t0)}s {json.dumps(r, ensure_ascii=False)[:420]}", flush=True)
            entry = output["experiments"].setdefault(exp, {"title": title, "targets": {}})
            entry["targets"][target_name] = {"repetitions": reps, "runs": runs}
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
        tg.stop()
    Target("final").start()
    print("results written to", out_path)


if __name__ == "__main__":
    main()
