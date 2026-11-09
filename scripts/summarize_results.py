"""Prints the per-experiment means from results/experiment_results.json. The report tables come from this output."""
import json
import os
import statistics
import sys

sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(ROOT, "results", "experiment_results.json"), encoding="utf-8") as f:
    data = json.load(f)
exps = data["experiments"]
print(data["environment"])


def runs(e, t):
    return exps.get(e, {}).get("targets", {}).get(t, {}).get("runs", [])


def spread(values):
    values = [v for v in values if v is not None]
    if not values:
        return "n/a"
    return f"{statistics.mean(values):.2f} [{min(values):g}..{max(values):g}] n={len(values)}"


for t in ("midterm", "endterm", "final"):
    print("\n#####", t)
    r = runs("E1", t)
    print("E1 reps", len(r), "total", sum(x["total"] for x in r), "ok", sum(x["success"] for x in r))
    for k in ("availability_pct", "latency_avg_ms", "latency_p95_ms", "latency_max_ms", "detection_time_sec", "recovery_time_sec"):
        print("  ", k, spread([x.get(k) for x in r]))
    print("   window p95", spread([x["failure_window"]["latency_p95_ms"] for x in r]), "max", spread([x["failure_window"]["latency_max_ms"] for x in r]))
    print("   after", [x["victim_state_after"] for x in r], "alerts", [x["alerts"] for x in r])

    r = runs("E2", t)
    print("E2 reps", len(r), "total", sum(x["total"] for x in r), "ok", sum(x["success"] for x in r), "codes", [x["status_codes"] for x in r])
    for k in ("availability_pct", "real_data_during_fault_pct", "degraded_responses", "first_degraded_after_sec", "first_fresh_after_resume_sec", "recovery_after_resume_sec", "breaker_state_during_fault", "latency_p95_ms", "latency_max_ms"):
        print("  ", k, spread([x.get(k) for x in r]))
    print("   during", spread([x["during_fault"]["latency_avg_ms"] for x in r]), "p95", spread([x["during_fault"]["latency_p95_ms"] for x in r]), "fail", [x["during_fault"]["failed"] for x in r])
    print("   pay", [x["payment_during_fault"] for x in r], "rows", [x["payments_in_db_after"] for x in r])

    r = runs("E3", t)
    print("E3 reps", len(r))
    for k in ("availability_pct", "latency_avg_ms", "latency_p95_ms", "latency_max_ms"):
        print("  ", k, spread([x["spike"][k] for x in r]))
    print("   rps", spread([x["spike"]["achieved_rps"] for x in r]), "codes", [x["status_codes"] for x in r])
    print("   acked", [x["payments_acknowledged"] for x in r], "db", [x["payments_in_db"] for x in r])
    if r:
        print("   res", r[-1]["resources_during_spike"])

    r = runs("E5", t)
    print("E5 reps", len(r), "blocked", [f'{x["denied"]}/{x["total"]}' for x in r], "unchanged", [x["data_unchanged"] for x in r], "audit", [x["audit_events_gateway"] for x in r])
    for c in (r[-1]["cases"] if r else []):
        print("    ", c["case"], c["status"], "exp", c["expected"], "exposed", c["data_exposed"], "pass", c["passed"])

    r = runs("E6", t)
    print("E6 reps", len(r), "rejected", [f'{x["malformed_rejected"]}/{x["malformed_total"]}' for x in r], "5xx", [x["server_errors"] for x in r], "unchanged", [x["data_unchanged"] for x in r])
    print("   burst", [x["burst"]["status_codes"] for x in r], "xff", [x["burst_with_spoofed_xff"]["status_codes"] for x in r], "after", [(x["normal_request_after"], x["health_after"]) for x in r])
    for c in (r[-1]["malformed"] if r else []):
        print("    ", c["case"], c["status"])

    r = runs("E7", t)
    print("E7 reps", len(r))
    for k in ("acknowledged", "rows_in_db", "duplicate_rows", "lost_acknowledged", "not_acknowledged_but_stored", "orphan_payments_without_key", "http_attempts", "failed_attempts", "server_500", "duration_sec", "crash_at_sec"):
        print("  ", k, spread([x.get(k) for x in r]))
    print("   codes", [x["failure_codes"] for x in r], [x["victim_state_after"] for x in r])

print("\n##### E4")
scen = {}
for run in runs("E4", "final"):
    for s in run["scenarios"]:
        scen.setdefault(s["scenario"], []).append(s)
for name, items in scen.items():
    print(name, "expected", items[0]["expected_alert"], "fired", [i["fired"] for i in items], "delay", spread([i["detection_delay_sec"] for i in items]), "extra", [i["extra_alerts"] for i in items])
