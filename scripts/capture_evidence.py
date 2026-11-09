"""Runs one short demonstration incident on the final system and saves evidence:
a Grafana screenshot, sample logs that share a correlation ID, audit logs and the
alert rules state. Numbers in the report come from run_experiments.py, not from here.

    docker compose up -d --build
    python scripts/capture_evidence.py
"""
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from run_experiments import PROM, RESULTS, ROOT, Load, Target, request, sh

OUT = os.path.join(RESULTS, "evidence")
EDGE_PATHS = [r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
              r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"]
BACKENDS = ["academic-service-1", "academic-service-2"]


def timed_phase(name, seconds):
    print(name, flush=True)
    time.sleep(seconds)


def main():
    os.makedirs(OUT, exist_ok=True)
    tg = Target("final")
    tg.reset()
    tok = tg.token
    for _ in range(12):
        request("GET", tg.transcript, token=tok)

    load = Load([(10, 170)], lambda i: ("GET", tg.transcript, None, tok)).start()
    timed_phase("normal traffic", 30)
    tg.crash(tg.victim)
    timed_phase("replica crashed", 30)
    sh(f"docker pause {tg.db}")
    timed_phase("database frozen", 30)
    sh(f"docker unpause {tg.db}")
    timed_phase("database back", 20)
    burst = Load([(60, 20)], lambda i: ("GET", "/api/courses", None, tok), workers=32).start()
    timed_phase("request burst", 25)
    burst.wait()
    for path in ("/api/transcripts/2", "/api/students"):
        request("GET", path, token=tok)
    request("GET", "/api/transcripts/1")
    with open(os.path.join(OUT, "alert_rules_state.json"), "w", encoding="utf-8") as f:
        json.dump(json.load(urllib.request.urlopen(PROM + "/api/v1/rules", timeout=5)), f, indent=1)
    load.halted.set()
    load.wait()

    # One request followed across services by its correlation ID
    status, body, _ = request("GET", tg.transcript, token=tok, headers={"X-Correlation-ID": "demo-trace-0001"})
    time.sleep(1)
    with open(os.path.join(OUT, "sample_correlated_request.log"), "w", encoding="utf-8") as f:
        for c in ["api-gateway"] + BACKENDS:
            f.writelines(line + "\n" for line in sh(f"docker logs {c}").splitlines() if "demo-trace-0001" in line)

    with open(os.path.join(OUT, "sample_gateway_incident.log"), "w", encoding="utf-8") as f:
        logs = sh("docker logs api-gateway").splitlines()
        wanted = [l for l in logs if '"AUDIT"' in l or "health changed" in l or "Rate limit" in l]
        f.writelines(line + "\n" for line in wanted[:40])
    with open(os.path.join(OUT, "sample_degraded_mode.log"), "w", encoding="utf-8") as f:
        logs = sh("docker logs academic-service-2").splitlines()
        f.writelines(line + "\n" for line in [l for l in logs if "cache" in l][:10])

    edge = next((p for p in EDGE_PATHS if os.path.exists(p)), shutil.which("chrome"))
    if not edge:
        print("no headless browser found; open the dashboard and take the screenshot by hand")
        return
    url = "http://127.0.0.1:3000/d/uis-resilience?orgId=1&from=now-4m&to=now&kiosk"
    profile = os.path.join(OUT, ".edge-profile")
    subprocess.run([edge, "--headless=new", "--disable-gpu", "--hide-scrollbars", f"--user-data-dir={profile}",
                    f"--screenshot={os.path.join(OUT, 'grafana_dashboard.png')}", "--window-size=1600,1500",
                    "--virtual-time-budget=20000", url], timeout=90, cwd=ROOT)
    shutil.rmtree(profile, ignore_errors=True)
    print("saved to", OUT)


if __name__ == "__main__":
    main()
