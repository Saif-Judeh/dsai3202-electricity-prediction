import json
import math
import statistics
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

import requests

root = Path(__file__).resolve().parents[1]
examples = root / "deployment/examples"
payload = json.loads((examples / "valid_request.json").read_text())
ref = json.loads((examples / "offline_reference.json").read_text())
expected, = ref["predictions_kwh"]

endpoint = "electricity-60306539"
scope = ["-g", "rg-dsai3202-saif-60306539", "-w", "mlwdsai320260306539",
         "-n", endpoint]

def endpoint_value(command, field):
    return subprocess.check_output(
        ["az", "ml", "online-endpoint", command, *scope,
         "--query", field, "-o", "tsv", "--only-show-errors"],
        text=True,
    ).strip()

url = endpoint_value("show", "scoring_uri")
key = endpoint_value("get-credentials", "primaryKey")
if not url.startswith("https://") or not key:
    raise SystemExit("Could not retrieve the endpoint URL and key.")

count = 20
target_ms = 1000
started = datetime.now(timezone.utc)
path = root / "evidence" / f"azure_latency_{started:%Y%m%dT%H%M%SZ}.json"
report = {
    "endpoint": endpoint,
    "started_utc": started.isoformat(),
    "method": "Sequential single-record requests from the VM using one HTTPS session",
    "warmup_requests_excluded": 1,
    "required_successes": count,
    "p95_target_ms": target_ms,
    "requests": [],
}
path.write_text(json.dumps(report, indent=2) + "\n")

def call(session):
    start = perf_counter()
    response = session.post(url, json=payload, timeout=15)
    elapsed_ms = (perf_counter() - start) * 1000
    response.raise_for_status()
    body = response.json()
    if isinstance(body, str):
        body = json.loads(body)
    actual, = body["predictions_kwh"]
    if not math.isclose(actual, expected, rel_tol=ref["relative_tolerance"],
                        abs_tol=ref["absolute_tolerance_kwh"]):
        raise ValueError("Prediction differs from offline reference")
    return elapsed_ms

with requests.Session() as session:
    session.headers["Authorization"] = f"Bearer {key}"
    call(session)  # Warm-up, excluded from timing statistics
    for number in range(1, count + 1):
        try:
            ms = call(session)
            result = {"request": number, "ok": True, "latency_ms": ms}
        except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
            result = {"request": number, "ok": False, "error": str(exc)}
        report["requests"].append(result)

times = sorted(r["latency_ms"] for r in report["requests"] if r["ok"])
p95 = times[math.ceil(0.95 * len(times)) - 1] if times else None
summary = {
    "successful_predictions": len(times),
    "measured_requests": count,
    "median_ms": round(statistics.median(times), 2) if times else None,
    "p95_ms": round(p95, 2) if times else None,
    "meets_target": len(times) == count and p95 <= target_ms,
}
report["summary"] = summary
path.write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(summary, indent=2))
print("Evidence:", path)
