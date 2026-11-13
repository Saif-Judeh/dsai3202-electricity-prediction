#!/usr/bin/env python3
"""Test stage execution/recording without cloud calls or real project data."""
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from run_pipeline import execute_stage, ROOT


def main():
    checks = []
    with tempfile.TemporaryDirectory(prefix="pipeline-stage-tests-") as temp:
        root = Path(temp)
        (root / "scripts").mkdir()
        for label, body, pattern, expected in [
            ("success", "from pathlib import Path\nPath('evidence/ok.json').write_text('{\"status\":\"passed\"}')\nprint('ok')\n", "ok.json", "passed"),
            ("exit_failure", "raise SystemExit(7)\n", None, "failed"),
            ("report_failure", "from pathlib import Path\nPath('evidence/bad.json').write_text('{\"status\":\"failed\"}')\n", "bad.json", "failed"),
            ("missing_report", "print('no evidence written')\n", "absent.json", "failed")]:
            (root / "scripts/stage.py").write_text(body)
            entry, data = execute_stage(root, uuid4().hex, label, "stage.py", pattern=pattern)
            if entry["status"] != expected or not Path(entry["console_log"]).exists():
                raise AssertionError("Unexpected stage handling: " + label)
            checks.append({"test": label, "status": "passed"})
    evidence = ROOT / "evidence"
    evidence.mkdir(exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    path = evidence / f"pipeline_test_{run_id}.json"
    path.write_text(json.dumps({"status": "passed", "scope": "offline synthetic child stages", "checks": checks}, indent=2) + "\n")
    print(f"PASSED: {len(checks)} runner checks; evidence: {path}")


if __name__ == "__main__":
    main()
