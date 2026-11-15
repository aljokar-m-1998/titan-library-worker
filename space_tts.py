"""Retire (or restore) Titan-Library narration on the Titan-Gateway Space.

main.py's lifespan calls start_library_worker(), which returns immediately when
LIBRARY_WORKER=0 (api/titan_library.py). One Space variable, no code edit,
and --state on puts it back.

Nothing else on the gateway is touched: api/books.py writes Titan-Books /
Titan-Books-Private, api/learn_voa.py writes Titan-Learn, api/titan_translate.py
writes Titan-Library-Arabic.
"""
from __future__ import annotations

import argparse
import json
import os
import time

from huggingface_hub import HfApi

SPACE = os.environ.get("HF_GATEWAY_SPACE", "M-hv1/Titan-Gateway")
KEY = "LIBRARY_WORKER"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", choices=("off", "on"), required=True)
    parser.add_argument("--restart-timeout", type=float, default=420.0)
    parser.add_argument("--no-restart", action="store_true")
    args = parser.parse_args()
    token = os.environ.get("HF_TOKEN", "").strip()
    if not token:
        print("HF_TOKEN is not set", flush=True)
        return 2
    api = HfApi(token=token)
    value = "0" if args.state == "off" else "1"
    before = api.get_space_runtime(SPACE)
    variables = api.get_space_variables(SPACE)
    report = {
        "space": SPACE, "key": KEY, "requested_value": value,
        "stage_before": before.stage,
        "value_before": variables[KEY].value if KEY in variables else None,
        # names only: a variable list must never carry a secret value into a log
        "variable_names": sorted(variables),
    }
    api.add_space_variable(SPACE, KEY, value,
                           description="Titan-Library narration retired; Actions ingests text only")
    report["value_after_set"] = api.get_space_variables(SPACE)[KEY].value
    if not args.no_restart:
        api.restart_space(SPACE)
        deadline = time.time() + args.restart_timeout
        stage = ""
        while time.time() < deadline:
            time.sleep(10)
            stage = api.get_space_runtime(SPACE).stage
            if stage == "RUNNING":
                break
            if stage in ("BUILD_ERROR", "RUNTIME_ERROR", "STOPPED"):
                report["refused"] = f"space ended in {stage}"
                print(json.dumps(report, ensure_ascii=False, indent=1))
                return 1
        report["stage_after"] = stage
        if stage != "RUNNING":
            report["refused"] = f"space did not return to RUNNING within {args.restart_timeout}s"
    print(json.dumps(report, ensure_ascii=False, indent=1))
    return 1 if report.get("refused") else 0


if __name__ == "__main__":
    raise SystemExit(main())