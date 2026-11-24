#!/usr/bin/env python3
"""Heartbeat monitor for Titan-Library ingest pipeline.

Checks backoff state, live ingest workflow runs, target book count, and
dataset commit freshness, then dispatches `ingest.yml` when stalled.
Falls back seamlessly from `GH_DISPATCH_TOKEN` to `GITHUB_TOKEN`.
"""
from __future__ import annotations

import calendar
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Callable

UA = {
    "User-Agent": (
        "TitanLibrary-Heartbeat/1.0 "
        "(+https://github.com/aljokar-m-1998/titan-library-worker)"
    ),
    "Accept": "application/vnd.github+json",
}


def resolve_dispatch_token(env: dict[str, str] | None = None) -> tuple[str, str]:
    """Return (token, token_source) preferring GH_DISPATCH_TOKEN over GITHUB_TOKEN."""
    source_env = os.environ if env is None else env
    for key in ("GH_DISPATCH_TOKEN", "DISPATCH_TOKEN"):
        val = (source_env.get(key) or "").strip()
        if val:
            return val, key
    gh_tok = (source_env.get("GITHUB_TOKEN") or "").strip()
    if gh_tok:
        return gh_tok, "GITHUB_TOKEN"
    return "", ""


def http_call(
    url: str,
    *,
    token: str = "",
    body: dict[str, Any] | None = None,
    timeout: int = 60,
) -> tuple[int, Any]:
    headers = dict(UA)
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, headers=headers, data=data)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read()
        return response.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as error:
        raw = error.read()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"body": str(raw)[:200]}
        return error.code, payload
    except Exception as error:
        return 0, {"error": f"{type(error).__name__}: {str(error)[:160]}"}


def run_heartbeat(
    *,
    env: dict[str, str] | None = None,
    caller: Callable[..., tuple[int, Any]] = http_call,
    now_fn: Callable[[], float] = time.time,
    sleep_fn: Callable[[float], None] = time.sleep,
    poll_attempts: int = 6,
    poll_interval: float = 5.0,
) -> tuple[int, dict[str, Any]]:
    source_env = os.environ if env is None else env
    dataset = source_env.get("HF_DATASET", "M-hv1/Titan-Library").strip()
    repo = source_env["GH_REPO"].strip()
    token, token_source = resolve_dispatch_token(source_env)
    stale_after = int(source_env.get("STALE_MINUTES") or "180") * 60
    batch_size = (source_env.get("BATCH_SIZE") or "100").strip()
    target_books = int(source_env.get("TARGET_BOOKS") or "0")
    state_only = (source_env.get("STATE_ONLY") or "false").strip().lower() == "true"
    runs_url = f"https://api.github.com/repos/{repo}/actions/workflows/ingest.yml/runs?per_page=10"
    now = int(now_fn())
    checks: list[dict[str, Any]] = []
    problems: list[str] = []

    def note(ok: bool, label: str, detail: str = "") -> None:
        checks.append({"ok": ok, "label": label, "detail": detail})
        print(
            ("PASS " if ok else "FAIL ") + label + (f" {detail}" if detail else ""),
            file=sys.stderr,
            flush=True,
        )

    # 1 - deliberate park marker outranks every other signal
    status, marker = caller(
        f"https://huggingface.co/datasets/{dataset}/resolve/main/_worker/backoff.json"
    )
    parked_until = 0
    if status == 404:
        note(True, "no backoff marker on the shelf")
    elif status == 200 and isinstance(marker, dict):
        parked_until = int(marker.get("not_before_epoch") or 0)
        note(
            True,
            "backoff marker read",
            "not_before=" + time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(parked_until)),
        )
    else:
        note(False, "backoff lookup", f"HTTP {status} {str(marker)[:160]}")
        problems.append(f"backoff lookup HTTP {status}")

    # 2 - optional target book count check from checkpoint
    books_indexed: int | None = None
    cp_status, cp_data = caller(
        f"https://huggingface.co/datasets/{dataset}/resolve/main/_worker/library_checkpoint.json"
    )
    if cp_status == 200 and isinstance(cp_data, dict) and cp_data.get("books_indexed") is not None:
        books_indexed = int(cp_data["books_indexed"])
        note(True, "checkpoint read", f"books_indexed={books_indexed}")
    elif cp_status == 404:
        note(True, "no checkpoint file yet")
    else:
        note(True, "checkpoint lookup skipped", f"HTTP {cp_status}")

    # 3 - never stack a dispatch on a live writer (queued / in_progress / waiting)
    if not token:
        note(False, "run lookup token present", "neither GH_DISPATCH_TOKEN nor GITHUB_TOKEN is set")
        problems.append("no GitHub token available for run lookup or dispatch")
        records: list[dict[str, Any]] = []
    else:
        status, runs = caller(runs_url, token=token)
        if status != 200:
            note(False, "run lookup", f"HTTP {status} ({token_source}) {str(runs)[:160]}")
            problems.append(f"run lookup HTTP {status}")
            records = []
        else:
            records = (runs or {}).get("workflow_runs") or []
    alive = [r for r in records if r.get("status") != "completed"]
    newest_id = max([int(r.get("id") or 0) for r in records] or [0])
    note(True, "run queue read", f"alive={len(alive)} newest_run={newest_id} auth={token_source or 'none'}")

    # 4 - freshness comes from the dataset commit timestamp (UTC)
    status, commits = caller(
        f"https://huggingface.co/api/datasets/{dataset}/commits/main?limit=1"
    )
    age: int | None = None
    last_commit = ""
    if status == 200 and commits:
        last_commit = (commits[0].get("id") or "")[:12]
        parsed = time.strptime(commits[0]["date"][:19], "%Y-%m-%dT%H:%M:%S")
        age = now - calendar.timegm(parsed)
        note(True, "newest dataset commit", f"{last_commit} age={age // 60}m")
    else:
        note(False, "dataset commit lookup", f"HTTP {status} {str(commits)[:160]}")
        problems.append(f"dataset commit lookup HTTP {status}")

    target_reached = (
        target_books > 0 and books_indexed is not None and books_indexed >= target_books
    )
    if parked_until and now < parked_until:
        decision = "parked"
    elif alive:
        decision = "run-alive"
    elif target_reached:
        decision = "target-reached"
    elif age is not None and age < stale_after:
        decision = "fresh"
    else:
        decision = "dispatch"

    dispatched: int | None = None
    if decision != "dispatch":
        note(True, f"nothing to do ({decision})")
    elif state_only:
        note(True, "state_only: would dispatch, dispatching nothing")
    elif not token:
        note(False, "dispatch token present", "neither GH_DISPATCH_TOKEN nor GITHUB_TOKEN is set")
    else:
        status, body = caller(
            f"https://api.github.com/repos/{repo}/actions/workflows/ingest.yml/dispatches",
            token=token,
            body={"ref": "main", "inputs": {"batch_size": batch_size}},
        )
        note(status == 204, "dispatch accepted", f"HTTP {status} ({token_source}) {str(body)[:120]}")
        if status != 204:
            problems.append(f"dispatch HTTP {status}: {str(body)[:200]}")
        else:
            for _ in range(poll_attempts):
                sleep_fn(poll_interval)
                poll_status, fresh = caller(runs_url, token=token)
                ids = [
                    int(r.get("id") or 0)
                    for r in (((fresh or {}).get("workflow_runs") or []) if poll_status == 200 else [])
                ]
                newer = [i for i in ids if i > newest_id]
                if newer:
                    dispatched = max(newer)
                    break
            note(
                True,
                "dispatch poll completed",
                f"run={dispatched}" if dispatched is not None else "accepted (HTTP 204); run pending visibility",
            )

    payload = {
        "now_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
        "decision": decision,
        "token_source": token_source,
        "parked_until_epoch": parked_until,
        "books_indexed": books_indexed,
        "target_books": target_books,
        "alive_runs": len(alive),
        "newest_dataset_commit": last_commit,
        "commit_age_minutes": None if age is None else age // 60,
        "stale_minutes": stale_after // 60,
        "state_only": state_only,
        "dispatched_run": dispatched,
        "checks": checks,
        "problems": problems,
    }
    return (1 if problems else 0), payload


def main() -> int:
    code, payload = run_heartbeat()
    print(json.dumps(payload, ensure_ascii=False, indent=1))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
