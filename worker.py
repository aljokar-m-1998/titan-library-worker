"""Project Gutenberg -> M-hv1/Titan-Library, entirely on GitHub's runners.

Clean chapter text, structured metadata and the original cover image. No audio is
synthesized, uploaded or indexed: the EAD client narrates on device.

Vendored, not reinvented:
  commit accounting / rate window / backoff / rollback <- titan-learn-worker worker.py
  Gutenberg boilerplate split + chapteriser            <- Titan-Gateway api/library.py
  unicode repair                                       <- Titan-Gateway api/books.py
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import re
import signal
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath

import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import CommitOperationAdd, HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError, HfHubHTTPError

REPO = os.environ.get("HF_LIBRARY_DATASET_REPO", "M-hv1/Titan-Library")
REVISION = os.environ.get("HF_LIBRARY_DATASET_REVISION", "main")
CATALOG_URL = "https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv.gz"
UA = {"User-Agent": "TitanLibrary-CloudIngest/1.0 "
                    "(+https://github.com/aljokar-m-1998/titan-library-worker)"}
GUTENBERG_HOSTS = {"gutenberg.org", "www.gutenberg.org", "aleph.gutenberg.org"}
GUTENBERG_LICENSE_URL = "https://www.gutenberg.org/policy/license.html"
GUTENBERG_REDISTRIBUTION = (
    "Free to copy and redistribute at no cost, provided this notice and the full "
    "Project Gutenberg License stay attached to the copy (§1.E.1, §1.E.4). "
    "Project Gutenberg is a registered trademark: it may not be used when charging "
    "for an eBook except under the trademark license (§1.C, §1.E.8)."
)
RAW = "https://huggingface.co/datasets/{repo}/resolve/main/{path}"
LEGACY_ROOT = "books/gutendex"
ROOT_FILES = {"ledger.json", "catalog.parquet"}
LFS_SUFFIXES = (".jpg", ".jpeg", ".png", ".parquet", ".gz")

MAX_TEXT_BYTES = 8 * 1024 * 1024
MAX_COVER_BYTES = 2 * 1024 * 1024
MAX_CATALOG_BYTES = 64 * 1024 * 1024
MAX_TEXT_CHARS = 6_000_000
CHAPTER_TARGET = 24_000
MIN_CHAPTER_CHARS = 1_200
TOC_GAP_CHARS = 200
TOC_RUN = 4
CHAPTER_SANITY_CAP = 1200
MAX_ATTEMPTS = 5
DIR_ENTRY_LIMIT = 10_000

STATE_HOOK: dict = {}


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", file=sys.stderr, flush=True)


def stamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())


def token() -> str:
    value = os.environ.get("HF_TOKEN", "").strip()
    if not value:
        raise SystemExit("HF_TOKEN is not set")
    return value


def hf_api() -> HfApi:
    return HfApi(token=token())


def read_file(api, path: str, required: bool = False) -> bytes | None:
    try:
        local = hf_hub_download(repo_id=REPO, repo_type="dataset", filename=path,
                                revision=REVISION, token=api.token)
        return Path(local).read_bytes()
    except EntryNotFoundError:
        if required:
            raise SystemExit(f"required file missing from the dataset: {path}")
        return None
    except HfHubHTTPError as error:
        status = getattr(getattr(error, "response", None), "status_code", 0)
        if status in (401, 403):
            raise SystemExit(f"HF_TOKEN cannot read {REPO}: HTTP {status}")
        if required:
            raise
        return None


def cdn_bytes(path: str) -> bytes | None:
    url = RAW.format(repo=REPO, path=path)
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=120) as r:
            return r.read()
    except (urllib.error.URLError, OSError, TimeoutError):
        return None


# ==============================================================
#   Commit accounting - vendored from titan-learn-worker
# ==============================================================
def api_headers(url: str | None = None) -> dict:
    """The hub's own accounting, read off a cheap response instead of guessed at."""
    wanted = ("ratelimit", "ratelimit-policy", "retry-after")
    try:
        request = urllib.request.Request(
            url or f"https://huggingface.co/api/datasets/{REPO}",
            headers={"Authorization": "Bearer " + token()})
        with urllib.request.urlopen(request, timeout=60) as response:
            return {k: v for k, v in response.headers.items()
                    if any(p in k.lower() for p in wanted)}
    except urllib.error.HTTPError as error:
        return {k: v for k, v in (error.headers or {}).items()
                if any(p in k.lower() for p in wanted)} or {"http_status": str(error.code)}
    except Exception as error:
        return {"probe_error": f"{type(error).__name__}: {str(error)[:80]}"}


def rate_state(headers: dict) -> dict:
    """Parse RateLimit `"api";r=988;t=277` plus the q/w window out of RateLimit-Policy."""
    state = {"remaining": None, "reset_in": None, "quota": None, "window": None}
    for part in (headers.get("RateLimit") or headers.get("ratelimit") or "").split(";"):
        part = part.strip().strip('"')
        if part.startswith("r="):
            state["remaining"] = int(part[2:])
        elif part.startswith("t="):
            state["reset_in"] = int(part[2:])
    policy = headers.get("RateLimit-Policy") or headers.get("ratelimit-policy") or ""
    quota = re.search(r"q=(\d+)", policy)
    window = re.search(r"w=(\d+)", policy)
    if quota:
        state["quota"] = int(quota.group(1))
    if window:
        state["window"] = int(window.group(1))
    retry = headers.get("Retry-After") or headers.get("retry-after")
    if retry:
        state["retry_after"] = int(retry)
    return state


class Budget:
    """Commit accounting measured from the dataset, never from an assumed HF number.

    The observed window is `q=1000; w=300` requests per 5 minutes; these caps are the
    operator's precaution: they stop before a limit is reached rather than discovering it.
    """

    def __init__(self, api, max_per_run: int, min_interval: float, min_remaining: int) -> None:
        self.api = api
        self.max_per_run = max_per_run
        self.min_interval = min_interval
        self.min_remaining = min_remaining
        self.used = 0
        self.headers: dict = {}
        self.state: dict = {}
        self.last_commit = 0.0
        self.files = 0
        self.bytes = 0
        self.last_checkpoint: dict = {}
        self.stop_reason = ""
        self.deadline = 0.0
        self.parked = False
        self.last_24h = 0

    def commits_last_24h(self) -> int:
        # Once parked, the report reuses the count taken before the marker commit:
        # the park promises the hub no further requests at all, reads included.
        if self.parked:
            return self.last_24h
        day_ago = time.time() - 86400
        count = 0
        for commit in self.api.list_repo_commits(REPO, repo_type="dataset", revision=REVISION):
            stamp_value = getattr(commit, "created_at", None)
            if stamp_value is None:
                continue
            created = (stamp_value.timestamp() if hasattr(stamp_value, "timestamp")
                       else float(stamp_value))
            if created >= day_ago:
                count += 1
            else:
                break
        self.last_24h = count
        return count

    def allow(self) -> str:
        if self.used >= self.max_per_run:
            return f"max_commits_per_run reached ({self.max_per_run})"
        if self.last_commit:
            wait = self.min_interval - (time.time() - self.last_commit)
            if wait > 0:
                time.sleep(wait)
        self.headers = api_headers()
        self.state = rate_state(self.headers)
        remaining = self.state.get("remaining")
        # A low window is not a refusal: wait it out inside this run and keep pumping
        # toward the commit cap. Only a refused commit parks the pipeline until tomorrow.
        while remaining is not None and remaining < self.min_remaining:
            reset = self.state.get("reset_in")
            pause = (reset if reset is not None else (self.state.get("window") or 300)) + 5
            if self.deadline and time.time() + pause > self.deadline:
                return (f"rate window low and no time left to wait: remaining={remaining} "
                        f"pause={pause}s deadline_in={int(self.deadline - time.time())}s")
            log(f"rate window low (remaining={remaining}); waiting {pause}s inside this run")
            time.sleep(pause)
            self.headers = api_headers()
            self.state = rate_state(self.headers)
            remaining = self.state.get("remaining")
        return ""

    def allow_ignoring_pacing(self) -> str:
        self.headers = api_headers()
        self.state = rate_state(self.headers)
        remaining = self.state.get("remaining")
        if remaining is not None and remaining < self.min_remaining:
            return (f"rate window low: remaining={remaining} "
                    f"reset_in={self.state.get('reset_in')}s of q={self.state.get('quota')}")
        return ""

    def record(self, files: int, uploaded_bytes: int, checkpoint: dict) -> None:
        self.used += 1
        self.files += files
        self.bytes += uploaded_bytes
        self.last_commit = time.time()
        self.last_checkpoint = checkpoint

    def report(self) -> dict:
        return {
            "generated_at": stamp(),
            "repo": REPO,
            "revision": REVISION,
            "commits_this_run": self.used,
            "commits_last_24h": self.commits_last_24h(),
            "cap_commits_per_run": self.max_per_run,
            "min_commit_interval_seconds": self.min_interval,
            "files_uploaded_this_run": self.files,
            "bytes_uploaded_this_run": self.bytes,
            "bytes_uploaded_this_run_mb": round(self.bytes / 1048576, 1),
            "last_rate_limit_headers": self.headers,
            "rate_window": self.state,
            "last_verified_checkpoint": self.last_checkpoint,
            "stopped_because": self.stop_reason,
            "hub_window_observed": "RateLimit-Policy q=1000 w=300 (requests per 5 min); "
                                   "these caps are precautionary, not a claimed HF limit",
        }


def looks_like_quota(message: str) -> bool:
    """Only the hub's own refusal words may turn a stop into a wait."""
    lowered = message.lower()
    return any(marker in lowered for marker in
               ("429", "rate limit", "rate-limit", "retry-after", "too many requests", "quota"))


def backoff_until(api) -> int:
    raw = read_file(api, "_worker/backoff.json")
    if not raw:
        return 0
    try:
        return int(json.loads(raw).get("not_before_epoch") or 0)
    except (json.JSONDecodeError, ValueError):
        return 0


def prime_last_commit(api, budget: Budget) -> dict:
    """Carry the previous run's verified expectations forward. A marker or a state flush
    written before this run's first commit must not erase what --verify-last reads."""
    raw = read_file(api, "_worker/library_checkpoint.json")
    if not raw:
        return {}
    try:
        last = json.loads(raw).get("last_commit") or {}
        expected = {p: tuple(v) for p, v in (last.get("expected") or {}).items()}
    except (json.JSONDecodeError, AttributeError, TypeError, ValueError):
        return {}
    if expected:
        budget.last_checkpoint = {"commit": last.get("commit", "")}
    return expected


def save_backoff(api, budget: Budget) -> int:
    """A refused commit parks the pipeline until the next UTC midnight. Writing this
    marker is the one last request; nothing else is sent until the new day starts."""
    reset = budget.state.get("reset_in") or budget.state.get("retry_after") or 300
    now = int(time.time())
    next_utc_midnight = ((now // 86400) + 1) * 86400
    not_before = max(next_utc_midnight, now + reset + 60)
    payload = {
        "_worker/backoff.json": json.dumps({
            "since": stamp(),
            "not_before_epoch": not_before,
            "not_before_utc": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(not_before)),
            "wait_seconds": not_before - now,
            "reason": budget.stop_reason,
            "last_rate_limit_headers": budget.headers,
            "rate_window": budget.state,
        }, ensure_ascii=False, indent=1).encode("utf-8"),
        **state_files(STATE_HOOK["index"], STATE_HOOK["ledger"], budget, 0,
                      STATE_HOOK.get("expected") or {},
                      budget.last_checkpoint.get("commit", "")),
    }
    api.create_commit(REPO, [CommitOperationAdd(path_in_repo=k, path_or_fileobj=v)
                             for k, v in sorted(payload.items())],
                      commit_message="library: backoff marker (commit refused, parked until UTC midnight)",
                      repo_type="dataset", revision=REVISION)
    budget.parked = True
    return not_before


class Stopped(Exception):
    """The runner is about to end this job; the checkpoint is written before it does."""


def _on_signal(signum, frame):
    print(f"signal {signum}: flushing checkpoint before exit", file=sys.stderr, flush=True)
    if STATE_HOOK:
        try:
            publish_state(STATE_HOOK["api"], STATE_HOOK["index"], STATE_HOOK["ledger"],
                          STATE_HOOK["budget"])
        except Exception as error:
            print(f"checkpoint flush failed: {error}", file=sys.stderr, flush=True)
    raise SystemExit(128 + signum)


# ==============================================================
#   Text cleaning - vendored from Titan-Gateway api/library.py
# ==============================================================
_GUTENBERG_START = re.compile(
    r"^\*{3}\s*START OF (?:THE|THIS) PROJECT GUTENBERG EBOOK.*?\*{3}\s*$", re.I | re.M)
_GUTENBERG_END = re.compile(
    r"^\*{3}\s*END OF (?:THE|THIS) PROJECT GUTENBERG EBOOK.*?\*{3}\s*$", re.I | re.M)
_HEADING_NUMERAL = (r"(?:[0-9٠-٩]+|[ivxlcdm]+(?=[ \t.:—–)\-]|$)|"
                    r"(?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|"
                    r"thirteen|fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|"
                    r"thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred|first|second|"
                    r"third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|last)(?![a-z]))")
_HEADING_TAIL = r"(?=[ \t]*(?:$|" + _HEADING_NUMERAL + r"|[.:—–)]))"
_HEADING_NUMBERED = re.compile(r"[A-Za-zÀ-ÿ]+[ \t]+" + _HEADING_NUMERAL, re.I)
_CHAPTER_HEADING = re.compile(
    r"^\s*((?:(?:chapter|book|part|volume|letter|cap[ií]tulo|kapitel)" + _HEADING_TAIL +
    r"|розділ|частина|الفصل|الباب)\b[^\n]{0,100})\s*$", re.I | re.M)

def _spans(*pairs: tuple[int, int]) -> str:
    """Character-class body from codepoints, so no invisible byte sits in the source."""
    return "".join(chr(a) if a == b else chr(a) + "-" + chr(b) for a, b in pairs)


def _cls(*pairs: tuple[int, int]) -> str:
    return "[" + _spans(*pairs) + "]"


_AR_MARKS = _cls((0x064B, 0x065F), (0x0670, 0x0670), (0x06D6, 0x06ED))
_INVISIBLE_RE = re.compile(_cls((0x200B, 0x200F), (0x202A, 0x202E),
                                (0x2060, 0x2069), (0xFEFF, 0xFEFF)))
_TATWEEL_RE = re.compile(chr(0x0640))
_PRESENT_FORMS_RE = re.compile(_cls((0xFB50, 0xFDFF), (0xFE70, 0xFEFF)))
_TORN_MARK_RE = re.compile(r"\s+(" + _AR_MARKS + ")")
_ORPHAN_MARK_RE = re.compile(
    r"(^|[^" + _spans((0x0621, 0x064A), (0x0671, 0x06D3), (0x064B, 0x065F),
                      (0x0670, 0x0670), (0x06D6, 0x06ED)) + "])" + _AR_MARKS + "+")
_SPACE_RUN_RE = re.compile("[ \t]{2,}")


def repair_text(text: str) -> str:
    text = _INVISIBLE_RE.sub("", text)
    text = _TATWEEL_RE.sub("", text)
    text = _PRESENT_FORMS_RE.sub(lambda m: unicodedata.normalize("NFKC", m.group(0)), text)
    text = _TORN_MARK_RE.sub(r"\1", text)
    text = _ORPHAN_MARK_RE.sub(r"\1", text)
    text = unicodedata.normalize("NFC", text)
    return _SPACE_RUN_RE.sub(" ", text)


def split_gutenberg(text: str) -> tuple[str, str]:
    """(the Project Gutenberg notice, the book itself). The notice is the license that
    §1.E.4 asks to travel with the copy, so it is stored as LICENSE.txt, not dropped."""
    text = text.replace("\r\n", "\n").replace("\r", "\n").lstrip(chr(0xFEFF))
    start = _GUTENBERG_START.search(text)
    head = text[:start.start()].strip() if start else ""
    body = text[start.end():] if start else text
    end = _GUTENBERG_END.search(body)
    tail = body[end.end():].strip() if end else ""
    if end:
        body = body[:end.start()]
    notice = "\n\n".join(part for part in (head, tail) if part)
    return notice, body.strip()


def split_long_text(text: str, target: int = CHAPTER_TARGET) -> list[str]:
    if len(text) <= target:
        return [text]
    parts: list[str] = []
    current = ""
    for paragraph in re.split(r"(\n\s*\n)", text):
        if current and len(current) + len(paragraph) > target:
            parts.append(current.strip())
            current = paragraph
        else:
            current += paragraph
        while len(current) > target * 2:
            cut = current.rfind("\n", 0, target)
            if cut < target // 2:
                cut = current.rfind(" ", 0, target)
            if cut < target // 2:
                cut = target
            parts.append(current[:cut].strip())
            current = current[cut:].lstrip()
    if current.strip():
        parts.append(current.strip())
    parts = [p for p in parts if p]
    if len(parts) > 1 and len(parts[-1]) < MIN_CHAPTER_CHARS:
        tail = parts.pop()
        parts[-1] = f"{parts[-1]}\n\n{tail}"
    return parts


def heading_matches(text: str) -> list:
    """Heading candidates minus the ones a wrapped prose sentence produced."""
    kept = []
    for match in _CHAPTER_HEADING.finditer(text):
        title = match.group(1)
        if title[:1].islower() and not _HEADING_NUMBERED.match(title):
            continue
        kept.append(match)
    return kept


def heading_sections(text: str) -> list[tuple[str, int, int]]:
    """Contiguous spans over the ORIGINAL text, so nothing is rewritten or dropped.
    A contents listing (a run of >= TOC_RUN headings with almost no text between them)
    is not structure: this is what produced 430 empty chapters in Les Miserables."""
    matches = heading_matches(text)
    if not matches:
        return [("", 0, len(text))]
    gaps = []
    for i, match in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        gaps.append(len(text[match.end():end].strip()))
    toc = [(i > 0 and gaps[i - 1] < TOC_GAP_CHARS) or gaps[i] < TOC_GAP_CHARS
           for i in range(len(matches))]
    kept = []
    i = 0
    while i < len(matches):
        if not toc[i]:
            kept.append(matches[i])
            i += 1
            continue
        j = i
        while j < len(matches) and toc[j]:
            j += 1
        if j - i < TOC_RUN:
            kept.extend(matches[i:j])
        i = j
    if not kept:
        return [("", 0, len(text))]
    spans: list[tuple[str, int, int]] = []
    if text[:kept[0].start()].strip():
        spans.append(("", 0, kept[0].start()))
    for i, match in enumerate(kept):
        end = kept[i + 1].start() if i + 1 < len(kept) else len(text)
        spans.append((" ".join(match.group(1).split())[:120], match.start(), end))
    return spans


def merge_short_spans(text: str, spans: list[tuple[str, int, int]]) -> list[tuple[str, int, int]]:
    merged: list[list] = []
    for title, start, end in spans:
        if merged and len(text[start:end].strip()) < MIN_CHAPTER_CHARS:
            merged[-1][2] = end
            continue
        merged.append([title, start, end])
    if len(merged) > 1 and len(text[merged[0][1]:merged[0][2]].strip()) < MIN_CHAPTER_CHARS:
        merged[1][1] = merged[0][1]
        merged[1][0] = merged[1][0] or merged[0][0]
        merged.pop(0)
    return [(title, start, end) for title, start, end in merged]


def chapterize(text: str) -> list[dict]:
    chapters: list[dict] = []
    for title, start, end in merge_short_spans(text, heading_sections(text)):
        parts = [p for p in split_long_text(text[start:end].strip()) if p.strip()]
        for part_index, chunk in enumerate(parts, start=1):
            number = len(chapters) + 1
            chapters.append({
                "chapter_number": number,
                "title": (title if len(parts) == 1 else
                          (f"{title} — {part_index}" if title else f"Part {part_index}"))
                         or f"Part {number}",
                "word_count": len(chunk.split()),
                "_text": chunk,
            })
    return chapters


# ==============================================================
#   Polite fetching - every byte of it on the runner
# ==============================================================
def is_gutenberg_url(url: str) -> bool:
    try:
        parsed = urllib.parse.urlparse(url)
        return parsed.scheme == "https" and parsed.hostname in GUTENBERG_HOSTS
    except ValueError:
        return False


_LAST_FETCH = 0.0


def polite_get(url: str, *, limit: int, interval: float, kind: str) -> dict:
    global _LAST_FETCH
    if not is_gutenberg_url(url):
        return {"status": "rejected", "error": "host outside Project Gutenberg"}
    last = "no attempt"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        delay = min(2 ** attempt, 60)
        wait = interval - (time.monotonic() - _LAST_FETCH)
        if wait > 0:
            time.sleep(wait)
        _LAST_FETCH = time.monotonic()
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA),
                                        timeout=120) as response:
                final_url = response.geturl()
                declared = int(response.headers.get("Content-Length") or 0)
                data = response.read(limit + 1)
            if not is_gutenberg_url(final_url):
                return {"status": "rejected", "error": "redirect outside Project Gutenberg"}
            if len(data) > limit:
                return {"status": "too_large", "error": f"over {limit} bytes"}
            if not data:
                last = "empty body"
            elif declared and declared != len(data):
                last = f"truncated {len(data)} of {declared}"
            else:
                return {"status": "ok", "data": data, "url": final_url, "bytes": len(data),
                        "sha256": hashlib.sha256(data).hexdigest()}
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return {"status": "missing", "error": "404"}
            last = f"HTTP {error.code}"
            if error.code not in (429, 500, 502, 503, 504):
                return {"status": "failed", "error": last}
            retry = (error.headers or {}).get("Retry-After") or ""
            if retry.isdigit():
                delay = min(float(retry), 300)
        except (urllib.error.URLError, OSError, TimeoutError) as error:
            last = f"{type(error).__name__}: {error}"
        log(f"{kind} {url} attempt {attempt}/{MAX_ATTEMPTS}: {last} - sleep {delay:.0f}s")
        time.sleep(delay)
    return {"status": "failed", "error": last}


def catalog_row_ok(row: dict) -> bool:
    return ((row.get("Type") or "").strip().lower() == "text"
            and (row.get("Language") or "").strip().lower() == "en")


def split_list(value: str | None) -> list[str]:
    return [" ".join(part.split()) for part in (value or "").split(";") if part.strip()]


def load_catalog(interval: float) -> tuple[list[dict], dict]:
    result = polite_get(CATALOG_URL, limit=MAX_CATALOG_BYTES, interval=interval, kind="catalog")
    if result["status"] != "ok":
        raise SystemExit(f"catalog unavailable: {result}")
    raw = gzip.decompress(result["data"])
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
    columns = reader.fieldnames or []
    missing = {"Text#", "Type", "Language", "Title", "Authors"} - set(columns)
    if missing:
        raise SystemExit(f"catalog columns changed; missing {sorted(missing)} in {columns}")
    rows: list[dict] = []
    stats: Counter = Counter()
    for row in reader:
        stats["rows"] += 1
        if not catalog_row_ok(row):
            stats["filtered_out"] += 1
            continue
        try:
            gid = int((row.get("Text#") or "").strip())
        except ValueError:
            stats["bad_id"] += 1
            continue
        if gid <= 0:
            stats["bad_id"] += 1
            continue
        stats["english_text"] += 1
        rows.append({"id": gid, "title": " ".join((row.get("Title") or "").split()),
                     "author": " ".join((row.get("Authors") or "").split()),
                     "subjects": split_list(row.get("Subjects")),
                     "bookshelves": split_list(row.get("Bookshelves")),
                     "issued": (row.get("Issued") or "").strip()})
    rows.sort(key=lambda r: r["id"])
    stats["unique_ids"] = len({r["id"] for r in rows})
    return rows, {"columns": columns, **stats}


# ==============================================================
#   Paths, state and the structural guard
# ==============================================================
def book_folder(gid: int) -> str:
    return f"{LEGACY_ROOT}/{gid // 1000}/{gid}/en"


def misplaced(name: str) -> bool:
    """The hub caps a directory at 10000 entries. Books are bucketed by id//1000,
    state lives one level down, and only two index files may sit at the root."""
    parts = PurePosixPath(name).parts
    if parts[0] == "books":
        return (len(parts) not in (5, 6) or parts[1] != "gutendex"
                or parts[-2] != "en" or not parts[-1])
    if name in ROOT_FILES:
        return len(parts) != 1
    return parts[0] not in ("_worker", "reports") or len(parts) != 2


def dir_capacity(api, folders: list[str]) -> None:
    """Measure the two levels that can actually accumulate, before committing into them."""
    parents = {LEGACY_ROOT} | {str(PurePosixPath(f).parent.parent) for f in folders}
    for parent in sorted(parents):
        try:
            existing = {PurePosixPath(e.path).name for e in
                        api.list_repo_tree(REPO, path_in_repo=parent, repo_type="dataset",
                                           revision=REVISION)}
        except (EntryNotFoundError, HfHubHTTPError):
            existing = set()
        children = {PurePosixPath(f).parent.name for f in folders
                    if str(PurePosixPath(f).parent.parent) == parent}
        new = len(children - existing)
        if len(existing) + new > DIR_ENTRY_LIMIT:
            raise SystemExit(f"{parent} holds {len(existing)} entries and this commit adds "
                             f"{new}; the hub refuses a directory past {DIR_ENTRY_LIMIT}")


def load_state(api) -> dict:
    raw = read_file(api, "_worker/library_index.json")
    return json.loads(raw) if raw else {"books": {}}


def load_ledger(api) -> dict:
    raw = read_file(api, "ledger.json")
    return json.loads(raw) if raw else {"version": 1, "queue": []}


def ledger_ids(ledger: dict) -> set[int]:
    ids = set()
    for entry in ledger.get("queue", []):
        if entry.get("source") == "gutendex" and entry.get("language") == "en":
            try:
                ids.add(int(entry["external_id"]))
            except (KeyError, TypeError, ValueError):
                continue
    return ids


def existing_books(api, ids: list[int]) -> set[int]:
    """Probe BOTH shapes for every candidate, so a legacy book is never re-ingested
    into a bucket and a bucketed book is never mistaken for a legacy one."""
    paths: list[str] = []
    for gid in ids:
        paths.append(f"{LEGACY_ROOT}/{gid}/en/book.json")
        paths.append(f"{book_folder(gid)}/book.json")
    found: set[int] = set()
    for start in range(0, len(paths), 500):
        if start:
            time.sleep(0.5)
        for entry in api.get_paths_info(REPO, paths[start:start + 500], repo_type="dataset",
                                        revision=REVISION):
            parts = PurePosixPath(entry.path).parts
            if len(parts) >= 3 and parts[-3].isdigit():
                found.add(int(parts[-3]))
    return found


def backfill_legacy(api, index: dict, ledger: dict) -> int:
    """Index the books the gateway already stored, reading them from the dataset and
    never from Gutenberg, so catalog.parquet is complete and nothing is re-fetched."""
    added = 0
    for entry in ledger.get("queue", []):
        if entry.get("source") != "gutendex" or entry.get("language") != "en":
            continue
        gid = entry.get("external_id")
        if not isinstance(gid, int) or str(gid) in index["books"]:
            continue
        folder = f"{LEGACY_ROOT}/{gid}/en"
        data = cdn_bytes(f"{folder}/book.json")
        if data is None:
            continue
        try:
            book = json.loads(data)
        except json.JSONDecodeError:
            continue
        chapters = book.get("chapters") or []
        index["books"][str(gid)] = {
            "status": "verified_remote", "origin": "legacy_gateway", "folder": folder,
            "title": book.get("title") or entry.get("title") or "",
            "author": book.get("author") or "",
            "subjects": book.get("subjects") or [],
            "bookshelves": book.get("bookshelves") or [],
            "chapters": len(chapters),
            "words": sum(int(c.get("word_count") or 0) for c in chapters),
            "chars": int(book.get("char_count") or 0),
            "cover": bool(book.get("cover")), "cover_url": book.get("cover") or "",
            "audio_available": bool(book.get("audio_available")),
            "attempts": 1, "verified_at": stamp(),
        }
        added += 1
    if added:
        log(f"indexed {added} legacy gateway books from the dataset")
    return added


def book_row(item: dict, status: str) -> dict:
    return {"status": status, "origin": "catalog", "folder": item["folder"],
            "title": item["title"], "author": item["author"],
            "subjects": item["subjects"], "bookshelves": item["bookshelves"],
            "chapters": item["chapters"], "words": item["words"], "chars": item["chars"],
            "cover": item["cover"], "cover_url": item["cover_url"],
            "audio_available": False,
            "text_sha256": item["text_sha256"], "text_url": item["text_url"],
            "text_bytes": item["text_bytes"], "asset_bytes": item["asset_bytes"],
            "attempts": 1, "attempted_at": stamp()}


def build_book(row: dict, *, interval: float) -> dict:
    gid = row["id"]
    folder = book_folder(gid)
    key = f"gutendex-{gid}-en"
    text = None
    for name in (f"pg{gid}.txt.utf8", f"pg{gid}.txt"):
        got = polite_get(f"https://www.gutenberg.org/cache/epub/{gid}/{name}",
                         limit=MAX_TEXT_BYTES, interval=interval, kind="text")
        if got["status"] == "ok":
            text = got
            break
        if got["status"] != "missing":
            return {"id": gid, "status": "failed", "error": got["error"]}
    if text is None:
        return {"id": gid, "status": "missing", "error": "no plain-text edition"}
    try:
        raw = text["data"].decode("utf-8-sig")
    except UnicodeDecodeError:
        raw = text["data"].decode("latin-1")
    notice, body = split_gutenberg(raw)
    body = repair_text(body)
    if not body.strip():
        return {"id": gid, "status": "failed", "error": "empty after boilerplate strip"}
    if len(body) > MAX_TEXT_CHARS:
        return {"id": gid, "status": "too_large", "error": f"{len(body)} chars"}
    chapters = chapterize(body)
    if not chapters:
        return {"id": gid, "status": "failed", "error": "chapter split produced nothing"}
    if len(chapters) > CHAPTER_SANITY_CAP:
        return {"id": gid, "status": "failed", "error": f"{len(chapters)} chapters past cap"}

    cover = polite_get(f"https://www.gutenberg.org/cache/epub/{gid}/pg{gid}.cover.medium.jpg",
                       limit=MAX_COVER_BYTES, interval=interval, kind="cover")
    cover_data = (cover["data"] if cover["status"] == "ok"
                  and cover["data"][:3] == b"\xff\xd8\xff" else None)

    files: dict[str, bytes] = {}
    stored: list[dict] = []
    chars = words = text_bytes = 0
    for chapter in chapters:
        number = chapter["chapter_number"]
        payload = chapter["_text"]
        data = payload.encode("utf-8")
        name = f"ch{number:03d}_en.txt"
        files[f"{folder}/{name}"] = data
        chars += len(payload)
        words += chapter["word_count"]
        text_bytes += len(data)
        stored.append({
            "chapter_number": number, "title": chapter["title"],
            "text_url": RAW.format(repo=REPO, path=f"{folder}/{name}"),
            "char_count": len(payload), "word_count": chapter["word_count"],
            "text_bytes": len(data),
            "audio_urls": {}, "karaoke_urls": {}, "audio_ready": False,
            "narrator_voice": "", "narrator_label": "",
            "dialogue_voice": "", "dialogue_label": "",
            "text_direction": "ltr", "audio_seconds": 0.0,
        })
    if cover_data:
        files[f"{folder}/cover.jpg"] = cover_data
    if notice:
        files[f"{folder}/LICENSE.txt"] = notice.encode("utf-8")
    cover_url = RAW.format(repo=REPO, path=f"{folder}/cover.jpg") if cover_data else ""
    licence_url = (RAW.format(repo=REPO, path=f"{folder}/LICENSE.txt") if notice
                   else GUTENBERG_LICENSE_URL)
    now = stamp()
    book = {
        "id": key, "key": key,
        "title": row["title"], "title_ar": row["title"], "title_en": row["title"],
        "author": row["author"],
        "language": "en", "text_direction": "ltr",
        "voice": "", "cast": [], "rate": "+0%",
        "owner_uid": "library",
        "created_at": now, "updated_at": now,
        "status": "complete",
        "source": "gutendex", "external_id": gid,
        "source_url": f"https://www.gutenberg.org/ebooks/{gid}",
        "provider": "Project Gutenberg",
        "cover": cover_url,
        "subjects": row["subjects"], "bookshelves": row["bookshelves"],
        "download_count": 0,
        "char_count": chars, "word_count": words,
        "audio_available": False, "published": True,
        "rights": {
            "basis": "public_domain", "jurisdiction": "US", "provider": "Project Gutenberg",
            "verified": True, "claim": "public_domain", "publishable": True,
            "statement_version": "1",
            "note": "Listed in Project Gutenberg's own catalog of US public-domain editions; "
                    "the verbatim notice and license travel beside the text as LICENSE.txt.",
            "license": "Project Gutenberg License", "license_url": licence_url,
            "redistribution": GUTENBERG_REDISTRIBUTION,
        },
        "chapters": stored,
    }
    files[f"{folder}/book.json"] = json.dumps(book, ensure_ascii=False).encode("utf-8")
    return {"id": gid, "status": "built", "files": files, "folder": folder,
            "chapters": len(stored), "chars": chars, "words": words,
            "cover": bool(cover_data), "cover_url": cover_url,
            "title": row["title"], "author": row["author"],
            "subjects": row["subjects"], "bookshelves": row["bookshelves"],
            "text_bytes": text_bytes,
            "asset_bytes": sum(len(d) for d in files.values()),
            "text_sha256": text["sha256"], "text_url": text["url"],
            "chapter_titles": [c["title"] for c in stored]}


def merge_ledger(ledger: dict, group: list[dict]) -> None:
    queue = ledger.setdefault("queue", [])
    positions = {entry.get("key"): i for i, entry in enumerate(queue)}
    rank = max((int(entry.get("rank") or 0) for entry in queue), default=0)
    for item in group:
        gid = item["id"]
        key = f"gutendex-{gid}-en"
        rank += 1
        entry = {
            "key": key, "source": "gutendex", "external_id": gid, "language": "en",
            "title": item["title"], "author": item["author"], "cover": item["cover_url"],
            "source_url": f"https://www.gutenberg.org/ebooks/{gid}",
            "download_count": 0, "stage": "ready", "status": "complete",
            "origin": "catalog", "rank": rank, "added_at": stamp(),
            "last_work_at": time.time(),
            "chapters": item["chapters"],
            # Narration is retired: for a catalog book this counts chapters a reader can open.
            "chapters_ready": item["chapters"], "chapters_failed": 0,
            "chars": item["chars"], "word_count": item["words"],
            "audio_seconds": 0.0, "estimated_audio_seconds": 0.0,
            "failures": {}, "turn": 0,
            "text_bytes": item["text_bytes"], "asset_bytes": item["asset_bytes"],
        }
        if key in positions:
            queue[positions[key]] = {**queue[positions[key]], **entry}
        else:
            queue.append(entry)
            positions[key] = len(queue) - 1
    ledger["updated_at"] = stamp()


CATALOG_SCHEMA = pa.schema([
    ("gutenberg_id", pa.int32()), ("key", pa.string()), ("title", pa.string()),
    ("author", pa.string()), ("language", pa.string()),
    ("subjects", pa.list_(pa.string())), ("bookshelves", pa.list_(pa.string())),
    ("chapter_count", pa.int32()), ("word_count", pa.int32()), ("char_count", pa.int32()),
    ("cover_path", pa.string()), ("book_path", pa.string()),
    ("audio_available", pa.bool_()), ("status", pa.string()), ("added_at", pa.string()),
])


def catalog_parquet(index: dict, ledger: dict) -> bytes:
    rows = []
    for entry in ledger.get("queue", []):
        if entry.get("source") != "gutendex":
            continue
        gid = entry.get("external_id")
        meta = index["books"].get(str(gid), {})
        rows.append({
            "gutenberg_id": int(gid or 0), "key": entry.get("key") or "",
            "title": entry.get("title") or "", "author": entry.get("author") or "",
            "language": entry.get("language") or "en",
            "subjects": meta.get("subjects") or [],
            "bookshelves": meta.get("bookshelves") or [],
            "chapter_count": int(entry.get("chapters") or 0),
            "word_count": int(meta.get("words") or entry.get("word_count") or 0),
            "char_count": int(entry.get("chars") or 0),
            "cover_path": meta.get("cover_url") or entry.get("cover") or "",
            "book_path": meta.get("folder") or "",
            "audio_available": bool(meta.get("audio_available", False)),
            "status": entry.get("status") or "", "added_at": entry.get("added_at") or "",
        })
    rows.sort(key=lambda r: r["gutenberg_id"])
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=CATALOG_SCHEMA), buffer, compression="zstd")
    return buffer.getvalue()


def state_files(index: dict, ledger: dict, budget: Budget, new_books: int,
                expected: dict, commit_url: str) -> dict[str, bytes]:
    checkpoint = {
        "saved_at": stamp(),
        "books_indexed": len(index["books"]),
        "by_status": dict(Counter(r.get("status", "?") for r in index["books"].values())),
        "new_books_in_this_commit": new_books,
        "last_commit": {"commit": commit_url, "expected": expected},
    }
    return {
        "_worker/library_index.json": json.dumps(index, ensure_ascii=False).encode("utf-8"),
        "_worker/library_checkpoint.json": json.dumps(checkpoint, ensure_ascii=False,
                                                      indent=1).encode("utf-8"),
        "reports/commit_budget.json": json.dumps(budget.report(), ensure_ascii=False,
                                                 indent=1).encode("utf-8"),
        "ledger.json": json.dumps(ledger, ensure_ascii=False).encode("utf-8"),
        "catalog.parquet": catalog_parquet(index, ledger),
    }


def publish_state(api, index: dict, ledger: dict, budget: Budget) -> None:
    payload = state_files(index, ledger, budget, 0, STATE_HOOK.get("expected") or {},
                          budget.last_checkpoint.get("commit", ""))
    api.create_commit(REPO, [CommitOperationAdd(path_in_repo=k, path_or_fileobj=v)
                             for k, v in sorted(payload.items())],
                      commit_message="library: resume state flush",
                      repo_type="dataset", revision=REVISION)


# ==============================================================
#   Remote verification - the repository is the only truth
# ==============================================================
def git_blob_sha1(data: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


def expectations(files: dict[str, bytes]) -> dict[str, tuple[str, str, int]]:
    """.txt/.json are plain git blobs here; .jpg/.parquet are LFS (see .gitattributes)."""
    out = {}
    for path, data in files.items():
        if path.lower().endswith(LFS_SUFFIXES):
            out[path] = ("lfs", hashlib.sha256(data).hexdigest(), len(data))
        else:
            out[path] = ("blob", git_blob_sha1(data), len(data))
    return out


def remote_check(api, expected: dict[str, tuple[str, str, int]]) -> tuple[int, list[str]]:
    bad: list[str] = []
    refetch: list[str] = []
    paths = sorted(expected)
    for start in range(0, len(paths), 500):
        if start:
            time.sleep(0.5)
        chunk = paths[start:start + 500]
        seen = {entry.path: entry for entry in
                api.get_paths_info(REPO, chunk, expand=True, repo_type="dataset",
                                   revision=REVISION)}
        for path in chunk:
            mode, digest, size = expected[path]
            entry = seen.get(path)
            if entry is None:
                bad.append(f"{path}: missing")
                continue
            if mode == "lfs":
                lfs = getattr(entry, "lfs", None)
                actual = (getattr(lfs, "sha256", "") or "").lower()
                actual_size = int(getattr(lfs, "size", 0) or 0)
                if not actual:
                    refetch.append(path)   # Xet-backed: no LFS oid, compare the bytes
                    continue
            else:
                actual = (getattr(entry, "blob_id", "") or "").lower()
                actual_size = int(getattr(entry, "size", 0) or 0)
            if actual != digest or (actual_size and actual_size != size):
                bad.append(f"{path}: remote {actual or 'none'}/{actual_size} != {digest}/{size}")
    if refetch:
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda p: (p, cdn_bytes(p)), refetch))
        for path, data in results:
            _mode, digest, _size = expected[path]
            if data is None or hashlib.sha256(data).hexdigest() != digest:
                bad.append(f"{path}: CDN bytes differ or are unreadable")
    return len(paths) - len(bad), bad


def verify_last(api) -> dict:
    raw = read_file(api, "_worker/library_checkpoint.json", required=True)
    checkpoint = json.loads(raw)
    last = checkpoint.get("last_commit") or {}
    expected = {p: tuple(v) for p, v in (last.get("expected") or {}).items()}
    if not expected:
        return {"refused": "checkpoint carries no last_commit expectations", "mismatches": 0}
    verified, bad = remote_check(api, expected)
    return {"checked": len(expected), "verified_remote": verified, "mismatches": len(bad),
            "mismatch_sample": bad[:20], "commit": last.get("commit", ""),
            "books_indexed": checkpoint.get("books_indexed"),
            "by_status": checkpoint.get("by_status")}


# ==============================================================
#   The run
# ==============================================================
def run(args) -> dict:
    api = hf_api()
    until = backoff_until(api)
    if until and time.time() < until:
        return {"waiting_for_backoff": True, "seconds_left": int(until - time.time()),
                "not_before_epoch": until,
                "reason": "a previous run was rate limited; this run exits without touching the hub"}
    budget = Budget(api, args.max_commits_per_run, args.min_commit_interval, args.min_remaining)
    if args.reserve_minutes >= args.max_minutes:
        return {"refused": f"max_minutes {args.max_minutes} must exceed reserve_minutes "
                           f"{args.reserve_minutes}, otherwise no batch can start",
                "batches": [], "commit_budget": budget.report()}
    carried = prime_last_commit(api, budget)
    index = load_state(api)
    ledger = load_ledger(api)
    legacy = backfill_legacy(api, index, ledger)
    catalog, stats = load_catalog(args.fetch_interval)
    known = {int(k) for k, v in index["books"].items() if v.get("status") == "verified_remote"}
    known |= ledger_ids(ledger)
    candidates = [row for row in catalog if row["id"] not in known]
    scan = candidates[:args.scan or args.batch_size * 40]
    present = existing_books(api, [row["id"] for row in scan])
    queue = [row for row in scan if row["id"] not in present]
    log(f"catalog {stats} | candidates {len(candidates)} | scanned {len(scan)} | "
        f"already present {len(present)} | queue {len(queue)}")

    if args.dry_run:
        samples = []
        for row in queue[:3]:
            item = build_book(row, interval=args.fetch_interval)
            samples.append({"id": row["id"], "title": row["title"], "status": item["status"],
                            "chapters": item.get("chapters"), "words": item.get("words"),
                            "chars": item.get("chars"), "cover": item.get("cover"),
                            "files": len(item.get("files") or {}),
                            "folder": item.get("folder"),
                            "first_titles": (item.get("chapter_titles") or [])[:5],
                            "error": item.get("error")})
        return {"dry_run": True, "catalog": stats, "legacy_backfilled": legacy,
                "books_indexed": len(index["books"]), "candidates": len(candidates),
                "scanned": len(scan), "already_present": sorted(present),
                "queue_head": [{"id": r["id"], "title": r["title"]} for r in queue[:10]],
                "samples": samples, "commit_budget": budget.report()}

    STATE_HOOK.clear()
    STATE_HOOK.update({"api": api, "index": index, "ledger": ledger, "budget": budget,
                       "expected": carried})
    started = time.time()
    budget.deadline = started + (args.max_minutes - args.reserve_minutes) * 60
    history: list[dict] = []
    cursor = 0
    carry: dict | None = None
    dirty = bool(legacy)
    refused = ""
    parked_until = 0
    last_batch_seconds = 0.0
    for batch_number in range(1, args.batches + 1):
        if ((time.time() - started) + last_batch_seconds + args.reserve_minutes * 60
                > args.max_minutes * 60):
            history.append({"batch": batch_number, "skipped": "safety margin before the deadline",
                            "elapsed_minutes": round((time.time() - started) / 60, 1),
                            "last_batch_minutes": round(last_batch_seconds / 60, 1)})
            break
        batch_start = time.time()
        built: list[dict] = []
        files: dict[str, bytes] = {}
        failures: list[dict] = []
        while len(built) < args.batch_size and (carry is not None or cursor < len(queue)):
            if carry is not None:
                item, carry = carry, None
            else:
                row = queue[cursor]
                cursor += 1
                item = build_book(row, interval=args.fetch_interval)
            if item["status"] != "built":
                gid = str(item["id"])
                previous = index["books"].get(gid, {})
                attempts = int(previous.get("attempts", 0)) + 1
                index["books"][gid] = {
                    "status": "failed" if attempts >= MAX_ATTEMPTS else item["status"],
                    "error": str(item.get("error", ""))[:200], "attempts": attempts,
                    "attempted_at": stamp(), "title": previous.get("title", ""),
                }
                failures.append({"id": item["id"], "status": item["status"],
                                 "error": item.get("error")})
                dirty = True
                continue
            if files and len(files) + len(item["files"]) > args.commit_files:
                carry = item          # already downloaded; the next commit takes it
                break
            built.append(item)
            files.update(item["files"])
        if not built:
            history.append({"batch": batch_number, "drained": cursor >= len(queue) and carry is None,
                            "books_fetched": 0, "failures": failures[:10]})
            break

        block = budget.allow()
        if block:
            budget.stop_reason = block
            history.append({"batch": batch_number, "books_fetched": len(built),
                            "commit_blocked": block})
            dirty = False
            break

        saved_books = {str(i["id"]): index["books"].get(str(i["id"])) for i in built}
        saved_queue = json.loads(json.dumps(ledger.get("queue", [])))
        saved_updated = ledger.get("updated_at")
        for item in built:
            index["books"][str(item["id"])] = book_row(item, "uploaded_unverified")
        merge_ledger(ledger, built)
        expected = expectations({**files,
                                 "ledger.json": json.dumps(ledger, ensure_ascii=False).encode("utf-8")})
        payload = {**files, **state_files(index, ledger, budget, len(built), expected,
                                          budget.last_checkpoint.get("commit", ""))}
        expected["catalog.parquet"] = expectations(
            {"catalog.parquet": payload["catalog.parquet"]})["catalog.parquet"]
        offenders = sorted(name for name in payload if misplaced(name))
        if offenders:
            raise RuntimeError(f"staged paths violate the sharding rule; offenders: {offenders[:5]}")
        dir_capacity(api, [item["folder"] for item in built])
        try:
            commit = api.create_commit(
                REPO,
                [CommitOperationAdd(path_in_repo=name, path_or_fileobj=data)
                 for name, data in sorted(payload.items())],
                commit_message=(f"library: {len(built)} books, {len(files)} text/cover files "
                                f"+ ledger + catalog + resume state (batch {batch_number})"),
                repo_type="dataset", revision=REVISION)
        except Exception as error:
            for gid, previous in saved_books.items():
                if previous is None:
                    index["books"].pop(gid, None)
                else:
                    index["books"][gid] = previous
            ledger["queue"] = saved_queue
            ledger["updated_at"] = saved_updated
            waited = looks_like_quota(str(error))
            refused = f"commit refused by the hub ({'quota' if waited else type(error).__name__})"
            budget.stop_reason = f"{refused}: {str(error)[:300]}"
            history.append({"batch": batch_number, "refused": refused, "error": str(error)[:500],
                            "rate_window": budget.state, "rolled_back_books": len(built)})
            if waited:
                try:
                    parked_until = save_backoff(api, budget)
                    history[-1]["parked_until_epoch"] = parked_until
                    history[-1]["parked_until_utc"] = time.strftime(
                        "%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(parked_until))
                except Exception as marker_error:
                    history[-1]["marker_write_failed"] = str(marker_error)[:300]
                    budget.stop_reason += f" | marker_write_failed: {str(marker_error)[:200]}"
                dirty = False
            log(budget.stop_reason)
            break

        commit_url = getattr(commit, "commit_url", "") or str(commit)[:80]
        budget.record(len(payload), sum(len(d) for d in files.values()),
                      {"commit": commit_url, "books_indexed": len(index["books"]),
                       "queue_remaining": len(queue) - cursor})
        verified, bad = remote_check(api, expected)
        for item in built:
            row = index["books"][str(item["id"])]
            problems = [b for b in bad if b.startswith(item["folder"] + "/")]
            if problems:
                row["status"] = "failed"
                row["error"] = "; ".join(problems)[:300]
                row["attempts"] = MAX_ATTEMPTS
            else:
                row["status"] = "verified_remote"
                row["verified_at"] = stamp()
        STATE_HOOK["expected"] = expected
        dirty = True
        last_batch_seconds = time.time() - batch_start
        history.append({"batch": batch_number, "books_committed": len(built),
                        "books_failed": len(failures), "failures": failures[:10],
                        "files_committed": len(payload), "verified_remote": verified,
                        "mismatches": len(bad), "mismatch_sample": bad[:5],
                        "commit": commit_url, "commits_used": budget.used,
                        "commits_last_24h": budget.commits_last_24h(),
                        "rate_remaining": budget.state.get("remaining"),
                        "queue_remaining": len(queue) - cursor,
                        "minutes": round(last_batch_seconds / 60, 1)})
        log(f"batch {batch_number}: {len(built)} books, {verified} files verified, "
            f"{len(bad)} mismatches, {len(queue) - cursor} left in queue")
        if bad:
            refused = f"{len(bad)} committed files did not verify remotely"
            budget.stop_reason = refused
            break
        if cursor >= len(queue) and carry is None:
            history.append({"batch": batch_number, "drained": True})
            break

    if not budget.stop_reason:
        budget.stop_reason = budget.allow_ignoring_pacing() or "queue or batch budget exhausted"
    if dirty and budget.used < budget.max_per_run and not budget.allow_ignoring_pacing():
        try:
            publish_state(api, index, ledger, budget)
        except Exception as error:
            log(f"final state publish failed: {error}")
    return {"repo": REPO, "revision": REVISION, "catalog": stats,
            "legacy_backfilled": legacy, "books_indexed": len(index["books"]),
            "by_status": dict(Counter(r.get("status", "?") for r in index["books"].values())),
            "candidates": len(candidates), "queue_scanned": len(queue),
            "batches": history, "refused": refused, "parked_until_epoch": parked_until,
            "commit_budget": budget.report(),
            "minutes": round((time.time() - started) / 60, 1)}


# ==============================================================
#   Self-test - a positive control that needs no network
# ==============================================================
FRONT_MATTER = (
    "TEST BOOK\r\n\r\nby A. Author\r\n\r\nPREFACE\r\n\r\n"
    "This preface stands between the contents listing and the first real chapter, "
    "as it does in a printed Gutenberg edition. It is deliberately longer than the "
    "TOC gap threshold so the contents run and the first chapter heading are "
    "distinguishable to the gap heuristic.\r\n\r\n"
)

FIXTURE = (
    "The Project Gutenberg eBook of Test Book\r\n\r\n"
    "This eBook is for the use of anyone anywhere in the United States and most\r\n"
    "other parts of the world at no cost and with almost no restrictions\r\n"
    "whatsoever. You may copy it, give it away or re-use it under the terms of\r\n"
    "the Project Gutenberg License included with this eBook or online at\r\n"
    "www.gutenberg.org. If you are not located in the United States, you will\r\n"
    "have to check the laws of the country where you are located before using\r\n"
    "this eBook.\r\n\r\n"
    "*** START OF THE PROJECT GUTENBERG EBOOK TEST BOOK ***\r\n\r\n"
    "CHAPTER I\r\nCHAPTER II\r\nCHAPTER III\r\nCHAPTER IV\r\n\r\n"
    + FRONT_MATTER +
    "CHAPTER I\r\n\r\n" + ("First chapter prose sentence. " * 120) + "\r\n\r\n"
    "CHAPTER II\r\n\r\n" + ("Second chapter prose sentence. " * 120) + "\r\n\r\n"
    "CHAPTER III\r\n\r\n" + ("Third chapter prose sentence. " * 120) + "\r\n\r\n"
    "*** END OF THE PROJECT GUTENBERG EBOOK TEST BOOK ***\r\n\r\n"
    "Updated editions will replace the previous one--the old editions will be\r\n"
    "renamed. Creating the works from print editions not protected by U.S.\r\n"
    "copyright law means that no one owns a United States copyright in these\r\n"
    "works, so the Foundation (and you!) can copy and distribute it in the\r\n"
    "United States without permission and without paying copyright royalties.\r\n"
)


def selftest() -> int:
    failures: list[str] = []

    def check(name: str, condition: bool, detail: str = "") -> None:
        print(f"{'PASS' if condition else 'FAIL'} {name} {detail}", flush=True)
        if not condition:
            failures.append(name)

    notice, body = split_gutenberg(FIXTURE)
    check("license kept in the notice", "Project Gutenberg License" in notice)
    check("body has no START marker", "*** START" not in body)
    check("body has no END marker", "*** END" not in body)
    check("CRLF normalised", "\r" not in body)
    chapters = chapterize(body)
    check("contents listing is not chapters", len(chapters) == 3, f"got {len(chapters)}")
    check("real headings survive",
          [c["title"] for c in chapters] == ["CHAPTER I", "CHAPTER II", "CHAPTER III"],
          str([c["title"] for c in chapters]))
    check("every chapter has words", all(c["word_count"] > 0 for c in chapters))
    check("no chapter below the floor",
          all(len(c["_text"]) >= MIN_CHAPTER_CHARS for c in chapters))

    def squashed(value: str) -> str:
        return re.sub(r"\s+", "", value)

    check("chapter spans are lossless",
          squashed("".join(c["_text"] for c in chapters)) == squashed(body))
    # Very short front matter lets the contents run absorb the first real heading, so
    # its title degrades to "Part 1". Not one character of the book may be lost anyway.
    _, tiny_body = split_gutenberg(FIXTURE.replace(FRONT_MATTER, "A dedication line.\r\n\r\n"))
    tiny = chapterize(tiny_body)
    check("short front matter still loses no text",
          squashed("".join(c["_text"] for c in tiny)) == squashed(tiny_body),
          f"{len(tiny)} chapters, first title {tiny[0]['title']!r}")
    check("zero-width damage removed",
          repair_text("a" + chr(0x200B) + "b" + chr(0xFEFF) + "c") == "abc")
    check("invisible class covers its codepoints",
          all(_INVISIBLE_RE.search(chr(c)) for c in (0x200B, 0x200F, 0x202A, 0x202E,
                                                     0x2060, 0x2069, 0xFEFF)))
    check("tatweel and presentation forms matched",
          bool(_TATWEEL_RE.search(chr(0x0640))) and bool(_PRESENT_FORMS_RE.search(chr(0xFB50))))
    check("orphan mark stripped between latin letters",
          repair_text("b" + chr(0x064B) + "c") == "bc")
    check("mark attached to an arabic letter is kept",
          repair_text(chr(0x0643) + chr(0x064B)) == chr(0x0643) + chr(0x064B))
    check("torn mark reattached to its arabic letter",
          repair_text(chr(0x0643) + " " + chr(0x064B)) == chr(0x0643) + chr(0x064B))
    check("git blob sha1 vector",
          git_blob_sha1(b"hello") == "b6fc4c620b67d95f953a5c1c1230aaab5db5a1b0")
    check("sharded path accepted", not misplaced("books/gutendex/76/76000/en/ch001_en.txt"))
    check("legacy path accepted", not misplaced("books/gutendex/100/en/book.json"))
    check("flat book path rejected", misplaced("books/gutendex/ch001_en.txt"))
    check("state one level down", not misplaced("_worker/library_index.json"))
    check("nested state rejected", misplaced("_worker/sub/library_index.json"))
    check("root index allowed", not misplaced("catalog.parquet"))
    check("stray root file rejected", misplaced("notes.txt"))
    check("bucket arithmetic", book_folder(76000) == "books/gutendex/76/76000/en")
    check("filter is strict english text",
          catalog_row_ok({"Type": "Text", "Language": "en"})
          and not catalog_row_ok({"Type": "Text", "Language": "en; fr"})
          and not catalog_row_ok({"Type": "Sound", "Language": "en"}))
    check("subject list split and embedded CRLF collapsed",
          split_list("A\r\nB; C ;") == ["A B", "C"])

    # A state flush must rewrite ledger.json byte-for-byte as the batch committed it,
    # otherwise --verify-last compares the repo against a stale expectation and fails
    # a run whose books are all intact.
    class _Budget:
        def report(self):
            return {}

    probe = {"version": 1, "queue": [], "updated_at": ""}
    merge_ledger(probe, [{"id": 1, "title": "T", "author": "A", "cover_url": "",
                          "chapters": 1, "chars": 10, "words": 2,
                          "text_bytes": 10, "asset_bytes": 20}])
    committed = expectations(
        {"ledger.json": json.dumps(probe, ensure_ascii=False).encode("utf-8")})
    # Advance the clock deterministically: a flush that re-stamps the ledger would
    # otherwise look identical whenever both calls land in the same second.
    real_stamp = globals()["stamp"]
    globals()["stamp"] = lambda: "1999-01-01T00:00:00+00:00"
    try:
        flushed = state_files({"books": {}}, probe, _Budget(), 1, committed, "")
    finally:
        globals()["stamp"] = real_stamp
    check("state flush rewrites the committed ledger byte-for-byte",
          git_blob_sha1(flushed["ledger.json"]) == committed["ledger.json"][1])

    # A refused commit parks the pipeline until the next UTC midnight; a run that starts
    # before then must exit without sending one request. Both halves are proved here with
    # a stub api and a frozen clock, so the check needs no network.
    class _Api:
        def __init__(self):
            self.commits = []
            self.reads = 0

        def list_repo_commits(self, *a, **k):
            self.reads += 1
            return iter(())

        def create_commit(self, repo, operations, **k):
            self.commits.append({op.path_in_repo: op.path_or_fileobj for op in operations})
            return "stub-commit"

    class _Proceeded(Exception):
        pass

    class _BudgetProbe:
        def __init__(self, *a, **k):
            raise _Proceeded()

    fake = _Api()
    parked = Budget(fake, 500, 0.0, 150)
    parked.state = {"remaining": 12, "reset_in": 45}
    parked.stop_reason = "commit refused by the hub (quota): 429"
    hook = dict(STATE_HOOK)
    STATE_HOOK.clear()
    STATE_HOOK.update({"index": {"books": {}}, "ledger": {"version": 1, "queue": []},
                       "budget": parked, "expected": {}})
    real_time, real_sleep = time.time, time.sleep
    real_api, real_read, real_budget = hf_api, read_file, Budget
    real_headers = api_headers
    frozen = 1_800_000_000                     # mid-day, so midnight beats reset+60
    midnight = ((frozen // 86400) + 1) * 86400
    try:
        time.time = lambda: float(frozen)
        epoch = save_backoff(fake, parked)
        marker = json.loads(fake.commits[-1]["_worker/backoff.json"])
        check("a refused commit parks until the next UTC midnight",
              epoch == midnight and marker["not_before_epoch"] == midnight
              and midnight % 86400 == 0, f"epoch={epoch} midnight={midnight}")
        reads_before = fake.reads
        parked.report()
        check("the report after a park sends no further request",
              fake.reads == reads_before, f"extra_reads={fake.reads - reads_before}")

        sent = len(fake.commits)
        globals()["hf_api"] = lambda: fake
        globals()["read_file"] = lambda api, path, required=False: (
            json.dumps({"not_before_epoch": epoch}).encode() if path == "_worker/backoff.json"
            else None)
        globals()["Budget"] = _BudgetProbe
        probe_args = argparse.Namespace(max_commits_per_run=500, min_commit_interval=5.0,
                                        min_remaining=150)
        time.time = lambda: float(epoch - 60)
        before = run(probe_args)
        check("a run before the marker exits at once and sends nothing",
              before.get("waiting_for_backoff") is True and len(fake.commits) == sent,
              f"requests={len(fake.commits) - sent}")
        time.time = lambda: float(epoch + 1)
        try:
            run(probe_args)
            proceeded = False
        except _Proceeded:
            proceeded = True
        check("a run after the marker proceeds, so the gate is time-sensitive", proceeded)
        globals()["Budget"] = real_budget

        # A refusal on the very first batch must not erase the checkpoint the previous
        # run verified, or --verify-last goes red on a park that is meant to be green.
        previous = {"last_commit": {"commit": "https://hub/previous",
                                    "expected": {"ledger.json": ["blob", "abc123", 12]}}}
        globals()["read_file"] = lambda api, path, required=False: (
            json.dumps(previous).encode() if path == "_worker/library_checkpoint.json" else None)
        heir = real_budget(fake, 500, 0.0, 150)
        carried = prime_last_commit(fake, heir)
        check("a run carries the previous verified expectations forward",
              carried == {"ledger.json": ("blob", "abc123", 12)}
              and heir.last_checkpoint.get("commit") == "https://hub/previous",
              f"carried={len(carried)}")
        STATE_HOOK["expected"] = carried
        save_backoff(fake, heir)
        written = json.loads(fake.commits[-1]["_worker/library_checkpoint.json"])
        check("the marker commit keeps the previous checkpoint verifiable",
              written["last_commit"] == previous["last_commit"],
              f"last_commit={written['last_commit']}")
        check("run() actually carries it, not just the helper",
              "prime_last_commit" in run.__code__.co_names)

        sleeps: list[float] = []
        time.sleep = lambda s: sleeps.append(s)
        windows = iter([{"RateLimit": '"api";r=12;t=7', "RateLimit-Policy": '"api";q=1000;w=300'},
                        {"RateLimit": '"api";r=980;t=293', "RateLimit-Policy": '"api";q=1000;w=300'}])
        globals()["api_headers"] = lambda *a, **k: next(windows)
        time.time = real_time
        waiter = Budget(fake, 500, 0.0, 150)
        check("a low rate window waits inside the run and then continues",
              waiter.allow() == "" and sleeps == [12.0], f"sleeps={sleeps}")
        globals()["api_headers"] = lambda *a, **k: {"RateLimit": '"api";r=12;t=200'}
        tight = Budget(fake, 500, 0.0, 150)
        tight.deadline = time.time() + 30
        block = tight.allow()
        check("a low window near the deadline stops instead of waiting past it",
              block.startswith("rate window low and no time left") and sleeps == [12.0],
              f"block={block!r}")

        # The park is a planned stop, so the job must be green; a refusal that could not
        # write its marker has no guaranteed wait and must stay red.
        real_run, real_argv, real_stdout = run, sys.argv, sys.stdout
        globals()["run"] = lambda args: {"refused": "commit refused by the hub (quota)",
                                         "parked_until_epoch": midnight}
        sys.argv = ["worker.py"]
        sys.stdout = io.StringIO()
        try:
            parked_code = main()
            globals()["run"] = lambda args: {"refused": "commit refused by the hub (quota)"}
            unmarked_code = main()
        finally:
            globals()["run"], sys.argv, sys.stdout = real_run, real_argv, real_stdout
        check("an intentional quota park exits green", parked_code == 0, f"code={parked_code}")
        check("a refusal whose marker failed stays red", unmarked_code == 1,
              f"code={unmarked_code}")
    finally:
        time.time, time.sleep = real_time, real_sleep
        globals()["hf_api"], globals()["read_file"] = real_api, real_read
        globals()["Budget"], globals()["api_headers"] = real_budget, real_headers
        STATE_HOOK.clear()
        STATE_HOOK.update(hook)
    print(f"self-test: {len(failures)} failures", flush=True)
    return 1 if failures else 0


def main() -> int:
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--commit-files", type=int, default=2500)
    parser.add_argument("--batches", type=int, default=500)
    parser.add_argument("--scan", type=int, default=0)
    parser.add_argument("--fetch-interval", type=float, default=0.3)
    parser.add_argument("--max-minutes", type=float, default=320.0)
    parser.add_argument("--reserve-minutes", type=float, default=10.0)
    parser.add_argument("--max-commits-per-run", type=int,
                        default=int(os.environ.get("MAX_COMMITS_PER_RUN", "500")))
    parser.add_argument("--min-commit-interval", type=float, default=5.0)
    parser.add_argument("--min-remaining", type=int, default=150)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--verify-last", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return selftest()
    if args.verify_last:
        report = verify_last(hf_api())
        print(json.dumps(report, ensure_ascii=False, indent=1))
        return 1 if report.get("mismatches") or report.get("refused") else 0
    report = run(args)
    print(json.dumps(report, ensure_ascii=False, indent=1))
    # Parking until UTC midnight after a quota refusal is a planned stop, so the job is
    # green; a refusal whose marker could not be written is still a failure.
    if report.get("refused") and not report.get("parked_until_epoch"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())