#!/usr/bin/env python3
"""Rebuild and upload M-hv1/Titan-Library/catalog/titan_books_index.db.gz
and books_index_meta.json with 100% schema parity for the Android app's
BooksIndexMetaCodec.
"""
from __future__ import annotations

import os

# Must be set before importing huggingface_hub so uploads avoid Xet pitfalls.
os.environ.setdefault("HF_HUB_DISABLE_XET", "1")

import argparse
import concurrent.futures
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import pathlib
import random
import re
import sqlite3
import tempfile
import threading
import time
from typing import Any

import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter

TITAN_LIBRARY_REPO_ID = os.environ.get("HF_LIBRARY_DATASET_REPO", "M-hv1/Titan-Library")
TITAN_LIBRARY_REVISION = os.environ.get("HF_LIBRARY_DATASET_REVISION", "main")
TITAN_LIBRARY_BASE = f"https://huggingface.co/datasets/{TITAN_LIBRARY_REPO_ID}/resolve/{TITAN_LIBRARY_REVISION}"
TITAN_LIBRARY_TREE_URL = f"https://huggingface.co/api/datasets/{TITAN_LIBRARY_REPO_ID}/tree/{TITAN_LIBRARY_REVISION}/books/gutendex"
TITAN_LIBRARY_LEDGER_URL = f"{TITAN_LIBRARY_BASE}/ledger.json"
TITAN_LIBRARY_CATALOG_PARQUET_URL = f"{TITAN_LIBRARY_BASE}/catalog.parquet"

BOOKS_INDEX_SCHEMA_VERSION = 1
BOOKS_INDEX_DB_NAME = "titan_books_index.db"
BOOKS_INDEX_GZ_NAME = "titan_books_index.db.gz"
BOOKS_INDEX_META_NAME = "books_index_meta.json"
TITAN_LIBRARY_CATALOG_META_URL = f"{TITAN_LIBRARY_BASE}/catalog/{BOOKS_INDEX_META_NAME}"

_THREAD_LOCAL = threading.local()

_MARC_SUBFIELD_COLON_RE = re.compile(r"\s*:\s*\$[a-z]\b\s*", re.IGNORECASE)
_MARC_SUBFIELD_RE = re.compile(r"\s*\$[a-z]\b\s*", re.IGNORECASE)
_MARC_TRAILING_SLASH_RE = re.compile(r"\s*/\s*$")
_WHITESPACE_RE = re.compile(r"\s+")
_AUTHOR_DATE_RE = re.compile(
    r",\s*(?:(?:ca\.|fl\.|b\.|d\.|approximately)\s*)?\d{3,4}\??(?:\s*[-–]\s*(?:\d{3,4}\??)?)?\.?\s*$",
    re.IGNORECASE,
)
_AUTHOR_INLINE_DATE_RE = re.compile(
    r",\s*(?:(?:ca\.|fl\.|b\.|d\.|approximately)\s*)?\d{3,4}\??\s*[-–]\s*(?:\d{3,4}\??)?\.?",
    re.IGNORECASE,
)
_CORPORATE_AUTHOR_WORDS = (
    "university", "society", "department", "committee", "library", "institute",
    "association", "commission", "united states", "great britain", "anonymous",
    "various", "unknown", "project gutenberg", "church", "government", "academy",
)
_GENERATIONAL_SUFFIXES = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v", "esq", "esq."}


def _hf_headers() -> dict[str, str]:
    headers = {
        "User-Agent": (
            "TitanLibrary-IndexBuilder/1.0 "
            "(+https://github.com/aljokar-m-1998/titan-library-worker)"
        )
    }
    tok = os.environ.get("HF_TOKEN", "").strip()
    if tok:
        headers["Authorization"] = f"Bearer {tok}"
    return headers


def _get_http_session() -> requests.Session:
    sess = getattr(_THREAD_LOCAL, "session", None)
    if sess is None:
        sess = requests.Session()
        adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16)
        sess.mount("https://", adapter)
        sess.mount("http://", adapter)
        sess.headers.update(_hf_headers())
        _THREAD_LOCAL.session = sess
    return sess


def http_get_bytes(url: str, timeout: int = 30, max_attempts: int = 5) -> bytes:
    """GET bytes with connection pooling and polite exponential backoff on 429/5xx."""
    sess = _get_http_session()
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            resp = sess.get(url, timeout=timeout)
            if resp.status_code == 404:
                resp.raise_for_status()
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                retry_after = resp.headers.get("Retry-After")
                wait = (
                    float(retry_after)
                    if (retry_after and retry_after.replace(".", "", 1).isdigit())
                    else (0.6 * (2 ** attempt) + random.uniform(0.05, 0.35))
                )
                if attempt < max_attempts - 1:
                    time.sleep(min(wait, 20.0))
                    continue
            resp.raise_for_status()
            return resp.content
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                raise
            last_exc = exc
        except requests.RequestException as exc:
            last_exc = exc
        if attempt < max_attempts - 1:
            time.sleep(min(0.6 * (2 ** attempt) + random.uniform(0.05, 0.35), 20.0))
    assert last_exc is not None
    raise last_exc


def fetch_json(url: str) -> Any:
    return json.loads(http_get_bytes(url, timeout=30).decode("utf-8"))


def clean_book_title(raw: str | None) -> str:
    """Strip MARC catalog artifacts ($b, $c, trailing '/') and collapse whitespace."""
    if not raw:
        return ""
    s = _MARC_SUBFIELD_COLON_RE.sub(": ", raw)
    s = _MARC_SUBFIELD_RE.sub(" ", s)
    s = _MARC_TRAILING_SLASH_RE.sub("", s)
    return _WHITESPACE_RE.sub(" ", s).strip()


def _normalize_single_author(part: str) -> str:
    s = _AUTHOR_INLINE_DATE_RE.sub("", part)
    s = _AUTHOR_DATE_RE.sub("", s).strip().rstrip(",").strip()
    if not s:
        return ""
    lower = s.lower()
    if any(w in lower for w in _CORPORATE_AUTHOR_WORDS):
        return _WHITESPACE_RE.sub(" ", s)
    if "," in s:
        pieces = [p.strip() for p in s.split(",") if p.strip()]
        if len(pieces) == 2 and pieces[1].lower() not in _GENERATIONAL_SUFFIXES:
            s = f"{pieces[1]} {pieces[0]}"
        elif len(pieces) == 3 and pieces[2].lower() in _GENERATIONAL_SUFFIXES:
            s = f"{pieces[1]} {pieces[0]} {pieces[2]}"
    return _WHITESPACE_RE.sub(" ", s).strip()


def clean_book_author(raw: str | None) -> str:
    """Normalize 'Last, First, 1819-1891' -> 'First Last' while preserving multi-author/corporate names."""
    if not raw or not raw.strip():
        return "Classic Literature"
    parts = [p.strip() for p in raw.split(";") if p.strip()]
    cleaned = [_normalize_single_author(p) for p in parts]
    cleaned = [c for c in cleaned if c]
    if not cleaned:
        return "Classic Literature"
    return "; ".join(cleaned)


def _parse_book_path_entry(raw_path: str, default_lang: str = "en") -> tuple[str, str] | None:
    """Normalize a dataset path into (repo_path, lang_subdir), e.g. ('books/gutendex/1/1001', 'en')."""
    clean = raw_path.strip().strip("/")
    if not clean:
        return None
    if clean.endswith("/book.json"):
        clean = clean[: -len("/book.json")].strip("/")
    parts = clean.split("/")
    if len(parts) >= 2 and re.fullmatch(r"[a-z]{2,3}", parts[-1]):
        return "/".join(parts[:-1]), parts[-1]
    lang = default_lang.strip().lower() if default_lang else "en"
    if not re.fullmatch(r"[a-z]{2,3}", lang):
        lang = "en"
    return clean, lang


def discover_titan_library_books() -> list[tuple[str, str]]:
    """Discover all (repo_path, lang) pairs via catalog.parquet + HF tree shard traversal."""
    discovered: dict[str, tuple[str, str]] = {}

    def register(repo_path: str, lang: str) -> None:
        key = f"{repo_path}/{lang}"
        if key not in discovered:
            discovered[key] = (repo_path, lang)

    try:
        parquet_bytes = http_get_bytes(TITAN_LIBRARY_CATALOG_PARQUET_URL, timeout=60)
        table = pq.read_table(io.BytesIO(parquet_bytes), columns=["gutenberg_id", "book_path", "language"])
        for row in table.to_pylist():
            bpath = row.get("book_path")
            lang = str(row.get("language") or "en").strip().lower() or "en"
            if isinstance(bpath, str) and bpath.strip():
                parsed = _parse_book_path_entry(bpath, default_lang=lang)
                if parsed:
                    register(*parsed)
        print(f"catalog.parquet discovered {len(discovered)} book entries")
    except Exception as exc:
        print(f"WARN catalog.parquet discovery failed ({exc}); falling back to HF tree traversal")

    try:
        top_entries = fetch_json(TITAN_LIBRARY_TREE_URL)
        shard_dirs: list[str] = []
        for entry in top_entries:
            if entry.get("type") != "directory":
                continue
            path = str(entry.get("path") or "").strip("/")
            leaf = path.rsplit("/", 1)[-1]
            if len(leaf) == 1 and leaf.isdigit():
                shard_dirs.append(path)
            else:
                register(path, "en")

        def list_shard(shard_path: str) -> list[str]:
            url = f"https://huggingface.co/api/datasets/{TITAN_LIBRARY_REPO_ID}/tree/{TITAN_LIBRARY_REVISION}/{shard_path}"
            sub = fetch_json(url)
            return [
                str(d.get("path") or "").strip("/")
                for d in sub
                if d.get("type") == "directory" and d.get("path")
            ]

        if shard_dirs:
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
                for sub_paths in pool.map(list_shard, shard_dirs):
                    for sp in sub_paths:
                        register(sp, "en")
    except Exception as exc:
        print(f"WARN HF tree traversal partial/failed: {exc}")

    return list(discovered.values())


def infer_book_cefr(book: dict[str, Any]) -> str:
    text = f"{book.get('title', '')} {' '.join(book.get('subjects', []) or [])}".lower()
    if any(k in text for k in ("children", "fairy", "fable", "wonderland", "peter rabbit", "juvenile")):
        return "A2"
    if any(k in text for k in ("shakespeare", "philosophy", "tragedy", "republic", "homer", "iliad", "odyssey")):
        return "C1"
    total_words = sum((c.get("word_count") or 0) for c in (book.get("chapters") or []))
    return "B2" if total_words > 90000 else "B1"


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def build_books_index_dist(
    dist_dir: pathlib.Path,
    book_rows: list[tuple[Any, ...]],
    books_fts_rows: list[tuple[str, str, str, str]],
    chapter_rows: list[tuple[Any, ...]],
    updated_at: str,
) -> dict[str, Any]:
    dist_dir.mkdir(parents=True, exist_ok=True)
    books_db = dist_dir / BOOKS_INDEX_DB_NAME
    books_gz = dist_dir / BOOKS_INDEX_GZ_NAME
    meta_json = dist_dir / BOOKS_INDEX_META_NAME
    for stale in (books_db, books_gz):
        if stale.exists():
            stale.unlink()

    built_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    effective_updated_at = updated_at or built_at

    conn = sqlite3.connect(str(books_db))
    cur = conn.cursor()
    cur.executescript(
        """
        PRAGMA page_size = 4096;
        PRAGMA journal_mode = DELETE;

        CREATE TABLE index_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE books (
            book_id TEXT PRIMARY KEY,
            repo_path TEXT NOT NULL,
            title TEXT NOT NULL,
            title_ar TEXT,
            author TEXT NOT NULL,
            cefr TEXT NOT NULL,
            subjects_json TEXT NOT NULL,
            cover_url TEXT,
            audio_available INTEGER NOT NULL,
            chapter_count INTEGER NOT NULL,
            total_words INTEGER NOT NULL
        );

        CREATE VIRTUAL TABLE books_fts USING fts5(
            book_id UNINDEXED,
            title,
            author,
            subjects,
            tokenize='unicode61 remove_diacritics 2'
        );

        CREATE TABLE book_chapters (
            book_id TEXT NOT NULL,
            chapter_number INTEGER NOT NULL,
            title TEXT NOT NULL,
            word_count INTEGER NOT NULL,
            audio_seconds REAL NOT NULL,
            narrator_voice TEXT,
            dialogue_voice TEXT,
            text_url TEXT NOT NULL,
            audio_url TEXT,
            karaoke_url TEXT,
            PRIMARY KEY (book_id, chapter_number)
        );
        """
    )
    cur.executemany(
        "INSERT INTO index_meta(key, value) VALUES (?, ?)",
        [
            ("schema_version", str(BOOKS_INDEX_SCHEMA_VERSION)),
            ("titan_library_updated_at", effective_updated_at),
            ("titan_library_books_count", str(len(book_rows))),
            ("titan_library_chapters_count", str(len(chapter_rows))),
            ("built_at", built_at),
        ],
    )
    cur.executemany(
        "INSERT INTO books VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        book_rows,
    )
    cur.executemany(
        "INSERT INTO books_fts(book_id, title, author, subjects) VALUES (?, ?, ?, ?)",
        books_fts_rows,
    )
    cur.executemany(
        "INSERT INTO book_chapters VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        chapter_rows,
    )
    conn.commit()
    cur.execute("VACUUM")
    conn.close()

    with books_db.open("rb") as fin, books_gz.open("wb") as fout:
        with gzip.GzipFile(
            filename=BOOKS_INDEX_DB_NAME,
            mode="wb",
            fileobj=fout,
            compresslevel=9,
            mtime=0,
        ) as gzf:
            while True:
                chunk = fin.read(1024 * 1024)
                if not chunk:
                    break
                gzf.write(chunk)

    meta_payload: dict[str, Any] = {
        "schema_version": BOOKS_INDEX_SCHEMA_VERSION,
        "updated_at": effective_updated_at,
        "book_count": len(book_rows),
        "chapter_count": len(chapter_rows),
        "sha256_gz": sha256_file(books_gz),
        "sha256_db": sha256_file(books_db),
        "size_bytes_gz": books_gz.stat().st_size,
        "size_bytes_db": books_db.stat().st_size,
        "db_filename": BOOKS_INDEX_GZ_NAME,
    }
    meta_json.write_text(json.dumps(meta_payload, indent=2) + "\n", encoding="utf-8")
    return meta_payload


def upload_books_index_to_hf(dist_dir: pathlib.Path, expected_book_count: int) -> bool:
    from huggingface_hub import HfApi, get_token

    token = os.environ.get("HF_TOKEN", "").strip() or (get_token() or "").strip()
    if not token:
        raise RuntimeError("HF_TOKEN is required to upload books index to M-hv1/Titan-Library/catalog/")

    print(f"Uploading books index artifacts to {TITAN_LIBRARY_REPO_ID}/catalog/ ...")
    api = HfApi(token=token)
    api.upload_folder(
        folder_path=str(dist_dir),
        repo_id=TITAN_LIBRARY_REPO_ID,
        repo_type="dataset",
        revision=TITAN_LIBRARY_REVISION,
        path_in_repo="catalog",
        allow_patterns=[BOOKS_INDEX_GZ_NAME, BOOKS_INDEX_META_NAME],
        commit_message="Update remote books catalog index",
    )

    resp = requests.get(TITAN_LIBRARY_CATALOG_META_URL, headers=_hf_headers(), timeout=30)
    print(f"Live metadata check {TITAN_LIBRARY_CATALOG_META_URL} -> HTTP {resp.status_code}")
    resp.raise_for_status()
    live_meta = resp.json()
    live_count = int(live_meta.get("book_count") or 0)
    if live_count != expected_book_count:
        raise RuntimeError(
            f"Live books_index_meta.json book_count={live_count} != expected {expected_book_count}"
        )
    print(f"Verified live books_index_meta.json (HTTP 200, book_count={live_count})")
    return True


def collect_books_rows(
    cache_dir: pathlib.Path,
) -> tuple[list[tuple[Any, ...]], list[tuple[str, str, str, str]], list[tuple[Any, ...]], str]:
    ledger = fetch_json(TITAN_LIBRARY_LEDGER_URL)
    books_updated_at = str(ledger.get("updated_at") or "")
    book_targets = discover_titan_library_books()
    print(f"Discovered {len(book_targets)} Titan-Library book paths; fetching manifests...")

    book_cache_dir = cache_dir / "titan_library_books"
    book_cache_dir.mkdir(parents=True, exist_ok=True)

    def load_book(target: tuple[str, str], max_attempts: int = 6) -> tuple[str, str, dict[str, Any]] | None:
        bpath, lang = target
        cache_file = book_cache_dir / f"{bpath.replace('/', '__')}__{lang}.json"
        if cache_file.exists() and cache_file.stat().st_size > 0:
            try:
                return bpath, lang, json.loads(cache_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        try:
            raw = http_get_bytes(f"{TITAN_LIBRARY_BASE}/{bpath}/{lang}/book.json", timeout=30, max_attempts=max_attempts)
            data = json.loads(raw.decode("utf-8"))
            cache_file.write_bytes(raw)
            return bpath, lang, data
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404 and lang != "en":
                try:
                    raw = http_get_bytes(f"{TITAN_LIBRARY_BASE}/{bpath}/en/book.json", timeout=30, max_attempts=max_attempts)
                    data = json.loads(raw.decode("utf-8"))
                    cache_file.write_bytes(raw)
                    return bpath, "en", data
                except Exception:
                    return None
            return None
        except Exception:
            return None

    books_data: list[tuple[str, str, dict[str, Any]]] = []
    failed_targets: list[tuple[str, str]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        for target, res in zip(book_targets, pool.map(load_book, book_targets)):
            if res is not None:
                books_data.append(res)
            else:
                failed_targets.append(target)

    if failed_targets:
        print(f"Retrying {len(failed_targets)} rate-limited/transient book fetches after 15s cooldown...")
        time.sleep(15)
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            for target, res in zip(failed_targets, pool.map(lambda t: load_book(t, max_attempts=7), failed_targets)):
                if res is not None:
                    books_data.append(res)
                else:
                    print(f"WARN skipped book {target[0]}/{target[1]} after retries")

    book_rows: list[tuple[Any, ...]] = []
    chapter_rows: list[tuple[Any, ...]] = []
    books_fts_rows: list[tuple[str, str, str, str]] = []
    seen_book_ids: set[str] = set()

    for bpath, lang, book in sorted(books_data, key=lambda x: str(x[2].get("id") or x[0])):
        bid = str(book.get("id") or bpath.rsplit("/", 1)[-1])
        if bid in seen_book_ids:
            continue
        seen_book_ids.add(bid)

        raw_title = (book.get("title_en") or book.get("title") or bid).strip()
        title = clean_book_title(raw_title) or bid
        title_ar = clean_book_title(book.get("title_ar")) or None
        author = clean_book_author(book.get("author"))
        subjects = [s for s in (book.get("subjects") or []) if isinstance(s, str)]
        cover = book.get("cover")
        audio_avail = 1 if book.get("audio_available") else 0
        chapters = book.get("chapters") or []
        total_words = sum(int(c.get("word_count") or 0) for c in chapters)
        cefr = infer_book_cefr(book)

        book_rows.append(
            (
                bid,
                bpath,
                title,
                title_ar,
                author,
                cefr,
                json.dumps(subjects, ensure_ascii=False),
                cover,
                audio_avail,
                len(chapters),
                total_words,
            )
        )
        books_fts_rows.append((bid, title, author, ", ".join(subjects)))

        for ch in chapters:
            ch_num = int(ch.get("chapter_number") or 0)
            if ch_num <= 0:
                continue
            ch_title = clean_book_title(ch.get("title")) or f"Chapter {ch_num}"
            w_count = int(ch.get("word_count") or 0)
            a_secs = float(ch.get("audio_seconds") or 0.0)
            narrator = ch.get("narrator_label") or ch.get("narrator_voice")
            dialogue_v = ch.get("dialogue_label") or ch.get("dialogue_voice")

            raw_text_url = ch.get("text_url") or f"ch{ch_num:03d}_{lang}.txt"
            text_url = raw_text_url if raw_text_url.startswith("http") else f"{TITAN_LIBRARY_BASE}/{bpath}/{lang}/{raw_text_url}"

            audio_map = ch.get("audio_urls") or {}
            a_rel = audio_map.get(lang) or audio_map.get("en")
            audio_url = (a_rel if a_rel.startswith("http") else f"{TITAN_LIBRARY_BASE}/{bpath}/{lang}/{a_rel}") if a_rel else None

            kar_map = ch.get("karaoke_urls") or {}
            k_rel = kar_map.get(lang) or kar_map.get("en")
            kar_url = (k_rel if k_rel.startswith("http") else f"{TITAN_LIBRARY_BASE}/{bpath}/{lang}/{k_rel}") if k_rel else None

            chapter_rows.append(
                (
                    bid,
                    ch_num,
                    ch_title,
                    w_count,
                    a_secs,
                    narrator,
                    dialogue_v,
                    text_url,
                    audio_url,
                    kar_url,
                )
            )

    return book_rows, books_fts_rows, chapter_rows, books_updated_at


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild and upload Titan-Library app books SQLite index.")
    parser.add_argument("--dist-dir", type=pathlib.Path, default=None)
    parser.add_argument("--cache-dir", type=pathlib.Path, default=None)
    parser.add_argument("--dry-run", action="store_true", help="Build locally without uploading to Hugging Face.")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="titan_books_idx_") as tmp:
        tmp_path = pathlib.Path(tmp)
        dist_dir = args.dist_dir or (tmp_path / "dist")
        cache_dir = args.cache_dir or (tmp_path / "cache")

        book_rows, books_fts_rows, chapter_rows, updated_at = collect_books_rows(cache_dir)
        meta = build_books_index_dist(
            dist_dir=dist_dir,
            book_rows=book_rows,
            books_fts_rows=books_fts_rows,
            chapter_rows=chapter_rows,
            updated_at=updated_at,
        )
        print(json.dumps(meta, indent=2))
        if not args.dry_run:
            upload_books_index_to_hf(dist_dir, expected_book_count=int(meta["book_count"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
