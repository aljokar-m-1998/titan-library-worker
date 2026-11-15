# titan-library-worker

Cloud-only ingestion of Project Gutenberg into the Hugging Face dataset
[`M-hv1/Titan-Library`](https://huggingface.co/datasets/M-hv1/Titan-Library).

**Text, metadata and covers only.** No audio is synthesized, uploaded or indexed.
The American English Daily client narrates on device, so server-side TTS for this
dataset is retired (see `tts-switch.yml`).

Every byte of fetching, cleaning, packaging and committing happens on a GitHub
Actions runner. Nothing is downloaded to a laptop.

## What it reuses

This is not a new architecture. Three pieces are vendored from code already running
in production:

| Piece | Source |
| --- | --- |
| Commit accounting, rate-window probe, backoff marker, snapshot/rollback, SIGTERM flush | `aljokar-m-1998/titan-learn-worker` → `worker.py` |
| Gutenberg boilerplate split, TOC-run suppression, chapteriser, license notice | `M-hv1/Titan-Gateway` → `api/library.py` |
| Unicode repair (zero-width, tatweel, presentation forms, torn/orphan marks) | `M-hv1/Titan-Gateway` → `api/books.py` |

The workflow shape (pinned deps, `concurrency` with `cancel-in-progress: false`,
`permissions: contents: read`, explicit exit-code capture, independent verification
step, artifacts on `always()`) is copied from `titan-learn-worker/.github/workflows/archive.yml`.

`Titan-Learn`'s own audio pipeline is **not touched** by anything in this repository.

## Layout

```
books/gutendex/{id // 1000}/{id}/en/ch001_en.txt … chNNN_en.txt
books/gutendex/{id // 1000}/{id}/en/cover.jpg
books/gutendex/{id // 1000}/{id}/en/LICENSE.txt
books/gutendex/{id // 1000}/{id}/en/book.json
ledger.json                  # every existing top-level key is preserved
catalog.parquet              # compact index, zstd
_worker/library_index.json   # per-book status, the resume truth
_worker/library_checkpoint.json
_worker/backoff.json         # only written after a real quota refusal
reports/commit_budget.json
```

The 63 books the gateway already stored stay at `books/gutendex/{id}/en/` and are
never moved or re-fetched. `existing_books()` probes **both** shapes for every
candidate, because a bucket name can collide with a legacy book id (`11` is both).

Sharding exists because the hub refuses a directory past **10,000 entries** — the
failure that killed the flat `Titan-Learn` root at 9,989 files. `dir_capacity()`
measures both accumulating levels before every commit.

## Commit policy

There is no assumed daily ceiling; the hub's own answer decides.

- One `create_commit` per batch carries the book files **and** `ledger.json`,
  `catalog.parquet`, `_worker/*` and `reports/*` — content and state can never drift.
- A batch is capped by `--commit-files` (default 1500), not by a book count, because
  chapter counts vary by two orders of magnitude. A book that would overflow the cap
  is carried to the next commit **without being downloaded again**.
- `Budget.allow()` stops on `--max-commits-per-run`, on `--min-commit-interval`
  pacing, or when the observed `RateLimit` window drops below `--min-remaining`.
- Only a 429 or a quota refusal writes `_worker/backoff.json`. A structural refusal
  rolls the batch back, stops the run and exits non-zero without marking a book failed.
- A batch only starts if `elapsed + last_batch_seconds + reserve_minutes <= max_minutes`.

## Verification

`*.txt` and `*.json` are plain git blobs in this dataset; `*.jpg` and `*.parquet` are
LFS (see its `.gitattributes`). So `remote_check()` compares `blob_id` (git sha1) for
text and `lfs.sha256` + size for images, and falls back to re-fetching CDN bytes when
a Xet-backed object exposes no LFS oid. `--verify-last` re-reads the checkpoint and
verifies independently of the run that wrote it.

`--self-test` is a network-free positive control: a synthetic Gutenberg fixture with
a contents listing, a dedication and three real chapters must yield exactly three
chapters with the right titles, plus known-answer vectors for the git blob sha1, the
path guard and the strict `en` / `Text` filter.

Hash equality proves the hub stores what the runner wrote, not that a reader can fetch
it. The `audit the public shelf without a token` step closes that layer: it pulls
`ledger.json`, `catalog.parquet`, `_worker/library_index.json` and one recent book's
`book.json`, `ch001_en.txt` and `cover.jpg` over the public CDN **anonymously**, checks
the catalog schema and row count against the ledger, requires the full field set of
every entry the index marks `origin=catalog`, and requires a positive `word_count` on
every chapter of the sampled book — the number the client sums. Gateway-era entries are
reported as INFO rather than failed, because the client reads their word counts from
`book.json`, never from the ledger.

## Running

```bash
gh workflow run ingest.yml -f dry_run=true          # schema + cleaner, zero commits
gh workflow run ingest.yml -f batch_size=25         # small remote test
gh workflow run tts-switch.yml -f state=off         # retire gateway narration
gh workflow run tts-switch.yml -f state=on          # restore it
```

The daily cron (`17 3 * * *`) uses `batch_size=250`. Set the repository variable
`INGEST_DRY_RUN=true` to make scheduled runs commit nothing.

## Secret

`HF_TOKEN` must be able to **write the dataset** and **administer the Space**
(`add_space_variable` + `restart_space`). Set it with:

```bash
gh secret set HF_TOKEN --repo aljokar-m-1998/titan-library-worker
```

> **Credential lifetime.** The token currently wired in is an OAuth access token
> that expires on **2026-12-16**. It is not refreshed by GitHub Actions. Before that
> date, replace it with a non-expiring fine-grained write token from
> <https://huggingface.co/settings/tokens> scoped to `M-hv1/Titan-Library` (write) and
> `M-hv1/Titan-Gateway` (write). After expiry the "prove the token" step fails with
> HTTP 401 and the job goes red — it will not silently skip work.

## Known deviations from the gateway's behaviour

- `chapters_ready` in `ledger.json` counts **readable** chapters for catalog books,
  not audio-ready ones. Narration is retired; the gateway's shelf `startable` flag
  reads this field.
- `rights.verified` rests on Project Gutenberg's own catalog of US public-domain
  editions plus the verbatim `LICENSE.txt` travelling beside the text. The CSV carries
  no per-book rights field, so no Gutendex rights call is made.
- `download_count` is `0` for catalog books: `pg_catalog.csv.gz` does not carry it.
- The first chapter absorbs the title page and any front matter, exactly as the
  gateway's chapteriser does. Spans are lossless slices of the original text.
- The EAD index builder still enumerates `books/gutendex` **one level deep**
  (`american-english-daily-kmp/scripts/build_catalog_index.py:33` and `:287-292`), so it
  finds the 63 legacy books and skips the sharded ones with a warning. Teaching it
  two-level enumeration is a separate, deferred change — the books are stored and
  verified either way.