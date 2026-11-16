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
- A batch is capped by `--commit-files` (default 2500), not by a book count, because
  chapter counts vary by two orders of magnitude. A book that would overflow the cap
  is carried to the next commit **without being downloaded again**.
- `Budget.allow()` stops on `--max-commits-per-run` (default 500) and paces commits by
  `--min-commit-interval` (default 5s). A low `RateLimit` window is **not** a stop: the
  run sleeps `reset_in + 5` seconds inside itself and keeps pumping toward the cap. It
  only gives up on a low window when the sleep would cross the run's deadline
  (`max_minutes - reserve_minutes`).
- A batch only starts if `elapsed + last_batch_seconds + reserve_minutes <= max_minutes`.

### Smart backoff

Only a refused `create_commit` parks the pipeline. On a 429 / rate-limit / quota /
retry-after refusal the batch is rolled back and `_worker/backoff.json` is written with

```
not_before_epoch = max(next_utc_midnight, now + reset_in + 60)
next_utc_midnight = ((int(time.time()) // 86400) + 1) * 86400
```

That marker commit is the **one last request** — the report afterwards reuses the
24h commit count taken before it, so nothing else reaches the hub until the new UTC
day. Every later run reads the marker first and exits immediately with
`waiting_for_backoff: true`, sending zero requests. The marker rides with the previous
run's verified `last_commit` expectations (`prime_last_commit`), so a refusal on the
first batch cannot leave `--verify-last` with nothing to check.

The park is a planned stop, so the job exits **0 (green)** with the reason kept in
`refused`, `batches[-1].parked_until_utc` and `commit_budget.stopped_because`. If the
marker itself cannot be written, the run has no guaranteed wait: it records
`marker_write_failed` and exits **1**. A structural refusal also exits 1 — measured on
2026-11-16 at 11:20:19Z, when the hub answered `500 Internal Server Error` for
`POST /commit/main`: the batch rolled back, no marker was written, and the run stopped
red. That is the correct shape: a hub outage is not a quota, and waiting until tomorrow
would hide it.

## Index self-heal

A batch is written into `_worker/library_index.json` as `uploaded_unverified` *before*
`remote_check` promotes it, so the promotion only ever ships in the next commit — and a
later run never re-examines books it did not build itself. Without this, the last batch
of every run stays `uploaded_unverified` forever (50 such rows survived from
2026-11-15 until the heal landed). `heal_stale_rows()` asks the shelf once, with the
same `existing_books()` probe used for candidates, and settles the rows whose files are
really there. Rows whose files are **not** there are counted in
`index_still_unverified` and left alone: they still sit in `ledger.json`, so retrying
them would mean rewriting the ledger, which is a different change.

## Verification

`*.txt` and `*.json` are plain git blobs in this dataset; `*.jpg` and `*.parquet` are
LFS (see its `.gitattributes`). So `remote_check()` compares `blob_id` (git sha1) for
text and `lfs.sha256` + size for images, and falls back to re-fetching CDN bytes when
a Xet-backed object exposes no LFS oid. `--verify-last` re-reads the checkpoint and
verifies independently of the run that wrote it.

`--self-test` is a network-free positive control: **41 assertions**. A synthetic
Gutenberg fixture with a contents listing, a dedication and three real chapters must
yield exactly three chapters with the right titles, plus known-answer vectors for the
git blob sha1, the path guard and the strict `en` / `Text` filter. A stub api and a
frozen clock then prove the backoff contract: the park lands on the next UTC midnight,
the report afterwards sends no further request, a run before the marker exits without
one request, a run after it proceeds, a low window waits inside the run, a low window
near the deadline stops instead, and the exit code is green for a marked park but red
for an unmarked refusal. Three more checks pin the state bookkeeping: a refusal on the
first batch must leave the previous run's `last_commit` expectations intact, a stale
`uploaded_unverified` row whose files are on the shelf gets settled and one whose files
are *not* stays untouched, and `run()` must actually call both helpers.

Hash equality proves the hub stores what the runner wrote, not that a reader can fetch
it. The `audit the public shelf without a token` step closes that layer: it pulls
`ledger.json`, `catalog.parquet`, `_worker/library_index.json` and one recent book's
`book.json`, `ch001_en.txt` and `cover.jpg` over the public CDN **anonymously**, checks
the catalog schema and row count against the ledger, requires the full field set of
every entry the index marks `origin=catalog`, and requires a positive `word_count` on
every chapter of the sampled book — the number the client sums. Gateway-era entries are
reported as INFO rather than failed, because the client reads their word counts from
`book.json`, never from the ledger.

## Heartbeat

`heartbeat.yml` runs at `:27` every hour and asks one question of the shelf, not of the
clock: *has `M-hv1/Titan-Library` taken a commit within `stale_minutes` (default 180), is
an ingest run already alive, and is the worker parked?* It dispatches `ingest.yml` only
when all three say go, in that order, so a deliberate park until 00:00 UTC always wins.
It reads three things — `_worker/backoff.json`, the workflow run list, and the dataset's
newest commit date (parsed with `calendar.timegm`, because the hub's stamp is UTC and
`time.mktime` would silently use the runner's zone) — and writes nothing.

Two deliberate sharp edges:

- **`GH_DISPATCH_TOKEN` must be a PAT**, not the repository token. `GITHUB_TOKEN`
  returns `204 No Content` to a `workflow_dispatch` call and creates **no run at all**, so
  the heartbeat ignores the HTTP answer and waits for a run id newer than the newest one
  it saw before dispatching. Missing secret or silent dispatch both exit **1**.
- **The heartbeat is itself a scheduled workflow**, so it is exposed to the same
  best-effort dropping it exists to heal. The gain is that a missed window costs an hour
  instead of four; it is damage control, not immunity.

On 2026-11-16 GitHub created zero `schedule` run records across three eligible ingest
windows (03:17 under the old cron, then 04:17 and 08:17 under `17 */4 * * *`) while the
Actions status component read `operational`, the workflow was `active`, the copy of the
file on `main` carried the right cron, and the run queue was empty.

## Running

```bash
gh workflow run ingest.yml -f dry_run=true          # schema + cleaner, zero commits
gh workflow run ingest.yml -f batch_size=25         # small remote test
gh workflow run ingest.yml -f batch_size=100        # one scheduled-sized pump
gh workflow run heartbeat.yml -f state_only=true    # show the verdict, dispatch nothing
gh workflow run tts-switch.yml -f state=off         # retire gateway narration
gh workflow run tts-switch.yml -f state=on          # restore it
```

The cron (`17 */4 * * *`) uses `batch_size=100`, `timeout-minutes: 340`,
`--max-minutes 320` and `--reserve-minutes 10`. A run that uses its full window queues
the next one through `concurrency: titan-library-ingest` (`cancel-in-progress: false`),
so the effective cadence is about 5h20m rather than 4h. Set the repository variable
`INGEST_DRY_RUN=true` to make scheduled runs commit nothing.

Measured on 2026-11-16 at these settings: **2.5 minutes per commit and 36 books per
minute** (25 commits, 2,259 books, 58,781 files, 948 MB in 62.6 minutes, zero hash
mismatches). At that rate the 320-minute budget would allow ~128 commits, but the scan
window — `--batch-size × 40`, i.e. 4,000 candidates — closes the run near 110 minutes.
**The scan is the binding constraint, not the clock**; widen it with `--scan` if a
single run should ever approach the 500-commit ceiling.

## Secret

`HF_TOKEN` must be able to **write the dataset** and **administer the Space**
(`add_space_variable` + `restart_space`). Set it with:

```bash
gh secret set HF_TOKEN --repo aljokar-m-1998/titan-library-worker
```

`GH_DISPATCH_TOKEN` belongs to `heartbeat.yml` only and must be a **personal access
token with `Actions: Read and write` on this repository alone** — the repository's own
`GITHUB_TOKEN` cannot trigger workflows (see Heartbeat). Set it before the heartbeat's
first scheduled fire, or that run exits red with `GH_DISPATCH_TOKEN is not set`:

```bash
gh secret set GH_DISPATCH_TOKEN --repo aljokar-m-1998/titan-library-worker
```

Neither token is ever printed: the heartbeat puts its token into one `Authorization`
header and nowhere else, and no `secrets.GITHUB_TOKEN` expression appears in any
workflow file.

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