# titan-library-worker

Cloud-only ingestion of Project Gutenberg into the Hugging Face dataset
[`M-hv1/Titan-Library`](https://huggingface.co/datasets/M-hv1/Titan-Library).

**This worker writes text, metadata and covers only.** It synthesizes, uploads and
indexes no audio. The American English Daily client narrates on device, so
server-side TTS for this dataset is retired (see `tts-switch.yml`).

The dataset itself is not audio-free, and this repository is not its only writer.
Measured on 2026-11-16: the gateway's Arabic translation worker still writes English
cast `mp3` + `words.json` into the legacy flat folders `books/gutendex/{id}/en/`
(`api/titan_translate.py:3453`, `en_q.flush(...)`). `LIBRARY_WORKER=0` gates the
gateway's *shelf* worker (`api/titan_library.py:1201`), not that one; the whole Arabic
worker is gated by `ARABIC_WORKER=0`, and setting it is the owner's decision. The two
writers cannot corrupt each other's state: `_Queue.flush` rewrites `ledger.json` only
when an op path equals `LEDGER_PATH`, and the cast queue holds cast files only, so
`ledger.json` and `_worker/*` stay this worker's alone. The sharded buckets
`books/gutendex/{0..4}/` hold **zero** mp3 — 3,813 book folders, 86,729 chapters,
1.55 GiB of text, covers and json.

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
- A batch also stops **inside** its build loop. `--batch-minutes` (default 12) caps one
  batch's fetching, and one shared clock (`FETCH_DEADLINE = max_minutes - reserve_minutes`)
  caps every fetch: `polite_get` returns `status="deadline"` rather than retrying past
  it and clamps its backoff sleep to the time left, `build_book` propagates that status
  for both the text and the cover, and the batch ships what it already has.
  `batches[-1].cut_short` and `stopped_because_batch` say which guard fired.
  The clock is armed **before the first request of the run**, so it also covers the
  catalog feed and the startup reads, not just the books.

### Why the in-batch guard exists

Measured on 2026-11-16: `www.gutenberg.org/cache/epub/` answered **HTTP 503 with
`Retry-After: 300`** for twelve consecutive books (4171–4182) from 13:16:28Z to
18:11:49Z — five attempts × 300s per URL, ~25 minutes per book. The deadline was only
checked between batches, so the run committed nothing for five hours and GitHub
cancelled it at the 340-minute timeout (18:15:18Z). A timeout cancel flushes nothing:
the checkpoint stayed at `13:14:05Z` and the orphan python process was terminated.
The public shelf audit in that same cancelled run still passed every check
(3,776 ledger entries = 3,776 catalog rows, `problems: []`), so the stall cost time,
not data.

The same day the **catalog feed itself** answered 503
(`https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv.gz`). `load_catalog` raised
`SystemExit` and the run exited 1 after burning ~25 minutes on that one URL, because the
clock used to be armed *after* the catalog fetch. Arming it right after the
`reserve_minutes` guard closes that gap: the feed now obeys the same deadline as a book.

Measured again at 22:35Z from a runner (`probe-aleph.yml`, 256 bytes per URL): the feed
answered **HTTP 503 with `Retry-After: 3600`** — an hour, not the five minutes the cap
used to allow. `polite_get` now honors the source's own `Retry-After` up to **3600s**,
still clamped to the time left before `FETCH_DEADLINE`, so a run waits the hour it was
asked for instead of knocking five times in 25 minutes. The same probe found
`aleph.gutenberg.org` unusable over HTTPS (`CERTIFICATE_VERIFY_FAILED … Hostname
mismatch, certificate is not valid for 'aleph.gutenberg.org'`) while `www.gutenberg.org`
answered with a real HTTP status from the same runner — so the mirror's failure is its
certificate, not the runner's network. The probe also reads the mirror over plain HTTP
to separate a broken certificate from a dead host; it writes nothing and changes no
fetch path.

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

## Orphan recovery

A `create_commit` can answer **500 while the commit actually lands**. The batch path
treats any exception as a refusal and rolls the books out of the in-memory index; the
final state flush then publishes an index without them, and `existing_books()` skips
them forever because their files are already on the shelf. Measured on 2026-11-16 at
`11:17:27Z`: **100 books, ids 2821–2930**, present as files but invisible to the client.

`recover_orphans()` runs at startup, **before `load_catalog`**, so a source outage cannot
block it — it never touches Gutenberg:

1. `shelf_book_folders()` enumerates the hub's own tree under `books/gutendex` in both
   shapes. A bucket name can collide with a legacy book id (`11` is both), so a folder
   holding `en` is a legacy book *and* its digit children are still bucket books.
2. Anything on the shelf but absent from **both** the ledger and the index is an orphan.
   Its row is rebuilt from its own `book.json` (title, author, subjects, chapter and word
   counts, cover), and every chapter file it names must really be in the folder.
3. `shelf_check()` re-fetches those files over the public CDN and recomputes the digests
   from the bytes — git sha1 for text/json, sha256 for LFS images — then compares them
   with what the hub recorded. Comparing the hub's `blob_id` against itself would prove
   nothing. Any mismatch aborts the recovery without committing.
4. One commit publishes the rebuilt rows, `ledger.json`, `catalog.parquet` and the resume
   state, then `remote_check()` verifies that commit. The ledger entry keeps the book's
   original `created_at` as `added_at`, and the index row carries `recovered`,
   `recovered_at` and `first_created_at`. A refusal rolls the index and the ledger back
   exactly like a batch refusal, and a quota refusal parks until the next UTC midnight.

`RECOVER_LIMIT = 500` books per run; the rest are reported as `orphans_left` and picked up
by the next run. `--dry-run` only counts them. The report carries `orphan_recovery` with
`orphans_found`, `orphans_recovered`, `files_checked`, `files_verified`, `unreadable`,
`half_present` (on the shelf and in exactly one of the two indexes) and `mismatches`.
An abort **logs its reason at the moment it happens**: a run that later dies on the
catalog feed never prints its final report, so a reason kept only in that report is lost.

## Verification

`*.txt` and `*.json` are plain git blobs in this dataset; `*.jpg` and `*.parquet` are
LFS (see its `.gitattributes`). So `remote_check()` compares `blob_id` (git sha1) for
text and `lfs.sha256` + size for images, and falls back to re-fetching CDN bytes when
a Xet-backed object exposes no LFS oid. `--verify-last` re-reads the checkpoint and
verifies independently of the run that wrote it.

`--self-test` is a network-free positive control: **68 assertions**. A synthetic
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

Ten more pin the deadline contract, on a frozen clock with a counted `urlopen` stub:
`out_of_time()` is false before the deadline and true after it; a fetch started past
the deadline returns `deadline` having made **zero** requests; the positive control
(`FETCH_DEADLINE = 0`) still fetches and returns `ok`; a 429/503 with `Retry-After: 300`
ten seconds before the deadline sleeps exactly **10s**, not 300s, and then returns
`deadline`; a `Retry-After: 3600` with time to spare sleeps the full **3600s** on every
attempt, and a `Retry-After: 7200` is capped at **3600s**; `build_book` propagates a
deadlined text fetch and a deadlined cover fetch; and `run()` must reference both
`FETCH_DEADLINE` and `out_of_time` and carry the `deadline` status in its own constants.

Eleven more pin orphan recovery and the clock's position, on a stub shelf (a fake tree,
a fake `get_paths_info` and a CDN stub) with `polite_get` replaced by a function that
**raises if it is ever called**: the enumeration sees both folder shapes and the `11`
bucket/id collision; a shelf with no orphans commits nothing and reads no bytes; the
positive control rebuilds one orphan from its own `book.json`, publishes it in exactly
one commit with the full ledger field set and zero state mismatches; the entry keeps the
book's original `created_at`; a book already in the ledger is not duplicated; matching
CDN bytes pass `shelf_check` while bytes that differ from the recorded blob id are
flagged; a refused recovery commit rolls the index and the ledger back; and three
source-order guards prove `run()` calls `recover_orphans`, wires both helpers, and arms
`FETCH_DEADLINE` **before** `load_catalog`.

Three more pin the CDN read itself. `cdn_fetch()` retries a 429 / 500 / 502 / 503 / 504
with capped backoff and returns the reason beside the bytes, a 404 is named as missing
without spending a retry, and `shelf_check()` prints that reason. Measured on 2026-11-16
in run 43944429502: **994 of 2,440** shelf files came back `unreadable over the CDN` and
recovery aborted without committing. `cdn_bytes()` swallowed the HTTP status, so a
refusal and an absent file were the same `None`; the status is now in the log line
instead of being inferred.

Two more keep that retry inside the shared clock: a read started past `FETCH_DEADLINE`
returns `deadline` having made **zero** requests, and a `Retry-After: 300` ten seconds
before the deadline sleeps exactly **10s**. Without the clamp, 994 refused reads at 120s
each would outlive the 340-minute job, and a timeout cancel flushes nothing.

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
`--max-minutes 320`, `--reserve-minutes 10` and `--batch-minutes 12`. A run that uses
its full window queues the next one through `concurrency: titan-library-ingest`
(`cancel-in-progress: false`), so the effective cadence is about 5h20m rather than 4h.
Set the repository variable `INGEST_DRY_RUN=true` to make scheduled runs commit nothing.
A `workflow_dispatch` run freezes the commit it was created from, so a fix pushed while
a run is alive only takes effect on the **next** dispatch.

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