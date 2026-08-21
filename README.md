# CMS Hospitals Dataset Pipeline

A Python script that discovers every dataset tagged with the `"Hospitals"` theme in the [CMS Provider Data metastore API](https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items), downloads each one as a CSV, and rewrites its column headers into `snake_case`. It tracks each dataset's `modified` date locally so that re-running the script only downloads datasets that actually changed since the last successful run — everything else is skipped.

## Project purpose

CMS exposes its provider-data catalog (facility ratings, quality measures, survey results, etc.) through a metastore API rather than a single bulk download. This project:

- Discovers which datasets in that catalog are tagged `Hospitals`
- Downloads each one as a CSV and normalizes its headers
- Tracks a `modified` value per dataset so a repeat run skips anything unchanged
- Is meant to be re-run on a schedule (e.g. daily) without redoing work it already did

At the time of testing (2026-08-21), the API returned 236 total datasets, 75 of which were tagged `Hospitals`. Those numbers are a snapshot of the live catalog on that date, not a fixed property of the API — a later run may see a different total as CMS adds, removes, or re-tags datasets.

## Key features

- **CMS API integration** — single GET request to the metastore `dataset/items` endpoint
- **Hospital dataset discovery** — filters on the dataset's `theme` list for the exact value `"Hospitals"`
- **Incremental processing** — compares each dataset's current `modified` value against what's stored locally; unchanged datasets are skipped
- **Idempotent runs** — re-running with no upstream changes downloads nothing
- **Parallel downloads** — `ThreadPoolExecutor`, worker count configurable
- **CSV sanity checks** — rejects empty responses and responses that don't look like CSV (e.g. an HTML error page returned with a 200 status)
- **snake_case column normalization** — handles spaces, apostrophes, hyphens, slashes, asterisks, parentheses, and camelCase, with de-duplication of resulting name collisions
- **Timeout + retry on every HTTP request** — 30s timeout, up to 3 retries with backoff on 429/500/502/503/504
- **Per-dataset error isolation** — one dataset failing doesn't stop the others, and its metadata isn't updated so it's retried next run
- **Logging** — timestamped log lines to the console via Python's `logging` module
- **Configurable output directory, metadata file location, and worker count** — via CLI flags

Scheduling (cron / Windows Task Scheduler) is **not implemented in the script** — see [Scheduling](#scheduling) below for what that means in practice.

## Architecture / data flow

```
CMS Provider Data metastore API
        |
        v
Fetch full dataset catalog (single GET, JSON array)
        |
        v
Filter to theme == "Hospitals"
        |
        v
For each Hospitals dataset: read its "modified" value
        |
        v
Compare against stored metadata.json
        |
        +---- unchanged ----> log "unchanged - skipping", do nothing
        |
        v
   changed / new datasets
        |
        v
Parallel download (ThreadPoolExecutor, one thread per dataset)
        |
        v
Per-dataset: empty-file check + "does this look like CSV" check
        |
        v
Rewrite header row to snake_case, stream data rows through unchanged
        |
        v
Atomically move finished file into the output directory
        |
        v
Collect all results back on the main thread
        |
        v
Update metadata.json (only for datasets that succeeded), write atomically
```

Stage by stage:

1. **Fetch catalog** (`fetch_dataset_items`) — one `GET` to `METASTORE_URL`, with the shared session's timeout/retry policy applied. Raises if the response isn't a JSON array.
2. **Filter to Hospitals** (`filter_by_theme`) — keeps items where `"Hospitals"` appears in that item's `theme` list.
3. **Decide what changed** (`select_changed_datasets`) — for each Hospitals item, compares its current `modified` value to the value stored for that `identifier` in `metadata.json`. No stored entry, or a different value, means it needs processing; an identical value means it's logged as unchanged and skipped.
4. **Parallel download + transform** (`process_changed_datasets` → `download_and_process`) — submits one `download_and_process` call per changed dataset to a `ThreadPoolExecutor`. Each call downloads the dataset's CSV distribution, sanity-checks it, rewrites its header row to snake_case, and writes the result.
5. **Update metadata** (`update_metadata` / `save_metadata`) — after all threads finish, the main thread updates `metadata.json` with the new `modified` value **only** for datasets whose download+transform succeeded, then writes the file atomically.

## Project structure

```
cms_pipeline/
├── pipeline.py             # the entire pipeline: discovery, filtering, download,
│                           #  transform, metadata handling, CLI
├── requirements.txt         # third-party dependency: requests
├── README.md                 # this file
├── DESIGN.md                 # requirement mapping, design rationale, self code-review,
│                           #  interview Q&A, manual test/verification notes
├── metadata.example.json     # sample metadata.json structure (see below)
├── .gitignore                 # excludes .venv/, __pycache__/, *.pyc, output/, metadata.json
└── output/                     # generated at runtime, not committed
```

- **`pipeline.py`** — everything the pipeline does: fetching the catalog, filtering by theme, comparing metadata, downloading/transforming CSVs in parallel, and writing metadata back out. Also defines the CLI entry point.
- **`requirements.txt`** — the one third-party dependency, `requests`.
- **`README.md`** — what you're reading: what the project does, how to run it, and how metadata works.
- **`DESIGN.md`** — the deeper write-up: a requirement-by-requirement mapping, why specific design choices were made (threads vs. processes, JSON vs. a database, etc.), a self-review of the code, and notes from manually running the pipeline against the live API (first run, no-change run, single-changed-dataset run, and forced failure cases).
- **`metadata.example.json`** — a small, fabricated example (two placeholder datasets) showing the exact shape of `metadata.json` without shipping a real, 75-entry run log.
- **`.gitignore`** — keeps the local virtualenv, bytecode cache, generated `output/` CSVs, and the real runtime `metadata.json` out of version control.
- **`output/`** — created on first run; holds the processed CSVs. Not committed (see `.gitignore`).

## Technologies used

- **Python** — the whole pipeline; no other language involved
- **`requests`** — HTTP calls to the metastore API and to each CSV's `downloadURL`; used specifically for its `Session` + `HTTPAdapter` + `urllib3.util.retry.Retry` combination, which gives connection pooling and retry/backoff without hand-rolling it on top of `urllib.request`
- **Python standard library:**
  - `concurrent.futures` (`ThreadPoolExecutor`, `as_completed`) — parallel downloads
  - `csv` — streaming read/write of the dataset files
  - `json` — reading/writing `metadata.json`
  - `pathlib` — all file and directory handling
  - `re` — the snake_case conversion
  - `logging` — run output
  - `argparse` — CLI flags
  - `dataclasses` (`ProcessResult`) — structured return value from each download/transform call
  - `tempfile`, `os` — atomic file writes (write to a temp file, then `os.replace`)
  - `datetime` — timestamps written into metadata
- **CMS Provider Data metastore API** — the data source (`https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items`)

No database, no task queue, no cloud SDKs, no web framework — none of those are used by the code, so none are listed as dependencies.

## Requirements

- **Python:** developed and tested with Python 3.14.6 on Windows. The code has no OS-specific calls — file handling goes through `pathlib`/`os.replace`/`tempfile.mkstemp`, all cross-platform — so it should run unmodified on Linux/macOS, though only the Windows run has actually been verified.
- **Operating system:** no Windows- or Linux-specific code paths; runs as a plain script, not inside any specific platform's runtime.
- **Internet access:** required — the script calls the live CMS API and downloads CSVs directly from `data.cms.gov`. There is no offline/cached mode.
- **Third-party dependencies:** exactly what's in `requirements.txt`:

  ```
  requests>=2.31
  ```

  `requests` pulls in `urllib3`, which is where the retry policy (`urllib3.util.retry.Retry`) used by `pipeline.py` comes from.

## Installation

```bash
git clone <repository-url>
cd <repository-folder>/cms_pipeline

python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate

pip install -r requirements.txt
```

## Usage

Run with defaults:

```bash
python pipeline.py
```

This fetches the catalog, filters to `Hospitals`, downloads/processes anything new or changed into `output/`, and writes `metadata.json` in the current directory.

### CLI arguments

These are the only options the script accepts (from `parse_args` in `pipeline.py`):

| Flag | Default | Meaning |
|---|---|---|
| `--output-dir` | `output` | Directory processed CSVs are written to |
| `--metadata-file` | `metadata.json` | Path to the metadata tracking file |
| `--max-workers` | `8` | Number of threads in the `ThreadPoolExecutor` used for parallel downloads |
| `--verbose` | off | Switches logging from `INFO` to `DEBUG` |

Example:

```bash
python pipeline.py --output-dir data/hospitals --metadata-file data/hospitals_metadata.json --max-workers 4
```

There is no config file, environment-variable configuration, or interactive prompt — all configuration is these four flags.

### Exit code

`main()` returns `0` if every processed dataset succeeded (or none needed processing), and `1` if the catalog fetch failed outright or at least one dataset failed to download/process. This is meant to be checked by whatever schedules the script (see below).

## Metadata behavior

`metadata.json` is how the pipeline knows what it already has, so it doesn't need to re-download unchanged datasets every run.

**Structure** (see `metadata.example.json` for a runnable sample):

```json
{
  "datasets": {
    "<dataset identifier>": {
      "title": "...",
      "modified": "<the CMS 'modified' date last seen for this dataset>",
      "download_url": "...",
      "output_file": "<path to the processed CSV>",
      "row_count": 0,
      "last_processed_at": "<UTC ISO timestamp of when this entry was written>"
    }
  },
  "last_run_at": "<UTC ISO timestamp of the most recent run, successful or not>"
}
```

**How it's created:** if `--metadata-file` doesn't exist yet, `load_metadata` returns `{"datasets": {}}` in memory — every Hospitals dataset then has no stored entry and is treated as needing processing. The file itself is only written to disk at the end of the run, via `save_metadata`.

**How it's updated:** after all datasets for a run have been downloaded/processed (or attempted), `update_metadata` walks the results and, **only for datasets that succeeded**, writes a fresh entry keyed by `identifier` with the new `modified` value, the download URL, the output path, the row count, and a `last_processed_at` timestamp. A dataset that failed keeps whatever entry (or lack of one) it had before, so it's picked up again — and retried — on the next run. `last_run_at` is updated unconditionally, whether or not anything succeeded.

**How the write is done safely:** `save_metadata` writes to a temp file in the same directory and then `os.replace`s it over the real file, so a crash mid-write can't leave `metadata.json` truncated or corrupted. If the existing `metadata.json` can't be parsed as JSON (or can't be read at all), `load_metadata` logs a warning and continues with an empty `{"datasets": {}}` rather than crashing the run — the practical effect is the same as a first run.

**What `metadata.example.json` is, and why it's committed:** it's a small, fabricated two-dataset example showing the exact shape above, with placeholder identifiers and titles — not a copy of a real run. It's committed so anyone cloning the repo can see the metadata structure without having to run the pipeline first.

**Why the real `metadata.json` is *not* committed:** it's generated output, not source — it's rewritten by every run, will differ between machines and points in time, and (in this project's case) currently tracks 75 real dataset entries, which is exactly the kind of noisy, regenerable file `.gitignore` exists to keep out of version control.

## Output

Processed CSVs are written to `--output-dir` (default `output/`), one file per successfully processed dataset, named:

```
{dataset_identifier}__{original_filename_from_download_url}
```

for example: `4jcv-atw7__ASC_Facility.csv`. Only the header row is rewritten (to snake_case); data rows are copied through unchanged. During processing, temporary files (`.{filename}.download.tmp` and `.{filename}.processed.tmp`) are created in the same output directory and removed once the run finishes — you may see them appear briefly, or left behind if the process is killed mid-download, but a normal run cleans them up.

## Error handling, retries, and timeouts

- Every HTTP request (the catalog fetch and every CSV download) uses `REQUEST_TIMEOUT = 30` seconds.
- Both the catalog fetch and CSV downloads go through the same `requests.Session`, which has an `HTTPAdapter` configured with a `urllib3` `Retry`: up to 3 retries (4 attempts total) with a 1.5 backoff factor, on HTTP status codes 429, 500, 502, 503, and 504.
- If the catalog fetch itself fails (network error, non-2xx after retries, or a response that isn't a JSON array), the whole run logs an error and exits with status `1` — there's nothing to process without a catalog.
- Each dataset's download/transform is wrapped in its own `try/except`, so one dataset's failure (bad URL, HTTP error, empty response, non-CSV content, unparseable CSV) is logged and recorded as failed **without stopping the other datasets** running in parallel.
- Two specific content checks run before a downloaded file is treated as valid CSV: it must be non-empty, and its first few bytes must not look like the start of HTML or JSON (`_sanity_check_is_csv`) — this catches a 200-OK response that's actually an error/redirect page rather than real CSV data.
- A dataset that fails is **not** written into `metadata.json` for that run, so it looks "changed" again (or "new," if it never had an entry) on the next run and is retried automatically — there's no separate retry queue or backoff-between-runs mechanism beyond that.

## Concurrency

`process_changed_datasets` submits one `download_and_process` call per changed dataset to a `ThreadPoolExecutor` sized by `--max-workers` (default 8), and collects results via `as_completed`. Threads are used because the work is I/O-bound (waiting on HTTP responses) rather than CPU-bound, so they can run concurrently despite the GIL.

No worker thread touches `metadata.json` or any other shared, mutable state — each one returns a self-contained `ProcessResult`. The main thread waits for every thread to finish, then calls `update_metadata` once, after the fact, single-threaded. That's what avoids needing a lock: there's no shared state being written concurrently in the first place.

## Scheduling

The script itself does not schedule anything — there's no built-in loop, timer, or cron integration in `pipeline.py`. It runs once and exits. Running it "daily" means invoking `python pipeline.py` from an external scheduler:

- **Windows:** Task Scheduler, running `python pipeline.py` on whatever cadence is configured
- **Linux/macOS:** a `cron` entry calling the same command

Because the pipeline is idempotent (a run with no upstream changes does nothing), it's safe for the scheduler to invoke it repeatedly, or to re-run it manually after a failure, without extra coordination.

## Reproducing the manual verification runs

There's no automated test suite in this project (no `pytest`/`unittest` files) — verification so far has been done by actually running `pipeline.py` against the live API and inspecting the results. `DESIGN.md` documents exactly what was run and what was observed: a first run (fresh `metadata.json`, all Hospitals datasets processed), a second run with no upstream changes (everything skipped), a run with one dataset's stored `modified` value manually edited to look stale (only that dataset reprocessed), and a set of targeted failure cases (a broken download URL, a dataset with no CSV distribution, and an HTML page returned in place of a CSV). See `DESIGN.md` → "Testing plan" for the full list and the actual log output from those runs.

## Design decisions (summary)

The full rationale, including alternatives considered, is in `DESIGN.md`. Short version:

- **Threads, not processes or asyncio** — the work is I/O-bound; `ThreadPoolExecutor` gets the concurrency benefit without process overhead or an async rewrite.
- **JSON, not a database, for metadata** — one writer, ~75 rows, a single key-lookup access pattern; a database would add schema and connection-lifecycle overhead for no functional benefit at this scale.
- **`modified` (not `last_modified`)** — the CMS metastore API's actual field name for a dataset's modification date; verified against the live response rather than assumed.
- **`theme` matched as a list membership check, not a substring match** — the field is a list of strings, and a dataset can legitimately carry more than one theme.
