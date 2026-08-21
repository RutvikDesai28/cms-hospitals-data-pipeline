"""CMS Hospitals dataset pipeline.

Downloads CSV datasets tagged with the "Hospitals" theme from the CMS
provider-data metastore (data.cms.gov), normalizes their column headers to
snake_case, and tracks per-dataset modification metadata so repeat runs
only re-download datasets that actually changed upstream.

Usage:
    python pipeline.py
    python pipeline.py --max-workers 4 --output-dir output --metadata-file metadata.json
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
TARGET_THEME = "Hospitals"

DEFAULT_OUTPUT_DIR = Path("output")
DEFAULT_METADATA_FILE = Path("metadata.json")
DEFAULT_MAX_WORKERS = 8
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
BACKOFF_FACTOR = 1.5

logger = logging.getLogger("cms_pipeline")


def configure_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def build_session(max_workers: int) -> requests.Session:
    """Session shared by all worker threads. requests.Session is safe to
    share across threads for simple request/response calls; the retry
    policy below is applied uniformly to the catalog fetch and every CSV
    download."""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=max(max_workers, 1))
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# --------------------------------------------------------------------------
# Discovery
# --------------------------------------------------------------------------

def fetch_dataset_items(session: requests.Session) -> list[dict[str, Any]]:
    logger.info("Fetching dataset catalog from %s", METASTORE_URL)
    response = session.get(METASTORE_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    items = response.json()
    if not isinstance(items, list):
        raise ValueError("Unexpected metastore response shape: expected a JSON array of dataset items")
    logger.info("Catalog returned %d datasets", len(items))
    return items


def filter_by_theme(items: list[dict[str, Any]], theme: str) -> list[dict[str, Any]]:
    matches = [item for item in items if theme in (item.get("theme") or [])]
    logger.info("Found %d dataset(s) tagged with theme %r", len(matches), theme)
    return matches


# --------------------------------------------------------------------------
# Metadata store (JSON) - tracks the "modified" value we last processed
# for each dataset identifier.
# --------------------------------------------------------------------------

def load_metadata(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"datasets": {}}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Metadata file %s is unreadable (%s) - starting fresh", path, exc)
        return {"datasets": {}}
    data.setdefault("datasets", {})
    return data


def save_metadata(path: Path, metadata: dict[str, Any]) -> None:
    """Atomic write: build the new file next to the target, then rename
    over it. A crash mid-write never leaves metadata.json truncated."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=".metadata_", suffix=".tmp")
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, sort_keys=True)
        os.replace(tmp_name, path)
    except Exception:
        Path(tmp_name).unlink(missing_ok=True)
        raise


def select_changed_datasets(
    items: list[dict[str, Any]], metadata: dict[str, Any]
) -> list[dict[str, Any]]:
    known = metadata.get("datasets", {})
    changed = []
    for item in items:
        identifier = item.get("identifier")
        current_modified = item.get("modified")
        stored = known.get(identifier)
        if stored is None or stored.get("modified") != current_modified:
            changed.append(item)
        else:
            logger.info("Dataset %s (%s) unchanged - skipping", identifier, item.get("title"))
    logger.info("%d of %d dataset(s) changed and need processing", len(changed), len(items))
    return changed


# --------------------------------------------------------------------------
# Header normalization
# --------------------------------------------------------------------------

_APOSTROPHES = ("'", "’")
_CAMEL_BOUNDARY = re.compile(r"([a-z0-9])([A-Z])")
_NON_ALNUM_RUN = re.compile(r"[^0-9a-zA-Z]+")


def to_snake_case(name: str) -> str:
    """Convert a raw CSV header into snake_case.

    Handles spaces, punctuation (including apostrophes, parentheses,
    hyphens, slashes, asterisks), and camelCase word boundaries. Runs of
    separators collapse to a single underscore.
    """
    text = name.strip()
    for ch in _APOSTROPHES:
        text = text.replace(ch, "")
    text = _CAMEL_BOUNDARY.sub(r"\1_\2", text)
    text = _NON_ALNUM_RUN.sub("_", text)
    text = text.strip("_").lower()
    return text or "column"


def normalize_headers(headers: list[str]) -> list[str]:
    """snake_case every header, de-duplicating collisions (e.g. two
    source columns that both normalize to "rate" become "rate" / "rate_2")."""
    seen: dict[str, int] = {}
    result = []
    for h in headers:
        base = to_snake_case(h)
        count = seen.get(base, 0)
        seen[base] = count + 1
        result.append(base if count == 0 else f"{base}_{count + 1}")
    return result


# --------------------------------------------------------------------------
# Download + transform
# --------------------------------------------------------------------------

@dataclass
class ProcessResult:
    identifier: str
    title: str
    modified: str | None
    download_url: str
    success: bool
    output_path: str | None = None
    row_count: int | None = None
    error: str | None = None


def _safe_filename(identifier: str, download_url: str) -> str:
    original_name = download_url.rsplit("/", 1)[-1] or "data.csv"
    return f"{identifier}__{original_name}"


def _sanity_check_is_csv(path: Path) -> None:
    """Guard against a 200 OK response that is actually an HTML error/
    redirect page (csv.reader would otherwise happily "parse" it into
    garbage columns instead of failing)."""
    with path.open("rb") as f:
        head = f.read(256).lstrip()
    if head[:1] in (b"<", b"{", b"["):
        raise ValueError("Response does not look like CSV content (got HTML/JSON instead)")


def _transform_csv(src: Path, dst: Path) -> int:
    """Stream the raw CSV, rewrite only the header row, and copy data
    rows through unchanged. Streaming keeps memory flat regardless of
    file size instead of loading the whole dataset at once."""
    with src.open("r", encoding="utf-8-sig", newline="") as fin:
        reader = csv.reader(fin)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("CSV file has no header row")

        new_header = normalize_headers(header)
        row_count = 0
        with dst.open("w", encoding="utf-8", newline="") as fout:
            writer = csv.writer(fout)
            writer.writerow(new_header)
            for row in reader:
                if not row:
                    continue
                writer.writerow(row)
                row_count += 1
    return row_count


def download_and_process(
    session: requests.Session, item: dict[str, Any], output_dir: Path
) -> ProcessResult:
    identifier = item.get("identifier", "unknown")
    title = item.get("title", "")
    modified = item.get("modified")
    distributions = item.get("distribution") or []

    csv_dist = next((d for d in distributions if d.get("mediaType") == "text/csv"), None)
    if csv_dist is None or not csv_dist.get("downloadURL"):
        logger.error("Dataset %s has no CSV distribution - skipping", identifier)
        return ProcessResult(identifier, title, modified, "", False, error="No CSV distribution found")

    download_url = csv_dist["downloadURL"]
    filename = _safe_filename(identifier, download_url)
    final_path = output_dir / filename
    tmp_raw = output_dir / f".{filename}.download.tmp"
    tmp_out = output_dir / f".{filename}.processed.tmp"

    try:
        logger.info("Downloading dataset %s (%s)", identifier, title)
        with session.get(download_url, timeout=REQUEST_TIMEOUT, stream=True) as resp:
            resp.raise_for_status()
            with tmp_raw.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        f.write(chunk)

        if tmp_raw.stat().st_size == 0:
            raise ValueError("Downloaded file is empty")
        _sanity_check_is_csv(tmp_raw)

        row_count = _transform_csv(tmp_raw, tmp_out)
        if row_count == 0:
            logger.warning("Dataset %s has a header row but no data rows", identifier)

        os.replace(tmp_out, final_path)  # atomic: readers never see a half-written file
        logger.info("Successfully processed %s -> %s (%d rows)", identifier, final_path.name, row_count)
        return ProcessResult(identifier, title, modified, download_url, True, str(final_path), row_count)

    except Exception as exc:
        logger.error("Failed to process dataset %s: %s", identifier, exc)
        return ProcessResult(identifier, title, modified, download_url, False, error=str(exc))
    finally:
        tmp_raw.unlink(missing_ok=True)
        tmp_out.unlink(missing_ok=True)


def process_changed_datasets(
    session: requests.Session,
    items: list[dict[str, Any]],
    output_dir: Path,
    max_workers: int,
) -> list[ProcessResult]:
    """Fan out downloads across threads (network I/O bound, so threads -
    not processes - are the right tool). Each worker only returns a
    ProcessResult; nothing touches shared metadata from inside a thread,
    which is what keeps the metadata update below race-free."""
    if not items:
        return []
    results: list[ProcessResult] = []
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="worker") as pool:
        futures = {pool.submit(download_and_process, session, item, output_dir): item for item in items}
        for future in as_completed(futures):
            results.append(future.result())
    return results


def update_metadata(metadata: dict[str, Any], results: list[ProcessResult]) -> dict[str, Any]:
    """Single-threaded, post-hoc metadata update. Only successful results
    move the stored 'modified' watermark forward, so a failed dataset is
    retried on the next run instead of being silently marked done."""
    now = datetime.now(timezone.utc).isoformat()
    datasets = metadata.setdefault("datasets", {})
    for result in results:
        if not result.success:
            continue
        datasets[result.identifier] = {
            "title": result.title,
            "modified": result.modified,
            "download_url": result.download_url,
            "output_file": result.output_path,
            "row_count": result.row_count,
            "last_processed_at": now,
        }
    metadata["last_run_at"] = now
    return metadata


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def run(output_dir: Path, metadata_path: Path, max_workers: int) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    session = build_session(max_workers)
    metadata = load_metadata(metadata_path)

    try:
        items = fetch_dataset_items(session)
    except (requests.RequestException, ValueError) as exc:
        logger.error("Could not fetch dataset catalog, aborting run: %s", exc)
        return 1

    hospital_items = filter_by_theme(items, TARGET_THEME)
    changed_items = select_changed_datasets(hospital_items, metadata)

    results = process_changed_datasets(session, changed_items, output_dir, max_workers)

    succeeded = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    skipped = len(hospital_items) - len(changed_items)

    metadata = update_metadata(metadata, results)
    save_metadata(metadata_path, metadata)

    logger.info(
        "Run complete: %d unchanged/skipped, %d succeeded, %d failed",
        skipped, len(succeeded), len(failed),
    )
    for r in failed:
        logger.error("Dataset %s (%s) failed: %s", r.identifier, r.title, r.error)

    return 1 if failed else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download CMS Hospitals-theme datasets incrementally.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--metadata-file", type=Path, default=DEFAULT_METADATA_FILE)
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)
    return run(args.output_dir, args.metadata_file, args.max_workers)


if __name__ == "__main__":
    sys.exit(main())
