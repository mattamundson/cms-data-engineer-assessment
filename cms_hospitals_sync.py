"""
CMS Provider Data Catalog — Hospitals Theme Sync

Downloads all CSV datasets from the CMS provider-data metastore that are
tagged with theme "Hospitals", converts column headers to snake_case, and
writes them to ./output/{identifier}.csv.

Designed to run daily (via cron / Task Scheduler). On each run it diffs the
API-reported `modified` date against per-dataset state in ./state.json and
only re-downloads datasets that have changed since the last successful run.

Idempotent: safe to re-run any number of times. Delete state.json for a
clean rebuild. Delete a single key to force one dataset to re-pull.

Usage:
    python cms_hospitals_sync.py

Scheduling examples:
    Linux cron (daily at 02:00):
        0 2 * * * /usr/bin/python3 /path/to/cms_hospitals_sync.py >> /var/log/cms_sync.log 2>&1
    Windows Task Scheduler:
        Action: Start a program
        Program: python.exe
        Arguments: C:\\path\\to\\cms_hospitals_sync.py

Exit codes:
    0 = all datasets succeeded (or unchanged / had no CSV distribution)
    1 = one or more downloads failed (see logs)
"""

import csv
import json
import logging
import os
import re
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
TARGET_THEME = "Hospitals"
OUTPUT_DIR = Path("output")
STATE_PATH = Path("state.json")
MAX_WORKERS = 4  # CMS rate-limits aggressive parallelism; 4 is a polite ceiling
HTTP_TIMEOUT_DISCOVERY = 60
HTTP_TIMEOUT_DOWNLOAD = 300


def build_session() -> requests.Session:
    """Session configured with retries + backoff for transient HTTP failures.

    CMS occasionally returns 503 Service Unavailable on its CDN-backed download
    URLs under parallel load. Retrying with exponential backoff handles this
    cleanly so a single transient blip doesn't cost us a dataset for the day.

    Retry triggers: connection errors, read errors, and HTTP 429/500/502/503/504.
    Total of 5 attempts (1 initial + 4 retries) with backoff 1s, 2s, 4s, 8s.
    """
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


SESSION = build_session()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("cms_hospitals_sync")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def snake_case(s: str) -> str:
    """Convert an arbitrary header string to snake_case.

    Lowercases, then collapses any run of non-alphanumeric characters
    (spaces, punctuation, apostrophes, parens, etc.) to a single underscore.
    Strips leading and trailing underscores.

        "Patients' rating of the facility linear mean score"
        -> "patients_rating_of_the_facility_linear_mean_score"
    """
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        logger.warning("state file %s is corrupt (%s); starting from empty state", path, e)
        return {}


def save_state(path: Path, state: dict) -> None:
    """Atomically write state by writing to a temp file then os.replace."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def fetch_metastore() -> list[dict]:
    logger.info("fetching metastore: %s", METASTORE_URL)
    resp = SESSION.get(METASTORE_URL, timeout=HTTP_TIMEOUT_DISCOVERY)
    resp.raise_for_status()
    return resp.json()


def filter_by_theme(datasets: list[dict], theme: str) -> list[dict]:
    """Exact, case-insensitive match against the theme array."""
    needle = theme.lower()
    return [d for d in datasets if any(t.lower() == needle for t in d.get("theme", []))]


def pick_csv_url(dataset: dict) -> str | None:
    """Return the first CSV download URL from the distribution list, or None."""
    for dist in dataset.get("distribution", []):
        if dist.get("mediaType") == "text/csv" and dist.get("downloadURL"):
            return dist["downloadURL"]
    return None


# -----------------------------------------------------------------------------
# Worker
# -----------------------------------------------------------------------------
def process_dataset(dataset: dict, output_dir: Path, prior_state: dict) -> dict:
    """Download (if changed) and transform a single dataset.

    Strategy:
      1. Skip cleanly if dataset has no CSV distribution.
      2. Skip if API `modified` == state `modified` AND the output file
         still exists on disk (defends against an external delete of the CSV).
      3. Stream-download raw bytes to {id}.csv.download.
      4. Stream-rewrite: read header row, snake_case each column name,
         pass all data rows through unchanged. Output to {id}.csv.tmp.
      5. os.replace(tmp -> final) -> atomic on Windows and POSIX.
      6. Return a result dict; caller updates state only on 'downloaded'.

    Any exception during steps 3-5 is caught, partial files are cleaned up,
    and an 'error' status is returned. One dataset's failure does not
    abort the rest.
    """
    identifier = dataset["identifier"]
    api_modified = dataset.get("modified")

    csv_url = pick_csv_url(dataset)
    if not csv_url:
        return {"identifier": identifier, "status": "no_csv"}

    prior = prior_state.get(identifier, {})
    final_path = output_dir / f"{identifier}.csv"
    if (
        prior.get("modified") == api_modified
        and Path(prior.get("output_file", "")).exists()
    ):
        return {"identifier": identifier, "status": "unchanged"}

    raw_path = output_dir / f"{identifier}.csv.download"
    tmp_path = output_dir / f"{identifier}.csv.tmp"

    try:
        # 1. Raw download (streamed; iter_content auto-handles gzip Content-Encoding)
        #    SESSION applies retry-with-backoff for transient 5xx and 429
        with SESSION.get(csv_url, stream=True, timeout=HTTP_TIMEOUT_DOWNLOAD) as r:
            r.raise_for_status()
            with open(raw_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)

        # 2. Stream-transform headers; pass data rows through
        #    utf-8-sig handles BOM-prefixed files transparently
        #    errors='replace' is defensive against occasional bad bytes
        with open(raw_path, "r", encoding="utf-8-sig", errors="replace", newline="") as inf, \
             open(tmp_path, "w", encoding="utf-8", newline="") as outf:
            reader = csv.reader(inf)
            writer = csv.writer(outf)
            try:
                headers = next(reader)
            except StopIteration:
                raise ValueError("CSV is empty (no header row)")
            writer.writerow([snake_case(h) for h in headers])
            for row in reader:
                writer.writerow(row)

        # 3. Atomic finalize
        os.replace(tmp_path, final_path)
        raw_path.unlink(missing_ok=True)

        return {
            "identifier": identifier,
            "status": "downloaded",
            "modified": api_modified,
            "output_file": str(final_path),
            "source_url": csv_url,
        }
    except Exception as e:
        # Clean up any partial files so a retry has a clean slate
        for p in (raw_path, tmp_path):
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass
        return {"identifier": identifier, "status": "error", "error": str(e)}


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> int:
    OUTPUT_DIR.mkdir(exist_ok=True)
    prior_state = load_state(STATE_PATH)

    all_datasets = fetch_metastore()
    logger.info("metastore returned %d total datasets", len(all_datasets))

    hospitals = filter_by_theme(all_datasets, TARGET_THEME)
    logger.info("filtered to %d datasets in theme '%s'", len(hospitals), TARGET_THEME)

    if not hospitals:
        logger.warning("no datasets matched theme '%s'; nothing to do", TARGET_THEME)
        return 0

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(process_dataset, d, OUTPUT_DIR, prior_state): d
            for d in hospitals
        }
        for fut in as_completed(futures):
            r = fut.result()
            logger.info("  %s -> %s", r["identifier"], r["status"])
            results.append(r)

    # Update state for successful downloads only; preserve prior state for
    # unchanged datasets; do NOT touch state for errors so the next run retries.
    new_state = dict(prior_state)
    run_ts = datetime.now(timezone.utc).isoformat()
    for r in results:
        if r["status"] == "downloaded":
            new_state[r["identifier"]] = {
                "modified": r["modified"],
                "output_file": r["output_file"],
                "source_url": r["source_url"],
                "last_run": run_ts,
            }
    save_state(STATE_PATH, new_state)

    # Summary
    counts = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    logger.info("summary: %s", counts)

    errors = [r for r in results if r["status"] == "error"]
    if errors:
        for e in errors:
            logger.error("FAILED %s: %s", e["identifier"], e["error"])
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
