CMS Hospitals Assessment 

Downloads all CSV datasets tagged with theme "Hospitals" from the CMS provider-data metastore, converts CSV headers to snake_case, and tracks state so daily re-runs only download what's changed.

Run

```
pip install -r requirements.txt
python cms_hospitals_sync.py
```

Outputs CSVs to `./output/{identifier}.csv` and tracks per-dataset modified dates in `./state.json`.
 Design notes

- **Incremental loads** via per-dataset `modified` comparison (not a global last-run timestamp). Self-healing: delete `state.json` for a clean rebuild; delete a single key to force one dataset to re-pull.
- **Parallelism** via `ThreadPoolExecutor` (I/O-bound work; threads are the right tool). Capped at 4 workers — CMS rate-limits aggressive parallel requests.
- **Atomic writes**: download → .download, transform → .tmp, then os.replace to final path. Same pattern for state.json
- **Streamed downloads** with `iter_content` (auto-handles gzip Content-Encoding).
- **Header transformation only**: data rows pass through unchanged via `csv.reader`/`csv.writer`, so memory use is constant regardless of file size.
- **Failure isolation**: one dataset's exception doesn't abort the rest; state only updates for successful downloads, so failures auto-retry on the next run.

Scheduling

Daily at 02:00 UTC:

**Linux cron
```
0 2 * * * /usr/bin/python3 /path/to/cms_hospitals_sync.py >> /var/log/cms_sync.log 2>&1
```

**Windows Task Scheduler** — Action: Start a program; Program: `python.exe`; Arguments: `C:\path\to\cms_hospitals_sync.py`.

xit codes

- `0` — all datasets succeeded, unchanged, or had no CSV distribution
- `1` — one or more downloads failed after retries (logged; next run retries them)

 Sample output

contains three transformed CSVs from a real run medicare_spend_national.csv,asc_quality_measures_national.csv, oas_surgiacal_centersss_.csv,  plus the resulting `state.json`. Headers are snake_case; data is byte-for-byte from CMS.
