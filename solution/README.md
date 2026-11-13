# Assignment A: Interaction Ledger & Reporting Pipeline

This folder contains the complete solution for **Assignment A (Data)** of the Agentic Universe Junior SDE Take-Home.

---

## Changes Made & Implemented Components

The solution is contained in the `solution/` directory, leaving all root and assignment source files unmodified, accompanied by the required [SUBMISSION.md](../SUBMISSION.md) and [AI_USAGE.md](../AI_USAGE.md).

### 1. New Code Modules (`solution/`)
- [`__main__.py`](__main__.py): CLI entrypoint exposing `python -m solution ingest <path>` and `python -m solution report --out <dir>` with a `--db` option (defaulting to `solution.sqlite`).
- [`crypto.py`](crypto.py): PII protection module built with Python standard library primitives:
  - Deterministic HMAC-SHA256 blind index for exact phone lookups without storing plaintext phone numbers.
  - Authenticated symmetric encryption (CTR keystream + Encrypt-then-MAC tag verified with constant-time comparison) for PII fields (`customer_name`, `phone`).
  - Phone masking utility (`+91******XXXX`).
  - Automatic key generation/loading (`AU_PII_KEY` env var or `<db>.key` companion file).
- [`normalize.py`](normalize.py): Data cleaning and schema mapping:
  - Normalizes messy headers and maps aliases to canonical schema fields.
  - Normalizes Indian mobile numbers into canonical `+91XXXXXXXXXX` (validating 10 digits starting with 6–9 and stripping valid 91/0 prefixes; rejecting landlines and bad formats).
  - Normalizes day-first dates across 4 formats (`YYYY-MM-DD`, `DD/MM/YYYY`, `DD-MM-YYYY`, `DD-Mon-YYYY`) and rejects impossible dates (e.g. Feb 31).
  - Strips currency symbols (`₹`, `Rs`, `Rs.`, `INR`) and commas, rounding decimals to 2 places.
  - Case-insensitive enum normalization.
  - Converts UTC, offset, and naive timestamps into aware Indian Standard Time (`IST`).
  - Preloads schema definitions and disposition catalogs.
- [`db.py`](db.py): SQLite database schema (WAL mode, `temp_store = MEMORY`):
  - `ingested_files`: Tracks file SHA-256 hashes to guarantee file-level idempotency on re-runs.
  - `ingest_counters`: Tracks cumulative counts for total leads, rejections, duplicates, warnings, and events.
  - `leads`: Lead storage using `phone_hmac` as a unique identity per workflow, permanent first `batch_id`, masked phone, and encrypted PII.
  - `rejections`: Audit table for rejected lead rows with zero PII.
  - `events`: Append-only interaction ledger keyed by `event_id PRIMARY KEY`.
  - `build_report_temp_tables`: Materializes `_call_phone` (linking phone-less `disposition.override` events via `call_id`) and `_calls` (deriving call connection, telephony failures, and effective disposition precedence in SQL).
- [`pipeline.py`](pipeline.py): Core workflow orchestration:
  - `ingest`: Recursively processes drops, lead CSVs, and event JSONL files with batching, deduplication, non-null lead merging, and error tracking.
  - `report`: Executes analytical SQL queries and writes the 5 output files matching the exact output contracts.
- [`tests/test_pipeline.py`](tests/test_pipeline.py): Automated test suite verifying phone/date/decimal/enum normalization, blank row skipping, rejection precedence, permanent `batch_id`, out-of-order overrides, delayed cross-drop lead arrivals, idempotency, and raw database plaintext PII leakage checks.

### 2. Documentation Updated
- [`SUBMISSION.md`](../SUBMISSION.md): Completed with candidate details, architecture diagram, design decisions, failure mode analysis, benchmark results, and production scaling notes.
- [`AI_USAGE.md`](../AI_USAGE.md): Completed disclosure detailing how AI assistance was leveraged, reviewed, and tested.

---

## How to Run

### Setup
```bash
python -m venv .venv
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1
# Linux / macOS:
# source .venv/bin/activate

pip install -r requirements.txt
```

### 1. Generate Synthetic Data
```bash
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
```

### 2. Ingest Data (Incremental Drops)
```bash
python -m solution ingest data/drop_1
python -m solution ingest data/drop_2
```

### 3. Generate Reports
```bash
python -m solution report --out reports/
```
Generates all 5 output files in `reports/`:
- `batch_summary.csv`
- `daily_overview.csv`
- `lead_report.csv`
- `rejections.csv`
- `data_quality.json`

### 4. Run Tests
```bash
pytest -v
```
