# AI Usage Disclosure

AI tools are allowed. We want to know **how** you used them, because using them well is a skill. Honest answers are never held against you. Discovering undisclosed use is.

## Tools used
Antigravity (AI coding assistant)

## What I used them for
- Setting up the Python 3.13 virtual environment and generating synthetic test data.
- Drafting the pipeline architecture (`solution/crypto.py`, `solution/normalize.py`, `solution/db.py`, `solution/pipeline.py`, `solution/__main__.py`), SQL report queries, and `pytest` unit/integration tests.
- Profiling and optimizing lead batching and SQLite report temp tables on the `--scale large` dataset.

## What I wrote or decided myself
- Chose Assignment A (Data) and selected SQLite + Python stdlib to keep setup zero-dependency from a clean clone.
- Reviewed and verified the schema design, PII blind-index/encryption approach, and spec edge cases (cross-drop orphan linking, `current_disposition` fallback, and idempotency).

## Where the AI was wrong or unhelpful
- Initial `pytest` invocation failed collection with `ModuleNotFoundError: No module named 'solution'` until `solution/tests/__init__.py` was added.
- Initial `--scale large` implementation did per-row SQLite lookups/inserts and materialized a full 632k-row `_linked_events` temp table (~432s total), requiring batched lead inserts/updates and streamlined SQL joins (~286s).

## How I verified AI-generated code
- Ran `pytest -v` covering normalization edge cases, out-of-order events, orphan resolution across drops, idempotent re-ingestion, and raw SQLite dump checks confirming no plaintext PII is stored.
- Ran end-to-end ingestion and reporting on both `--scale small` and `--scale large` datasets and checked `batch_summary.csv` and `data_quality.json` totals manually.
