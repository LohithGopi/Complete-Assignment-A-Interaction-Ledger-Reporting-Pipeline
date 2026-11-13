# Submission

## Candidate
- **Name:** Lohith Gopi S
- **Assignment chosen:** A (Data) — Interaction Ledger & Reporting Pipeline
- **Time spent (honest estimate):** ~10h

## How to run
```bash
# Setup
py -3.13 -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt

# Generate synthetic data
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data

# Run incremental pipeline and generate reports
python -m solution ingest data/drop_1
python -m solution ingest data/drop_2
python -m solution report --out reports/

# Run tests
pytest -v
```

## Design

The pipeline uses Python 3.13 (standard library only for runtime; `pytest` for tests) backed by SQLite in WAL mode (`solution.sqlite`).

```
CSV Lead Batches ──► normalize & validate ──► leads (phone_hmac, encrypted PII, attrs_json)
                                          └── rejections (no PII)

JSONL Call Events ─► parse & timestamp IST ─► events (append-only ledger, phone_hmac)
                                                    │
SQL Report Engine ◄─────────────────────────────────┘
  ├─ _call_phone: links phone-less disposition.override via (workflow_id, call_id)
  ├─ _calls: derives call connection, telephony failure, and effective disposition
  └─ outputs: batch_summary.csv, daily_overview.csv, lead_report.csv, rejections.csv, data_quality.json
```

**Schema & PII protection:**
- `ingested_files(file_sha256 PK, kind, path, ingested_at)`: guarantees file-level idempotency so re-ingesting any drop is a no-op.
- `ingest_counters(key PK, value)`: persists cumulative ingestion counters (`lead_rows_total`, `lead_rows_rejected`, `duplicate_lead_rows_merged`, `field_warnings`, `events_total`, `malformed_events`, `duplicate_events_dropped`).
- `leads`: keyed by `UNIQUE(workflow_id, phone_hmac)`. Stores `phone_hmac` (HMAC-SHA256 blind index for exact matching), `phone_masked` (`+91******XXXX` for reporting), and `pii_enc` (HMAC-SHA256 CTR + Encrypt-then-MAC ciphertext of `customer_name` and `phone`). No plaintext PII is stored in SQLite or `rejections.csv`.
- `events`: append-only ledger keyed by `event_id PK` (using `INSERT OR IGNORE`). Stores `phone_hmac` instead of plaintext phone.
- `dispositions`: loaded from `assignment-a-data/schemas/dispositions.csv`.

## Decisions & assumptions

| Decision | Why | Alternative considered |
|---|---|---|
| Resolve orphans & unknown dispositions at report time in SQL | Events can arrive in `drop_1` before their lead arrives in `drop_2`, and `disposition.override` can arrive before `call.initiated` in shuffled JSONL files. Evaluating links dynamically ensures `drop_1` → `drop_2` matches single-shot ingestion. | Materializing `lead_id` on insert and backfilling on later lead arrival (more mutable state). |
| Stdlib HMAC-SHA256 blind index + HMAC-CTR Encrypt-then-MAC (`AU_PII_KEY` env var or `<db>.key` file) | Zero external dependencies beyond `requirements.txt`, works out-of-the-box across separate `ingest` / `report` commands, and prevents plaintext PII at rest. | `cryptography` Fernet (requires adding dependency) or KMS-backed envelope encryption (recommended in prod). |
| Deduplicate ingested files by SHA-256 content hash | Re-running `ingest` on a previously ingested drop changes nothing—even if the drop folder is moved or re-run—while still catching new files in future drops. | Tracking `(drop_name, filename)` only, which misses moved paths or silently modified files. |
| Store only non-null non-PII attributes in `attrs_json` without eagerly writing defaults | Preserves "defaults apply after merging" across multiple drops so an earlier null doesn't block or get confused with an explicit value from a later drop. | Applying defaults on first insert. |
| SQLite with `WAL`, `temp_store = MEMORY`, and SQL-based report aggregations | Zero external DB setup required to run from a clean clone while keeping aggregations declarative in SQL. | PostgreSQL via `docker-compose.yml` (better for multi-writer concurrency in prod). |

## Failure modes considered
- **Re-runs / duplicate files:** `ingested_files` skips previously seen file SHA-256 hashes; `events` uses `event_id` primary key (`INSERT OR IGNORE`) for cross-file/cross-drop event dedupe.
- **Dirty CSVs & malformed JSONL:** Blank CSV rows are skipped before row numbering; extra/aliased/reordered headers are mapped case-insensitively; malformed JSON lines or events missing required fields/valid timestamps increment `malformed_events` and skip cleanly; invalid optional fields on valid leads record `field_warnings` without failing the row.
- **Out-of-order & delayed data:** Shuffled events (`disposition.override` before `call.initiated`) and cross-drop lead arrivals (`events` in `drop_1`, `leads` in `drop_2`) resolve accurately via SQL joins at report time.
- **Not handled:** Concurrent writers to the same SQLite file from multiple processes simultaneously, and envelope key rotation for `pii_enc` (`<db>.key` is a local keyfile when `AU_PII_KEY` is unset).

## Testing
Tested via `pytest` in `solution/tests/test_pipeline.py`:
- `test_phone_normalization`: all 6 Indian mobile formats plus invalid lengths, prefixes (`5...`), landlines, US numbers, and `N/A`.
- `test_date_decimal_enum_header_normalization`: all 4 date formats, impossible dates (`31/02/2026`, `2026-13-01`), currency prefixes/commas/rounding/negative values, case-insensitive enums, header normalization, and UTC/offset/naive IST timestamp parsing.
- `test_end_to_end_rules_pii_and_idempotency`: blank rows, rejection precedence, field warnings skipped on rejected rows, permanent first `batch_id` on duplicate merge, out-of-order `disposition.override`, delayed lead arrival across drops, unknown disposition fallback, `current_disposition` on a lead whose latest call has no disposition, all 4 lead buckets, SQLite plaintext-PII leak check, re-ingest idempotency, and incremental vs all-at-once byte-for-byte report equivalence.

## Results
**Small dataset (`--seed 42 --scale small`, 2,057 lead rows / 8,745 event lines):**
- `lead_rows_total`: 2,057 (`1,892` unique valid leads + `108` rejected + `57` merged duplicates)
- `field_warnings`: 122
- `events_total`: 8,745 (`8` malformed, `237` duplicates dropped, `462` final orphans, `18` unknown dispositions)

**Large dataset (`--seed 42 --scale large`, 143,990 lead rows / 632,602 event lines, inside OneDrive-synced workspace on Windows):**
- `drop_1`: 94.15s, `drop_2`: 126.90s, `report`: 64.95s, **total: 286.00s** (exceeded the 2-minute stretch target due to SQLite index maintenance on 632k events + OneDrive folder I/O).

## If I had another day
1. **Faster `--scale large` ingestion (< 2 min):** Stage incoming batches into unindexed temp tables or drop/recreate secondary indexes around bulk loads, and switch to PostgreSQL (`COPY` + parallel workers) for 100k+ leads/day.
2. **True incremental rollups:** Maintain per-lead and per-day summary deltas on `ingest` instead of recomputing `_calls` across the entire `events` ledger on every `report`.
3. **Production hardening:** Store PII encryption keys in a KMS/secrets manager with key-version tags for rotation, and add structured logging + schema evolution checks.
