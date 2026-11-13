# Agentic Universe: Junior SDE Take-Home (Data & Workflow)

> **Chosen Assignment:** **Assignment A — Interaction Ledger & Reporting Pipeline (Data)**  
> **Candidate:** Lohith Gopi S  
> **Full Solution & Code:** [`solution/`](solution/) | **Submission Details:** [`SUBMISSION.md`](SUBMISSION.md) | **AI Disclosure:** [`AI_USAGE.md`](AI_USAGE.md)

---

## What I Have Done for Assignment A

I built an end-to-end, idempotent, privacy-preserving data ingestion and reporting pipeline in Python 3.13 and SQLite (WAL mode). Below is a summary of the implementation:

### 1. Ingestion & Normalization Engine ([`solution/normalize.py`](solution/normalize.py))
- **Header matching & alias mapping:** Lowercases, trims, and normalizes underscores/whitespace runs to map varying CSV headers (e.g. `Insured Name`, `Mobile No`) to canonical schema fields dynamically.
- **Phone normalization:** Cleans Indian mobile numbers, strips valid `91` (12 digits) or `0` (11 digits) prefixes, enforces 10 digits starting with `6`, `7`, `8`, or `9`, and outputs canonical `+91XXXXXXXXXX`. Rejects landlines and invalid formats.
- **Date parsing:** Day-first parser accepting `YYYY-MM-DD`, `DD/MM/YYYY`, `DD-MM-YYYY`, and `DD-Mon-YYYY` (e.g., `01-Dec-2026`), rejecting impossible calendar dates (such as `31/02/2026`).
- **Currency & decimal formatting:** Strips `₹`, `Rs`, `Rs.`, `INR` and thousands commas, rounding non-negative amounts to 2 decimal places.
- **Enum validation:** Case-insensitively validates values against workflow schema definitions.
- **IST timestamp parsing:** Parses UTC (`...Z`), explicit offsets (`...+05:30`), and naive timestamps (assumed IST per spec) into timezone-aware IST datetimes.

### 2. PII Protection at Rest ([`solution/crypto.py`](solution/crypto.py))
- **Deterministic blind index:** Computes HMAC-SHA256 digests (`phone_hmac`) using a secret key (`AU_PII_KEY` or auto-generated `<db>.key`) to enable exact relational lookups between leads and events without storing plain phone numbers.
- **Authenticated encryption:** Encrypts PII fields (`customer_name`, `phone`) using HMAC-CTR keystream encryption with Encrypt-then-MAC authentication verified with constant-time comparison.
- **Masking:** Masks customer phone numbers as `+91******XXXX` in exported reports.

### 3. Append-Only Database & Idempotency ([`solution/db.py`](solution/db.py))
- **File idempotency:** Tracks SHA-256 file hashes in `ingested_files` so re-ingesting an already-processed drop or file is a complete no-op.
- **Append-only ledger:** The `events` table is keyed by `event_id PRIMARY KEY` with `INSERT OR IGNORE` for duplicate dropping; subsequent calls or supervisor overrides simply append new records.
- **Lead deduplication & merging:** Keyed by `UNIQUE(workflow_id, phone_hmac)`. Locks in the first file as the lead's permanent `batch_id`, while merging subsequent records using last-write-wins for non-null attributes.
- **Zero-PII rejection log:** Logs rejected rows in `rejections` without sensitive data.

### 4. SQL-Driven Reporting & Fact Derivation ([`solution/pipeline.py`](solution/pipeline.py) & [`solution/db.py`](solution/db.py))
- Uses SQLite views and CTEs to compute analytics declaratively:
  - **Orphan resolution & delayed leads:** Events arriving in `drop_1` whose leads only arrive in `drop_2` link dynamically at report time without destructive mutations.
  - **Phone-less supervisor overrides:** Links `disposition.override` events back to the lead via `(workflow_id, call_id)`.
  - **Effective disposition:** Evaluates outcome hierarchy in SQL (`disposition.override` > `disposition.set` > latest timestamp > `event_id`).
  - **Lead bucketing:** Classifies leads in strict order: `yet_to_reach_out` → `completed` → `in_progress_telephony_failed` → `in_progress_dialed`.
- Generates all 5 output files matching the spec:
  1. `batch_summary.csv`
  2. `daily_overview.csv`
  3. `lead_report.csv`
  4. `rejections.csv`
  5. `data_quality.json`

### 5. Automated Tests & Verification ([`solution/tests/test_pipeline.py`](solution/tests/test_pipeline.py))
- Comprehensive `pytest` suite testing phone/date/decimal/timestamp normalization, blank row skipping, rejection precedence, permanent `batch_id` retention, out-of-order overrides, delayed lead linking, re-ingest idempotency, single-shot vs incremental equivalence, and raw database string assertions verifying zero plaintext PII leaks.

---

## Quickstart Commands

```bash
# Setup
python -m venv .venv
.\.venv\Scripts\Activate.ps1    # or: source .venv/bin/activate
pip install -r requirements.txt

# Generate synthetic test data
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data

# Run incremental ingestion and report generation
python -m solution ingest data/drop_1
python -m solution ingest data/drop_2
python -m solution report --out reports/

# Run tests
pytest -v
```

---

# Original Assignment Context

Thanks for your interest in Agentic Universe. We build AI voice agents for insurers, lenders and
consumer brands across India and APAC. Our platform has three layers:

- **Data:** leads, call events, dispositions and the interaction ledger that every client report
  is built on.
- **Workflow:** campaigns, scheduling, dialling, retries and callbacks, all orchestrated durably.
- **Application:** dashboards, consoles and APIs.

This take-home focuses on **data** and **workflow**. **Pick one assignment.** Don't do both.

| | Assignment | You'll mostly be doing |
|---|---|---|
| **A** | [Interaction Ledger & Reporting Pipeline](assignment-a-data/README.md) | Messy-data ingestion, schema design, idempotent pipelines, exact metrics |
| **B** | [Durable Campaign Dispatcher](assignment-b-workflow/README.md) | State machines, idempotency, failure handling, scheduling under constraints |

Both are judged on the same bar. Pick the one that excites you more.

## Ground rules

- **Time box:** plan for about **6–8 focused hours**. You have **72 hours** from when you receive
  this. We know you have a life. If you run out of time, stop and tell us in `SUBMISSION.md`
  what you'd do next. A well-explained partial solution beats a rushed complete one.
- **Language:** Python 3.11+. Use any libraries you like; list them in `requirements.txt`.
- **AI tools** (Claude, Cursor, Copilot, ChatGPT...) are **allowed**, as they would be on the
  job. You must fill in [`AI_USAGE.md`](AI_USAGE.md) honestly. In the follow-up interview we'll
  ask you to walk through your code and change it live. Submit only code you understand and can
  defend.
- **Ambiguity is intentional in places.** When the spec doesn't decide something, make a
  reasonable call and write it down in `SUBMISSION.md`. Don't wait for an answer from us.
- **Synthetic data only.** Everything here is generated. No real customer data is involved.

## Getting started

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# then follow your chosen assignment's README
```

`docker-compose.yml` starts a local Postgres if you want one (`docker compose up -d`). SQLite
is equally acceptable.

## What to submit

1. Push your work to a **private** GitHub or GitLab repository. Start from this repo and keep
   its structure. Put your code in `solution/` (or a similar top-level folder) and keep the
   original files unmodified.
2. Your repo must include:
   - `SUBMISSION.md`: fill in the template provided
   - `AI_USAGE.md`: fill in the template provided
   - Exact setup and run commands that work from a clean clone
   - Tests, runnable with one command (for example `pytest`)
3. Give read access to **`[[REVIEWER_HANDLES]]`** and email the repo link to
   **`[[SUBMISSION_EMAIL]]`** before the deadline.

Please **don't** publish your solution publicly, and **don't** commit generated data, `.env`
files, or databases.

## What happens next

If your submission clears the bar, we'll schedule a **45-minute technical conversation**:

- 15 min: you walk us through your design and the decisions in `SUBMISSION.md`
- 20 min: we change a requirement and you extend your code live (AI tools allowed; screen
  share)
- 10 min: your questions for us

## What we look for

| Signal | What good looks like |
|---|---|
| Correctness | It produces the right answer on data and seeds you haven't seen |
| Robustness | It behaves sensibly when inputs are dirty, things fail, or it's run twice |
| Design | Clear data and state model; you can explain why it's safe |
| Code quality | Readable, organised, tested where it matters |
| Communication | Honest about trade-offs, assumptions and what's missing |
| Ownership | You made decisions instead of waiting to be told |

Good luck. We're looking forward to reading your work.
