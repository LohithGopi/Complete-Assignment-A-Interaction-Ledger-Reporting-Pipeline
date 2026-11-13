"""SQLite schema and SQL-based report queries for Assignment A."""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -64000;

CREATE TABLE IF NOT EXISTS ingested_files (
    file_sha256 TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    path TEXT NOT NULL,
    ingested_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_counters (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dispositions (
    workflow_id TEXT NOT NULL,
    code TEXT NOT NULL,
    is_terminal INTEGER NOT NULL,
    is_success INTEGER NOT NULL,
    PRIMARY KEY (workflow_id, code)
);

CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT NOT NULL,
    phone_hmac TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    external_ref TEXT NOT NULL,
    phone_masked TEXT NOT NULL,
    pii_enc TEXT NOT NULL,
    attrs_json TEXT NOT NULL,
    UNIQUE (workflow_id, phone_hmac)
);

CREATE TABLE IF NOT EXISTS rejections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    row_number INTEGER NOT NULL,
    reason TEXT NOT NULL
);

-- Append-only interaction ledger. Never updated or deleted after insert.
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    call_id TEXT NOT NULL,
    occurred_at_us INTEGER NOT NULL,
    occurred_at_ist TEXT NOT NULL,
    occurred_date_ist TEXT NOT NULL,
    phone_hmac TEXT,
    disposition_code TEXT,
    payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_wf_phone ON events(workflow_id, phone_hmac);
CREATE INDEX IF NOT EXISTS idx_events_wf_call ON events(workflow_id, call_id);
CREATE INDEX IF NOT EXISTS idx_rejections_sort ON rejections(workflow_id, batch_id, row_number);
"""

COUNTER_KEYS = (
    "lead_rows_total",
    "lead_rows_rejected",
    "duplicate_lead_rows_merged",
    "field_warnings",
    "events_total",
    "malformed_events",
    "duplicate_events_dropped",
)


def connect_db(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    conn.executemany(
        "INSERT OR IGNORE INTO ingest_counters(key, value) VALUES (?, 0)",
        [(k,) for k in COUNTER_KEYS],
    )
    conn.commit()
    return conn


def sync_dispositions(conn: sqlite3.Connection, rows: list[tuple[str, str, int, int]]) -> None:
    conn.executemany(
        """
        INSERT INTO dispositions(workflow_id, code, is_terminal, is_success)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(workflow_id, code) DO UPDATE SET
            is_terminal = excluded.is_terminal,
            is_success = excluded.is_success
        """,
        rows,
    )
    conn.commit()


def bump_counter(conn: sqlite3.Connection, key: str, delta: int) -> None:
    if delta:
        conn.execute("UPDATE ingest_counters SET value = value + ? WHERE key = ?", (delta, key))


def build_report_temp_tables(conn: sqlite3.Connection) -> tuple[int, int]:
    """Build per-call temp table and return (orphan_events, unknown_disposition_events)."""
    conn.executescript("""
    DROP TABLE IF EXISTS temp._call_phone;
    DROP TABLE IF EXISTS temp._calls;

    CREATE TEMP TABLE _call_phone (
        workflow_id TEXT NOT NULL,
        call_id TEXT NOT NULL,
        phone_hmac TEXT NOT NULL,
        PRIMARY KEY (workflow_id, call_id)
    );
    INSERT INTO _call_phone(workflow_id, call_id, phone_hmac)
    SELECT workflow_id, call_id, MAX(phone_hmac)
    FROM events
    WHERE event_type != 'disposition.override' AND phone_hmac IS NOT NULL
    GROUP BY workflow_id, call_id;

    CREATE TEMP TABLE _calls AS
    WITH linked AS (
        SELECT
            e.event_id,
            e.event_type,
            e.workflow_id,
            e.call_id,
            e.occurred_at_us,
            e.occurred_at_ist,
            e.occurred_date_ist,
            e.disposition_code,
            l.id AS lead_id,
            d.code AS valid_disp_code
        FROM events e
        LEFT JOIN _call_phone cp
          ON e.event_type = 'disposition.override'
         AND e.phone_hmac IS NULL
         AND cp.workflow_id = e.workflow_id
         AND cp.call_id = e.call_id
        JOIN leads l
          ON l.workflow_id = e.workflow_id
         AND l.phone_hmac = COALESCE(e.phone_hmac, cp.phone_hmac)
        LEFT JOIN dispositions d
          ON e.event_type IN ('disposition.set', 'disposition.override')
         AND d.workflow_id = e.workflow_id
         AND d.code = e.disposition_code
        WHERE e.event_type IN (
            'call.initiated', 'call.connected', 'call.telephony_failed',
            'disposition.set', 'disposition.override'
        )
    ),
    init AS (
        SELECT
            workflow_id,
            call_id,
            lead_id,
            MIN(occurred_at_us) AS initiated_us,
            MIN(occurred_at_ist) AS initiated_ist,
            MIN(occurred_date_ist) AS initiated_date_ist
        FROM linked
        WHERE event_type = 'call.initiated'
        GROUP BY workflow_id, call_id, lead_id
    ),
    flags AS (
        SELECT
            workflow_id,
            call_id,
            MAX(CASE WHEN event_type = 'call.connected' THEN 1 ELSE 0 END) AS is_connected,
            MAX(CASE WHEN event_type = 'call.telephony_failed' THEN 1 ELSE 0 END) AS is_telephony_failed
        FROM linked
        GROUP BY workflow_id, call_id
    ),
    ranked_disp AS (
        SELECT
            workflow_id,
            call_id,
            valid_disp_code AS disposition_code,
            ROW_NUMBER() OVER (
                PARTITION BY workflow_id, call_id
                ORDER BY
                    CASE WHEN event_type = 'disposition.override' THEN 1 ELSE 0 END DESC,
                    occurred_at_us DESC,
                    event_id DESC
            ) AS rn
        FROM linked
        WHERE valid_disp_code IS NOT NULL
    )
    SELECT
        i.workflow_id,
        i.call_id,
        i.lead_id,
        i.initiated_us,
        i.initiated_ist,
        i.initiated_date_ist,
        f.is_connected,
        f.is_telephony_failed,
        rd.disposition_code AS effective_disposition,
        COALESCE(d.is_terminal, 0) AS is_terminal,
        COALESCE(d.is_success, 0) AS is_success
    FROM init i
    JOIN flags f ON f.workflow_id = i.workflow_id AND f.call_id = i.call_id
    LEFT JOIN ranked_disp rd ON rd.workflow_id = i.workflow_id AND rd.call_id = i.call_id AND rd.rn = 1
    LEFT JOIN dispositions d ON d.workflow_id = i.workflow_id AND d.code = rd.disposition_code;

    CREATE INDEX temp._idx_calls_lead ON _calls(lead_id);
    CREATE INDEX temp._idx_calls_wf_date ON _calls(workflow_id, initiated_date_ist);
    """)
    row = conn.execute("""
        SELECT
            SUM(CASE WHEN l.id IS NULL THEN 1 ELSE 0 END) AS orphan_events,
            SUM(CASE WHEN l.id IS NOT NULL
                      AND e.event_type IN ('disposition.set', 'disposition.override')
                      AND d.code IS NULL
                     THEN 1 ELSE 0 END) AS unknown_disposition_events
        FROM events e
        LEFT JOIN _call_phone cp
          ON e.event_type = 'disposition.override'
         AND e.phone_hmac IS NULL
         AND cp.workflow_id = e.workflow_id
         AND cp.call_id = e.call_id
        LEFT JOIN leads l
          ON l.workflow_id = e.workflow_id
         AND l.phone_hmac = COALESCE(e.phone_hmac, cp.phone_hmac)
        LEFT JOIN dispositions d
          ON e.event_type IN ('disposition.set', 'disposition.override')
         AND d.workflow_id = e.workflow_id
         AND d.code = e.disposition_code
    """).fetchone()
    return row["orphan_events"] or 0, row["unknown_disposition_events"] or 0
