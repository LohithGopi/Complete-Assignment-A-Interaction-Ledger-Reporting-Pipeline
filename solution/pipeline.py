"""Ingestion and reporting pipeline for Assignment A."""
from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from solution.crypto import blind_index, decrypt_pii, encrypt_pii, load_or_create_key, mask_phone
from solution.db import build_report_temp_tables, bump_counter, connect_db, sync_dispositions
from solution.normalize import (
    IST,
    load_dispositions,
    load_schemas,
    normalize_date,
    normalize_decimal,
    normalize_enum,
    normalize_header,
    normalize_phone,
    parse_timestamp_ist,
)

DEFAULT_SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "assignment-a-data" / "schemas"
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def _resolve_drops(input_path: Path) -> list[Path]:
    if (input_path / "leads").is_dir() or (input_path / "events").is_dir():
        return [input_path]
    drops = sorted(
        p for p in input_path.iterdir()
        if p.is_dir() and ((p / "leads").is_dir() or (p / "events").is_dir())
    )
    if not drops:
        raise FileNotFoundError(f"No drop directories with leads/ or events/ found in {input_path}")
    return drops


def _normalize_value(field: dict, raw: str) -> tuple[str | None, bool]:
    """Return (normalized_value_or_none, is_invalid_non_empty)."""
    val = raw.strip()
    if not val:
        return None, False
    ftype = field["type"]
    if ftype == "string":
        return val, False
    if ftype == "phone":
        norm = normalize_phone(val)
        return norm, norm is None
    if ftype == "date":
        norm = normalize_date(val)
        return norm, norm is None
    if ftype == "decimal":
        norm = normalize_decimal(val)
        return norm, norm is None
    if ftype == "enum":
        norm = normalize_enum(val, field["values"])
        return norm, norm is None
    raise ValueError(f"Unsupported field type: {ftype}")


def _ingest_lead_file(conn, key: bytes, schema: dict, csv_path: Path) -> None:
    sha = _file_sha256(csv_path)
    if conn.execute("SELECT 1 FROM ingested_files WHERE file_sha256 = ?", (sha,)).fetchone():
        return

    wf_id = schema["workflow_id"]
    batch_id = csv_path.stem
    alias_map: dict[str, str] = schema["_alias_map"]
    ref_field: str = schema["_ref_field"]
    fields: list[dict] = schema["fields"]

    rows_total = 0
    rows_rejected = 0
    dups_merged = 0
    warnings = 0
    rejections: list[tuple[str, str, int, str]] = []

    existing_map: dict[str, tuple[int, str, str]] = {
        r["phone_hmac"]: (r["id"], r["pii_enc"], r["attrs_json"])
        for r in conn.execute(
            "SELECT id, phone_hmac, pii_enc, attrs_json FROM leads WHERE workflow_id = ?",
            (wf_id,),
        )
    }
    to_insert: list[tuple[str, str, str, str, str, str, str]] = []
    to_update: list[tuple[str, str, str, int]] = []

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            raw_headers = next(reader)
        except StopIteration:
            raw_headers = []
        col_to_field: dict[int, str] = {}
        for idx, h in enumerate(raw_headers):
            mapped = alias_map.get(normalize_header(h))
            if mapped and mapped not in col_to_field.values():
                col_to_field[idx] = mapped
        field_to_col = {name: idx for idx, name in col_to_field.items()}

        row_num = 0
        for raw_row in reader:
            if all(cell.strip() == "" for cell in raw_row):
                continue
            row_num += 1
            rows_total += 1

            parsed: dict[str, str | None] = {}
            invalid_fields: set[str] = set()
            rejection_reason: str | None = None

            for field in fields:
                fname = field["name"]
                col_idx = field_to_col.get(fname)
                raw_val = raw_row[col_idx] if (col_idx is not None and col_idx < len(raw_row)) else ""
                norm_val, is_invalid = _normalize_value(field, raw_val)

                if norm_val is None and not is_invalid:
                    if field.get("required") and rejection_reason is None:
                        rejection_reason = f"missing_required:{fname}"
                elif is_invalid:
                    if fname == "phone" and rejection_reason is None:
                        rejection_reason = "invalid_phone"
                    elif field.get("required") and rejection_reason is None:
                        rejection_reason = f"invalid_{fname}"
                    else:
                        invalid_fields.add(fname)
                parsed[fname] = norm_val

            if rejection_reason is not None:
                rows_rejected += 1
                rejections.append((wf_id, batch_id, row_num, rejection_reason))
                continue

            warnings += sum(1 for f in fields if not f.get("required") and f["name"] in invalid_fields)

            canonical_phone = parsed["phone"]
            assert canonical_phone is not None
            p_hmac = blind_index(key, canonical_phone)
            p_masked = mask_phone(canonical_phone)
            ext_ref = parsed[ref_field]
            assert ext_ref is not None

            pii_data = {f["name"]: parsed[f["name"]] for f in fields if f.get("pii") and parsed[f["name"]] is not None}
            non_pii_data = {
                f["name"]: parsed[f["name"]]
                for f in fields
                if not f.get("pii") and f["name"] != ref_field and parsed[f["name"]] is not None
            }

            existing = existing_map.get(p_hmac)
            if existing is None:
                pii_tok = encrypt_pii(key, json.dumps(pii_data, ensure_ascii=False))
                attrs_s = json.dumps(non_pii_data, ensure_ascii=False)
                to_insert.append((wf_id, p_hmac, batch_id, ext_ref, p_masked, pii_tok, attrs_s))
                # Track intra-file duplicates cleanly by flushing updates via negative sentinel or direct insert
                existing_map[p_hmac] = (-len(to_insert), pii_tok, attrs_s)
            else:
                dups_merged += 1
                lead_id, prev_pii_tok, prev_attrs_s = existing
                cur_pii = json.loads(decrypt_pii(key, prev_pii_tok))
                cur_pii.update(pii_data)
                new_pii_tok = encrypt_pii(key, json.dumps(cur_pii, ensure_ascii=False))
                cur_attrs = json.loads(prev_attrs_s)
                cur_attrs.update(non_pii_data)
                new_attrs_s = json.dumps(cur_attrs, ensure_ascii=False)
                existing_map[p_hmac] = (lead_id, new_pii_tok, new_attrs_s)
                if lead_id < 0:
                    # Update pending intra-file insert in place (preserving original batch_id)
                    pos = -lead_id - 1
                    _, _, orig_batch, _, _, _, _ = to_insert[pos]
                    to_insert[pos] = (wf_id, p_hmac, orig_batch, ext_ref, p_masked, new_pii_tok, new_attrs_s)
                else:
                    to_update.append((ext_ref, new_pii_tok, new_attrs_s, lead_id))

    if to_insert:
        conn.executemany(
            """
            INSERT INTO leads(workflow_id, phone_hmac, batch_id, external_ref, phone_masked, pii_enc, attrs_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            to_insert,
        )
    if to_update:
        conn.executemany(
            "UPDATE leads SET external_ref = ?, pii_enc = ?, attrs_json = ? WHERE id = ?",
            to_update,
        )

    if rejections:
        conn.executemany(
            "INSERT INTO rejections(workflow_id, batch_id, row_number, reason) VALUES (?, ?, ?, ?)",
            rejections,
        )
    bump_counter(conn, "lead_rows_total", rows_total)
    bump_counter(conn, "lead_rows_rejected", rows_rejected)
    bump_counter(conn, "duplicate_lead_rows_merged", dups_merged)
    bump_counter(conn, "field_warnings", warnings)
    conn.execute(
        "INSERT INTO ingested_files(file_sha256, kind, path, ingested_at) VALUES (?, 'leads', ?, ?)",
        (sha, str(csv_path), datetime.now(IST).isoformat(timespec="seconds")),
    )
    conn.commit()


def _ingest_event_file(conn, key: bytes, jsonl_path: Path) -> None:
    sha = _file_sha256(jsonl_path)
    if conn.execute("SELECT 1 FROM ingested_files WHERE file_sha256 = ?", (sha,)).fetchone():
        return

    events_total = 0
    malformed = 0
    batch: list[tuple] = []

    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            events_total += 1
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                malformed += 1
                continue
            if not isinstance(obj, dict):
                malformed += 1
                continue
            event_id = obj.get("event_id")
            event_type = obj.get("event_type")
            workflow_id = obj.get("workflow_id")
            call_id = obj.get("call_id")
            occurred_raw = obj.get("occurred_at")
            if not all(isinstance(v, str) and v.strip() for v in (event_id, event_type, workflow_id, call_id, occurred_raw)):
                malformed += 1
                continue
            try:
                dt_ist = parse_timestamp_ist(occurred_raw)
            except ValueError:
                malformed += 1
                continue

            delta = dt_ist.astimezone(timezone.utc) - _EPOCH_UTC
            occurred_us = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
            ist_iso = dt_ist.isoformat(timespec="seconds")
            phone_raw = obj.get("phone")
            p_hmac: str | None = None
            if isinstance(phone_raw, str) and phone_raw.strip():
                norm_p = normalize_phone(phone_raw)
                if norm_p is not None:
                    p_hmac = blind_index(key, norm_p)

            payload = obj.get("payload") if isinstance(obj.get("payload"), dict) else {}
            disp_code: str | None = None
            if event_type in ("disposition.set", "disposition.override"):
                raw_code = payload.get("disposition_code")
                disp_code = raw_code.strip() if isinstance(raw_code, str) and raw_code.strip() else None

            batch.append((
                event_id.strip(),
                event_type.strip(),
                workflow_id.strip(),
                call_id.strip(),
                occurred_us,
                ist_iso,
                ist_iso[:10],
                p_hmac,
                disp_code,
                json.dumps(payload, ensure_ascii=False),
            ))

    before = conn.total_changes
    if batch:
        conn.executemany(
            """
            INSERT OR IGNORE INTO events(
                event_id, event_type, workflow_id, call_id,
                occurred_at_us, occurred_at_ist, occurred_date_ist,
                phone_hmac, disposition_code, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch,
        )
    inserted = conn.total_changes - before
    dups_dropped = len(batch) - inserted

    bump_counter(conn, "events_total", events_total)
    bump_counter(conn, "malformed_events", malformed)
    bump_counter(conn, "duplicate_events_dropped", dups_dropped)
    conn.execute(
        "INSERT INTO ingested_files(file_sha256, kind, path, ingested_at) VALUES (?, 'events', ?, ?)",
        (sha, str(jsonl_path), datetime.now(IST).isoformat(timespec="seconds")),
    )
    conn.commit()


def ingest(input_path: str | Path, db_path: str | Path, schemas_dir: Path = DEFAULT_SCHEMAS_DIR) -> None:
    key = load_or_create_key(db_path)
    schemas = load_schemas(schemas_dir)
    dispositions = load_dispositions(schemas_dir)
    conn = connect_db(db_path)
    try:
        sync_dispositions(conn, dispositions)
        for drop_dir in _resolve_drops(Path(input_path)):
            leads_root = drop_dir / "leads"
            if leads_root.is_dir():
                for wf_dir in sorted(p for p in leads_root.iterdir() if p.is_dir()):
                    if wf_dir.name not in schemas:
                        raise ValueError(f"Unknown workflow directory: {wf_dir.name}")
                    schema = schemas[wf_dir.name]
                    for csv_path in sorted(wf_dir.glob("*.csv")):
                        _ingest_lead_file(conn, key, schema, csv_path)
            events_root = drop_dir / "events"
            if events_root.is_dir():
                for jsonl_path in sorted(events_root.glob("*.jsonl")):
                    _ingest_event_file(conn, key, jsonl_path)
    finally:
        conn.close()


def _write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def report(out_dir: str | Path, db_path: str | Path) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    conn = connect_db(db_path)
    try:
        orphan_events, unknown_disp_events = build_report_temp_tables(conn)

        lead_rows = conn.execute("""
            WITH call_agg AS (
                SELECT
                    lead_id,
                    COUNT(*) AS total_calls,
                    SUM(is_connected) AS connected_calls,
                    MAX(initiated_us) AS max_init_us,
                    MAX(CASE WHEN is_terminal = 1 THEN 1 ELSE 0 END) AS has_terminal,
                    MAX(CASE WHEN is_success = 1 THEN 1 ELSE 0 END) AS ever_success,
                    MIN(is_telephony_failed) AS all_telephony_failed
                FROM _calls
                GROUP BY lead_id
            ),
            last_call AS (
                SELECT
                    lead_id,
                    initiated_ist,
                    ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY initiated_us DESC, call_id DESC) AS rn
                FROM _calls
            ),
            last_disp AS (
                SELECT
                    lead_id,
                    effective_disposition,
                    ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY initiated_us DESC, call_id DESC) AS rn
                FROM _calls
                WHERE effective_disposition IS NOT NULL
            )
            SELECT
                l.workflow_id,
                l.external_ref,
                l.batch_id,
                l.phone_masked,
                CASE
                    WHEN COALESCE(ca.total_calls, 0) = 0 THEN 'yet_to_reach_out'
                    WHEN COALESCE(ca.has_terminal, 0) = 1 THEN 'completed'
                    WHEN COALESCE(ca.all_telephony_failed, 0) = 1 THEN 'in_progress_telephony_failed'
                    ELSE 'in_progress_dialed'
                END AS bucket,
                COALESCE(ca.total_calls, 0) AS total_calls,
                COALESCE(ca.connected_calls, 0) AS connected_calls,
                COALESCE(lc.initiated_ist, '') AS last_call_at,
                COALESCE(ld.effective_disposition, '') AS current_disposition,
                CASE WHEN COALESCE(ca.ever_success, 0) = 1 THEN 'true' ELSE 'false' END AS ever_success
            FROM leads l
            LEFT JOIN call_agg ca ON ca.lead_id = l.id
            LEFT JOIN last_call lc ON lc.lead_id = l.id AND lc.rn = 1
            LEFT JOIN last_disp ld ON ld.lead_id = l.id AND ld.rn = 1
            ORDER BY l.workflow_id, l.external_ref
        """).fetchall()

        _write_csv(
            out / "lead_report.csv",
            [
                "workflow_id", "external_ref", "batch_id", "phone_masked", "bucket",
                "total_calls", "connected_calls", "last_call_at", "current_disposition", "ever_success",
            ],
            [[r[c] for c in r.keys()] for r in lead_rows],
        )

        # Compute batch_summary in SQL using the same bucket definition
        batch_rows = conn.execute("""
            WITH call_agg AS (
                SELECT
                    lead_id,
                    COUNT(*) AS total_calls,
                    MAX(CASE WHEN is_terminal = 1 THEN 1 ELSE 0 END) AS has_terminal,
                    MIN(is_telephony_failed) AS all_telephony_failed
                FROM _calls
                GROUP BY lead_id
            ),
            lead_buckets AS (
                SELECT
                    l.workflow_id,
                    l.batch_id,
                    CASE
                        WHEN COALESCE(ca.total_calls, 0) = 0 THEN 'yet_to_reach_out'
                        WHEN COALESCE(ca.has_terminal, 0) = 1 THEN 'completed'
                        WHEN COALESCE(ca.all_telephony_failed, 0) = 1 THEN 'in_progress_telephony_failed'
                        ELSE 'in_progress_dialed'
                    END AS bucket
                FROM leads l
                LEFT JOIN call_agg ca ON ca.lead_id = l.id
            )
            SELECT
                workflow_id,
                batch_id,
                COUNT(*) AS total_leads,
                SUM(CASE WHEN bucket = 'yet_to_reach_out' THEN 1 ELSE 0 END) AS yet_to_reach_out,
                SUM(CASE WHEN bucket = 'in_progress_telephony_failed' THEN 1 ELSE 0 END) AS in_progress_telephony_failed,
                SUM(CASE WHEN bucket = 'in_progress_dialed' THEN 1 ELSE 0 END) AS in_progress_dialed,
                SUM(CASE WHEN bucket = 'completed' THEN 1 ELSE 0 END) AS completed
            FROM lead_buckets
            GROUP BY workflow_id, batch_id
            ORDER BY workflow_id, batch_id
        """).fetchall()

        _write_csv(
            out / "batch_summary.csv",
            [
                "workflow_id", "batch_id", "total_leads", "yet_to_reach_out",
                "in_progress_telephony_failed", "in_progress_dialed", "completed",
            ],
            [[r[c] for c in r.keys()] for r in batch_rows],
        )

        daily_rows = conn.execute("""
            SELECT
                workflow_id,
                initiated_date_ist AS date,
                COUNT(*) AS total_calls,
                SUM(is_connected) AS connected_calls,
                PRINTF('%.4f', CAST(SUM(is_connected) AS REAL) / COUNT(*)) AS connectivity_rate,
                COUNT(DISTINCT CASE WHEN is_success = 1 THEN lead_id END) AS success_leads
            FROM _calls
            GROUP BY workflow_id, initiated_date_ist
            ORDER BY workflow_id, initiated_date_ist
        """).fetchall()

        _write_csv(
            out / "daily_overview.csv",
            ["workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate", "success_leads"],
            [[r[c] for c in r.keys()] for r in daily_rows],
        )

        rej_rows = conn.execute("""
            SELECT workflow_id, batch_id, row_number, reason
            FROM rejections
            ORDER BY workflow_id, batch_id, row_number
        """).fetchall()

        _write_csv(
            out / "rejections.csv",
            ["workflow_id", "batch_id", "row_number", "reason"],
            [[r[c] for c in r.keys()] for r in rej_rows],
        )

        counters = {
            r["key"]: r["value"]
            for r in conn.execute("SELECT key, value FROM ingest_counters").fetchall()
        }
        rej_by_reason = {
            r["reason"]: r["cnt"]
            for r in conn.execute(
                "SELECT reason, COUNT(*) AS cnt FROM rejections GROUP BY reason ORDER BY reason"
            ).fetchall()
        }
        dq = {
            "lead_rows_total": counters["lead_rows_total"],
            "lead_rows_rejected": counters["lead_rows_rejected"],
            "rejections_by_reason": rej_by_reason,
            "duplicate_lead_rows_merged": counters["duplicate_lead_rows_merged"],
            "field_warnings": counters["field_warnings"],
            "events_total": counters["events_total"],
            "malformed_events": counters["malformed_events"],
            "duplicate_events_dropped": counters["duplicate_events_dropped"],
            "orphan_events": orphan_events,
            "unknown_disposition_events": unknown_disp_events,
        }
        (out / "data_quality.json").write_text(json.dumps(dq, indent=2) + "\n", encoding="utf-8")
    finally:
        conn.close()
