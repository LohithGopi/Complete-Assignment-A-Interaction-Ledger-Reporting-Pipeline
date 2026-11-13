"""Unit and end-to-end tests for Assignment A pipeline rules."""
from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

from solution.normalize import (
    normalize_date,
    normalize_decimal,
    normalize_enum,
    normalize_header,
    normalize_phone,
    parse_timestamp_ist,
)
from solution.pipeline import ingest, report


def test_phone_normalization():
    assert normalize_phone("9876543210") == "+919876543210"
    assert normalize_phone("+91 98765 43210") == "+919876543210"
    assert normalize_phone("09876543210") == "+919876543210"
    assert normalize_phone("91-61234-56789") == "+916123456789"
    assert normalize_phone("(+91) 7000000001") == "+917000000001"
    assert normalize_phone("8999999999") == "+918999999999"

    assert normalize_phone("5876543210") is None
    assert normalize_phone("987654321") is None
    assert normalize_phone("022-2345-6789") is None
    assert normalize_phone("+1 415 555 0134") is None
    assert normalize_phone("N/A") is None


def test_date_decimal_enum_header_normalization():
    assert normalize_header("  Mobile__No ") == "mobile no"
    assert normalize_header("Customer_Name") == "customer name"

    assert normalize_date("2026-10-05") == "2026-10-05"
    assert normalize_date("05/10/2026") == "2026-10-05"
    assert normalize_date("05-10-2026") == "2026-10-05"
    assert normalize_date("01-dec-2026") == "2026-12-01"
    assert normalize_date("31/02/2026") is None
    assert normalize_date("2026-13-01") is None
    assert normalize_date("soon") is None

    assert normalize_decimal("₹12,345") == "12345.00"
    assert normalize_decimal("Rs. 1,200.5") == "1200.50"
    assert normalize_decimal("inr 99.995") == "100.00"
    assert normalize_decimal("0") == "0.00"
    assert normalize_decimal("-5") is None
    assert normalize_decimal("N/A") is None

    assert normalize_enum(" HI ", ["en", "hi"]) == "hi"
    assert normalize_enum("Hindi", ["en", "hi"]) is None

    assert parse_timestamp_ist("2026-10-05T00:00:00Z").strftime("%Y-%m-%d %H:%M:%S") == "2026-10-05 05:30:00"
    assert parse_timestamp_ist("2026-10-05 10:01:00").isoformat(timespec="seconds") == "2026-10-05T10:01:00+05:30"


def _write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_end_to_end_rules_pii_and_idempotency(tmp_path: Path):
    d1 = tmp_path / "data" / "drop_1"
    d2 = tmp_path / "data" / "drop_2"

    # Drop 1 leads: blank rows, rejections, duplicate merge, optional warnings
    _write_file(
        d1 / "leads" / "motor_renewal" / "batch_001.csv",
        "\n".join([
            "customer_name,phone,policy_number,policy_expiry,premium_amount,preferred_language",
            "Alice Secret,9876543210,POL-1,05/10/2026,₹1000,HI",
            ",,,,,",  # blank row: skipped, not counted in row_number
            "Bob Bad,12345,POL-2,31/02/2026,,",  # row 2: invalid_phone (optional warning ignored on rejected row)
            ",9876543211,POL-3,,,",  # row 3: missing_required:customer_name
            "Carol Warn,9876543212,POL-4,31/02/2026,bad,Hindi",  # row 4: valid lead, 3 field warnings
            "Dave Tel,9876543213,POL-5,,,",  # row 5: all calls telephony_failed
            "Eve Idle,9876543214,POL-6,,,",  # row 6: no calls -> yet_to_reach_out
        ]) + "\n",
    )

    # Drop 2 leads: aliased headers, duplicate of POL-1 (keeps batch_001), plus delayed lead POL-7
    _write_file(
        d2 / "leads" / "motor_renewal" / "batch_003.csv",
        "\n".join([
            "Insured Name,Mobile No,Policy No,Language,Extra Col",
            "Alice Updated,09876543210,POL-1,,ignored",
            "Frank Late,9876543215,POL-7,ta,ignored",
        ]) + "\n",
    )

    # Drop 1 events:
    # - override arrives BEFORE call.initiated and has NO phone
    # - call for Frank Late arrives BEFORE Frank's lead arrives in drop_2
    # - malformed line + duplicate event across drops + pure orphan + unknown disposition
    d1_events = [
        json.dumps({
            "event_id": "e_ovr1", "event_type": "disposition.override",
            "workflow_id": "motor_renewal", "call_id": "c_alice_1",
            "occurred_at": "2026-10-02T12:00:00+05:30",
            "payload": {"disposition_code": "renewed"},
        }),
        json.dumps({
            "event_id": "e_init1", "event_type": "call.initiated",
            "workflow_id": "motor_renewal", "call_id": "c_alice_1",
            "occurred_at": "2026-10-01 10:00:00", "phone": "+91 98765 43210",
            "payload": {},
        }),
        json.dumps({
            "event_id": "e_conn1", "event_type": "call.connected",
            "workflow_id": "motor_renewal", "call_id": "c_alice_1",
            "occurred_at": "2026-10-01 10:00:10", "phone": "9876543210",
            "payload": {},
        }),
        json.dumps({
            "event_id": "e_set1", "event_type": "disposition.set",
            "workflow_id": "motor_renewal", "call_id": "c_alice_1",
            "occurred_at": "2026-10-01 10:02:00", "phone": "9876543210",
            "payload": {"disposition_code": "not_interested"},
        }),
        # Call 2 for Alice has NO disposition -> current_disposition must still be renewed (most recent call that has one)
        json.dumps({
            "event_id": "e_init2", "event_type": "call.initiated",
            "workflow_id": "motor_renewal", "call_id": "c_alice_2",
            "occurred_at": "2026-10-03 10:00:00", "phone": "9876543210",
            "payload": {},
        }),
        # Carol: connected, callback_requested + unknown disposition ignored
        json.dumps({
            "event_id": "e_c_init", "event_type": "call.initiated",
            "workflow_id": "motor_renewal", "call_id": "c_carol_1",
            "occurred_at": "2026-10-01 11:00:00", "phone": "9876543212",
            "payload": {},
        }),
        json.dumps({
            "event_id": "e_c_set", "event_type": "disposition.set",
            "workflow_id": "motor_renewal", "call_id": "c_carol_1",
            "occurred_at": "2026-10-01 11:02:00", "phone": "9876543212",
            "payload": {"disposition_code": "callback_requested"},
        }),
        json.dumps({
            "event_id": "e_c_bad", "event_type": "disposition.override",
            "workflow_id": "motor_renewal", "call_id": "c_carol_1",
            "occurred_at": "2026-10-01 12:00:00",
            "payload": {"disposition_code": "bogus_code"},
        }),
        # Dave: telephony failed
        json.dumps({
            "event_id": "e_d_init", "event_type": "call.initiated",
            "workflow_id": "motor_renewal", "call_id": "c_dave_1",
            "occurred_at": "2026-10-01 13:00:00", "phone": "9876543213",
            "payload": {},
        }),
        json.dumps({
            "event_id": "e_d_tf", "event_type": "call.telephony_failed",
            "workflow_id": "motor_renewal", "call_id": "c_dave_1",
            "occurred_at": "2026-10-01 13:00:05", "phone": "9876543213",
            "payload": {"reason": "sip_503"},
        }),
        # Frank (lead arrives in drop_2)
        json.dumps({
            "event_id": "e_f_init", "event_type": "call.initiated",
            "workflow_id": "motor_renewal", "call_id": "c_frank_1",
            "occurred_at": "2026-10-01T18:40:00Z", "phone": "9876543215",
            "payload": {},
        }),
        # Pure orphan
        json.dumps({
            "event_id": "e_orphan", "event_type": "call.initiated",
            "workflow_id": "motor_renewal", "call_id": "c_orphan",
            "occurred_at": "2026-10-01 15:00:00", "phone": "9999999999",
            "payload": {},
        }),
        "{not valid json",
    ]
    _write_file(d1 / "events" / "events_0001.jsonl", "\n".join(d1_events) + "\n")

    # Drop 2 events: duplicate of e_init1
    _write_file(
        d2 / "events" / "events_1001.jsonl",
        json.dumps({
            "event_id": "e_init1", "event_type": "call.initiated",
            "workflow_id": "motor_renewal", "call_id": "c_alice_1",
            "occurred_at": "2026-10-01 10:00:00", "phone": "+91 98765 43210",
            "payload": {},
        }) + "\n",
    )

    db_inc = tmp_path / "inc.sqlite"
    rep_inc = tmp_path / "rep_inc"
    ingest(d1, db_inc)
    ingest(d2, db_inc)
    # Re-ingest both drops to verify idempotency
    ingest(d1, db_inc)
    ingest(d2, db_inc)
    report(rep_inc, db_inc)

    # All-at-once comparison
    db_all = tmp_path / "all.sqlite"
    rep_all = tmp_path / "rep_all"
    ingest(tmp_path / "data", db_all)
    report(rep_all, db_all)

    for fname in ("batch_summary.csv", "daily_overview.csv", "lead_report.csv", "rejections.csv", "data_quality.json"):
        assert (rep_inc / fname).read_text(encoding="utf-8") == (rep_all / fname).read_text(encoding="utf-8")

    dq = json.loads((rep_inc / "data_quality.json").read_text(encoding="utf-8"))
    assert dq == {
        "lead_rows_total": 8,
        "lead_rows_rejected": 2,
        "rejections_by_reason": {"invalid_phone": 1, "missing_required:customer_name": 1},
        "duplicate_lead_rows_merged": 1,
        "field_warnings": 3,
        "events_total": 14,
        "malformed_events": 1,
        "duplicate_events_dropped": 1,
        "orphan_events": 1,
        "unknown_disposition_events": 1,
    }

    with (rep_inc / "lead_report.csv").open("r", encoding="utf-8") as f:
        by_ref = {r["external_ref"]: r for r in csv.DictReader(f)}

    assert by_ref["POL-1"]["batch_id"] == "batch_001"
    assert by_ref["POL-1"]["bucket"] == "completed"
    assert by_ref["POL-1"]["current_disposition"] == "renewed"
    assert by_ref["POL-1"]["ever_success"] == "true"
    assert by_ref["POL-1"]["phone_masked"] == "+91******3210"
    assert by_ref["POL-4"]["bucket"] == "in_progress_dialed"
    assert by_ref["POL-4"]["current_disposition"] == "callback_requested"
    assert by_ref["POL-5"]["bucket"] == "in_progress_telephony_failed"
    assert by_ref["POL-6"]["bucket"] == "yet_to_reach_out"
    assert by_ref["POL-7"]["bucket"] == "in_progress_dialed"
    assert by_ref["POL-7"]["last_call_at"] == "2026-10-02T00:10:00+05:30"

    # Verify NO plaintext PII exists anywhere in SQLite
    raw_db = sqlite3.connect(db_inc)
    dump = "\n".join(str(row) for row in raw_db.execute("SELECT * FROM leads").fetchall())
    dump += "\n".join(str(row) for row in raw_db.execute("SELECT * FROM events").fetchall())
    dump += "\n".join(str(row) for row in raw_db.execute("SELECT * FROM rejections").fetchall())
    raw_db.close()
    for secret in ("Alice Secret", "Alice Updated", "Bob Bad", "Carol Warn", "9876543210", "9876543212"):
        assert secret not in dump
