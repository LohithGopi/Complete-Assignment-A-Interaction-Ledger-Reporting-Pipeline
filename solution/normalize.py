"""Schema loading and field/timestamp normalization per Assignment A spec."""
from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

IST = timezone(timedelta(hours=5, minutes=30))

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
_CURRENCY_RE = re.compile(r"^(?:₹|rs\.?|inr)\s*", re.IGNORECASE)
_DECIMAL_RE = re.compile(r"^\d+(?:\.\d+)?$")
_DATE_YMD_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_DATE_DMY_RE = re.compile(r"^(\d{2})[/-](\d{2})[/-](\d{4})$")
_DATE_DMONY_RE = re.compile(r"^(\d{2})-([A-Za-z]{3})-(\d{4})$")


def normalize_header(raw: str) -> str:
    return re.sub(r"[_\s]+", " ", raw.strip().lower())


def normalize_phone(raw: str) -> str | None:
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) == 10 and digits[0] in "6789":
        return f"+91{digits}"
    return None


def normalize_date(raw: str) -> str | None:
    s = raw.strip()
    if m := _DATE_YMD_RE.match(s):
        y, mo, d = int(m[1]), int(m[2]), int(m[3])
    elif m := _DATE_DMY_RE.match(s):
        d, mo, y = int(m[1]), int(m[2]), int(m[3])
    elif m := _DATE_DMONY_RE.match(s):
        d, mon_s, y = int(m[1]), m[2].lower(), int(m[3])
        mo = _MONTHS.get(mon_s)
        if mo is None:
            return None
    else:
        return None
    try:
        return datetime(y, mo, d).strftime("%Y-%m-%d")
    except ValueError:
        return None


def normalize_decimal(raw: str) -> str | None:
    s = _CURRENCY_RE.sub("", raw.strip()).replace(",", "").strip()
    if not _DECIMAL_RE.match(s):
        return None
    try:
        val = Decimal(s).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return None
    if val < 0:
        return None
    return f"{val:.2f}"


def normalize_enum(raw: str, allowed: list[str]) -> str | None:
    v = raw.strip().lower()
    return v if v in {a.lower() for a in allowed} else None


def parse_timestamp_ist(raw: str) -> datetime:
    """Parse UTC (...Z), offset (...+HH:MM), or naive (assumed IST) into aware IST datetime."""
    s = raw.strip()
    if s.endswith("Z"):
        dt = datetime.fromisoformat(s[:-1] + "+00:00")
    else:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def load_schemas(schemas_dir: Path) -> dict[str, dict]:
    schemas: dict[str, dict] = {}
    for p in sorted(schemas_dir.glob("*.json")):
        data = json.loads(p.read_text(encoding="utf-8"))
        alias_map: dict[str, str] = {}
        ref_field = None
        for f in data["fields"]:
            alias_map[normalize_header(f["name"])] = f["name"]
            for a in f.get("aliases", []):
                alias_map[normalize_header(a)] = f["name"]
            if f.get("reference"):
                ref_field = f["name"]
        data["_alias_map"] = alias_map
        data["_ref_field"] = ref_field
        schemas[data["workflow_id"]] = data
    return schemas


def load_dispositions(schemas_dir: Path) -> list[tuple[str, str, int, int]]:
    rows: list[tuple[str, str, int, int]] = []
    with (schemas_dir / "dispositions.csv").open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            rows.append((
                row["workflow_id"].strip(),
                row["code"].strip(),
                1 if row["is_terminal"].strip().lower() == "true" else 0,
                1 if row["is_success"].strip().lower() == "true" else 0,
            ))
    return rows
