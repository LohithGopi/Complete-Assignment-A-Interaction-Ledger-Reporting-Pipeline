"""CLI entrypoint: `python -m solution ingest <path>` / `python -m solution report --out <dir>`."""
from __future__ import annotations

import argparse
from pathlib import Path

from solution.pipeline import DEFAULT_SCHEMAS_DIR, ingest, report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m solution")
    ap.add_argument("--db", default="solution.sqlite", help="Path to SQLite database file")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_ing = sub.add_parser("ingest", help="Ingest a drop directory (or directory of drops)")
    p_ing.add_argument("path", help="Drop directory (e.g. data/drop_1)")
    p_ing.add_argument("--schemas", default=str(DEFAULT_SCHEMAS_DIR), help="Schemas directory")

    p_rep = sub.add_parser("report", help="Generate the 5 output report files")
    p_rep.add_argument("--out", required=True, help="Output directory")

    args = ap.parse_args(argv)
    if args.cmd == "ingest":
        ingest(args.path, args.db, Path(args.schemas))
    elif args.cmd == "report":
        report(args.out, args.db)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
