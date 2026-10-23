"""Load the Meta DWH for a window of query dates.

Every step replaces the window it is given, so the job is idempotent: the nightly run reprocesses
the last few days (to absorb late events), and a backfill is the same command with a wider window.

    python pipeline/run.py                                   # nightly: yesterday + lookback
    python pipeline/run.py --start 2026-01-01 --end 2026-06-30   # backfill / reprocess

Scheduled from Airflow as a single daily task after the raw ingestion task.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import re
from pathlib import Path
from typing import Callable, Iterable

ROOT = Path(__file__).resolve().parent.parent
DDL = sorted((ROOT / "sql" / "ddl").glob("01_*.sql"))
TRANSFORMS = sorted((ROOT / "sql" / "transform").glob("*.sql"))
CHECKS = sorted((ROOT / "sql" / "checks").glob("*.sql"))

DEFAULT_LOOKBACK_DAYS = 3

# execute(sql) runs one statement and returns its rows (empty list for DDL/DML).
Execute = Callable[[str], list]


def render(sql: str, start: dt.date, end: dt.date) -> str:
    return (sql.replace("${start_date}", f"DATE '{start.isoformat()}'")
               .replace("${end_date}", f"DATE '{end.isoformat()}'"))


def statements(sql: str) -> Iterable[str]:
    """Split a file into statements. Our SQL has no '--' or ';' inside string literals, so dropping
    comments and splitting on ';' is enough."""
    code = re.sub(r"--[^\n]*", "", sql)
    for chunk in code.split(";"):
        if chunk.strip():
            yield chunk.strip()


def run_files(files: list[Path], execute: Execute, start: dt.date, end: dt.date) -> list[tuple[str, list]]:
    results = []
    for path in files:
        for stmt in statements(render(path.read_text(), start, end)):
            results.append((path.name, execute(stmt)))
    return results


def load(execute: Execute, start: dt.date, end: dt.date) -> None:
    if start > end:
        raise ValueError(f"empty window: {start} > {end}")
    run_files(DDL, execute, start, end)
    run_files(TRANSFORMS, execute, start, end)
    failures = [(name, rows) for name, rows in run_files(CHECKS, execute, start, end) if rows]
    if failures:
        raise RuntimeError(f"data-quality checks failed: {failures}")


def trino_executor() -> Execute:
    import trino  # imported here so the tests do not need the client installed

    conn = trino.dbapi.connect(
        host=os.environ["TRINO_HOST"],
        port=int(os.environ.get("TRINO_PORT", "443")),
        user=os.environ.get("TRINO_USER", "meta-dwh"),
        catalog=os.environ.get("TRINO_CATALOG", "iceberg"),
        http_scheme="https",
        client_tags=["team=data-platform", "job=meta-dwh"],
    )

    def execute(sql: str) -> list:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    return execute


def main() -> None:
    yesterday = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=dt.date.fromisoformat,
                        default=yesterday - dt.timedelta(days=DEFAULT_LOOKBACK_DAYS - 1))
    parser.add_argument("--end", type=dt.date.fromisoformat, default=yesterday)
    args = parser.parse_args()
    load(trino_executor(), args.start, args.end)
    print(f"meta DWH loaded for {args.start}..{args.end}")


if __name__ == "__main__":
    main()
