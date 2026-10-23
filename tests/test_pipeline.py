"""End-to-end test of the transformations on a hand-built scenario.

The production SQL is Trino; here it is transpiled to DuckDB with sqlglot and run in memory, so the
modelling logic (dedup, SCD2 as-of joins, cost split, idempotency, example answers) is checked
without a cluster. Three Trino-only features get a DuckDB stand-in: Iceberg table properties are
dropped, HyperLogLog sketches become NULL, and the row-expanding UNNEST is rewritten.

    pip install duckdb sqlglot && python tests/test_pipeline.py
"""
from __future__ import annotations

import datetime as dt
import logging
import re
import sys
from pathlib import Path

import duckdb
import sqlglot

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))
import run  # noqa: E402

START, END = dt.date(2026, 10, 1), dt.date(2026, 10, 3)
IDE_EVENTS = "iceberg.analytics.ide_events"

logging.getLogger("sqlglot").setLevel(logging.ERROR)  # "unsupported property" = Iceberg table properties


def duckdb_executor(con: duckdb.DuckDBPyConnection) -> run.Execute:
    def execute(sql: str) -> list:
        sql = re.sub(r"CAST\(approx_set\(.*?\) AS varbinary\)", "CAST(NULL AS varbinary)", sql)
        sql = re.sub(r"CROSS JOIN UNNEST\((\w+)\.(\w+)\) AS (\w+) \([^)]*\)",
                     r"CROSS JOIN (SELECT UNNEST(\1.\2) AS \3) AS _unnested", sql)
        for stmt in sqlglot.transpile(sql, read="trino", write="duckdb"):
            con.execute(stmt)
        return con.fetchall() if con.description else []

    return execute


def lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    if isinstance(v, dt.datetime):
        return f"TIMESTAMP '{v.isoformat(sep=' ')}'"
    if isinstance(v, dt.date):
        return f"DATE '{v.isoformat()}'"
    if isinstance(v, list):
        return "[" + ", ".join(lit(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{" + ", ".join(f"'{k}': {lit(x)}" for k, x in v.items()) + "}"
    return repr(v)


def table(catalog, schema, name, nbytes=0, columns=("id",)):
    return {"catalog_name": catalog, "schema_name": schema, "table_name": name,
            "columns": list(columns), "physical_input_bytes": nbytes, "physical_input_rows": nbytes // 10}


def ts(day, hh=12, mm=0):
    return dt.datetime(2026, 10, day, hh, mm) if day > 0 else dt.datetime(2026, 9, 30, hh, mm)


# (query_id, user, end_time, state, type, cpu_ms, client_tags, inputs, output)
QUERIES = [
    ("q1", "alice", ts(1), "FINISHED", "SELECT", 3_600_000, [],
     [table("iceberg", "analytics", "ide_events", 900), table("iceberg", "analytics", "dim_product", 100)], None),
    ("q2", "bob", ts(1), "FINISHED", "SELECT", 10_000, [],
     [table("iceberg", "analytics", "ide_events", 0)], None),
    # bob changed team on 10-02; this query started before midnight but finished after it
    ("q3", "bob", ts(2, 0, 10), "FINISHED", "SELECT", 20_000, [],
     [table("iceberg", "analytics", "ide_events", 50)], None),
    # shared service account charged through its client tag
    ("q4", "airflow", ts(2), "FINISHED", "INSERT", 500_000, ["team=ide-analytics", "dag=load_events"],
     [table("iceberg", "raw", "ide_events_raw", 500)],
     {"catalog_name": "iceberg", "schema_name": "analytics", "table_name": "ide_events"}),
    # write-only query with a tag that names no real team: falls back to the directory
    ("q5", "airflow", ts(2), "FINISHED", "INSERT", 1_000, ["team=bogus"], [],
     {"catalog_name": "iceberg", "schema_name": "ops", "table_name": "audit_log"}),
    ("q6", "dave", ts(3), "FAILED", "SELECT", 1_000, [],
     [table("iceberg", "analytics", "ide_events", 10)], None),
    ("q7", "alice", ts(3), "FINISHED", "DESCRIBE", 100, [],
     [table("iceberg", "information_schema", "tables", 0)], None),
    ("q8", "carol", ts(3), "FINISHED", "SELECT", 60_000, [],
     [table("iceberg", "analytics", "ide_events", 70), table("iceberg", "analytics", "ide_events$snapshots", 5)], None),
    ("q9", "alice", ts(0), "FINISHED", "SELECT", 99_999, [],
     [table("iceberg", "analytics", "ide_events", 1)], None),     # 09-30: outside the window
]

DIRECTORY = {  # snapshot day -> {user: (type, team)}
    1: {"alice": ("human", "ide-analytics"), "bob": ("human", "ide-analytics"),
        "carol": ("human", "ide-analytics"), "airflow": ("service", "data-platform")},
    2: {"alice": ("human", "ide-analytics"), "bob": ("human", "kotlin-team"),
        "airflow": ("service", "data-platform")},                  # carol missing for a day
    3: {"alice": ("human", "ide-analytics"), "bob": ("human", "kotlin-team"),
        "carol": ("human", "ide-analytics"), "airflow": ("service", "data-platform")},
}


def seed(execute: run.Execute, con: duckdb.DuckDBPyConnection) -> None:
    for stmt in run.statements((ROOT / "sql" / "ddl" / "00_sources.sql").read_text()):
        execute(stmt)
    for i, (qid, user, end, state, qtype, cpu, tags, inputs, output) in enumerate(QUERIES):
        row = [qid, state, qtype, None, user, "test", tags, "global", end - dt.timedelta(minutes=30), end,
               cpu, cpu // 4, 5, 10**9, 2.0e12, sum(t["physical_input_bytes"] for t in inputs), 1, 1,
               100 if output else 0, inputs, output, end]
        copies = 2 if qid == "q3" else 1                          # at-least-once delivery
        for c in range(copies):
            row[-1] = end + dt.timedelta(minutes=c)
            con.execute(f"INSERT INTO raw.trino_query_events VALUES ({', '.join(lit(v) for v in row)})")
    for day, users in DIRECTORY.items():
        for user, (utype, team) in users.items():
            con.execute(f"INSERT INTO ref.user_directory_snapshot VALUES "
                        f"({lit(dt.date(2026, 10, day))}, {lit(user)}, {lit(user.title())}, {lit(utype)}, {lit(team)})")
    con.execute("INSERT INTO ref.team VALUES ('ide-analytics', 'IDE Analytics', 'Product', 'CC1'), "
                "('kotlin-team', 'Kotlin', 'Product', 'CC2'), ('data-platform', 'Data Platform', 'Infra', 'CC3')")
    con.execute("INSERT INTO ref.table_owner VALUES ('iceberg', 'analytics', 'ide-analytics'), "
                "('iceberg', 'raw', 'data-platform')")
    con.execute("INSERT INTO ref.cost_rate VALUES (DATE '2026-01-01', DATE '9999-12-31', 0.05, 0.005)")


def rows(con, sql) -> list:
    return con.execute(sql).fetchall()


def snapshot(con) -> dict:
    return {t: rows(con, f"SELECT * FROM meta.{t} ORDER BY ALL") for t in
            ("fct_query", "fct_table_access", "dim_table", "agg_table_usage_daily", "agg_team_usage_daily")}


def examples(execute) -> list[list]:
    sql = (ROOT / "sql" / "queries" / "examples.sql").read_text()
    return [execute(s) for s in run.statements(sql) if "HyperLogLog" not in s]


def main() -> None:
    con = duckdb.connect()
    execute = duckdb_executor(con)
    seed(execute, con)
    run.load(execute, START, END)

    # Grain and dedup: q3 delivered twice, q9 outside the window.
    q = dict(rows(con, "SELECT query_id, team_id FROM meta.fct_query"))
    assert sorted(q) == [f"q{i}" for i in range(1, 9)], sorted(q)

    # Team resolution: SCD2 as of query date, valid client tag wins, bogus tag ignored, unknown user.
    assert q == {"q1": "ide-analytics", "q2": "ide-analytics", "q3": "kotlin-team", "q4": "ide-analytics",
                 "q5": "data-platform", "q6": "unknown", "q7": "ide-analytics", "q8": "ide-analytics"}, q
    assert rows(con, "SELECT query_date FROM meta.fct_query WHERE query_id = 'q3'") == [(dt.date(2026, 10, 2),)]

    # Bridge: system/information_schema and $metadata tables dropped; weights follow bytes.
    w = {(r[0], r[1], r[2]): round(r[3], 6) for r in rows(
        con, "SELECT query_id, table_fqn, access_type, cost_weight FROM meta.fct_table_access")}
    assert w == {
        ("q1", IDE_EVENTS, "READ"): 0.9, ("q1", "iceberg.analytics.dim_product", "READ"): 0.1,
        ("q2", IDE_EVENTS, "READ"): 1.0, ("q3", IDE_EVENTS, "READ"): 1.0,
        ("q4", "iceberg.raw.ide_events_raw", "READ"): 1.0, ("q4", IDE_EVENTS, "WRITE"): 0.0,
        ("q5", "iceberg.ops.audit_log", "WRITE"): 1.0,
        ("q6", IDE_EVENTS, "READ"): 1.0, ("q8", IDE_EVENTS, "READ"): 1.0,
    }, w

    # dim_user: bob changed team, carol left for one snapshot and came back.
    versions = rows(con, "SELECT user_name, team_id, valid_from, valid_to FROM meta.dim_user "
                         "WHERE user_name IN ('bob', 'carol') ORDER BY ALL")
    assert versions == [
        ("bob", "ide-analytics", dt.date(1970, 1, 1), dt.date(2026, 10, 2)),
        ("bob", "kotlin-team", dt.date(2026, 10, 2), dt.date(9999, 12, 31)),
        ("carol", "ide-analytics", dt.date(1970, 1, 1), dt.date(2026, 10, 2)),
        ("carol", "ide-analytics", dt.date(2026, 10, 3), dt.date(9999, 12, 31)),
    ], versions

    assert rows(con, "SELECT owner_team_id, first_seen_date, last_seen_date FROM meta.dim_table "
                     f"WHERE table_fqn = '{IDE_EVENTS}'") == [("ide-analytics", START, END)]

    # Example queries answer the two questions from the task.
    results = examples(execute)
    assert results[0] == [(4,)], results[0]                     # alice, bob, dave, carol read ide_events
    ide_cpu_ms = sum(r[5] for r in QUERIES if r[0] in {"q1", "q2", "q4", "q7", "q8"})
    team = results[2][0]
    assert team[0] == "IDE Analytics" and team[1] == 5 and abs(team[3] - ide_cpu_ms / 3.6e6) < 1e-9, team

    # Idempotency: rerunning the full window, or reprocessing one day of it, changes nothing.
    before = snapshot(con)
    run.load(execute, START, END)
    run.load(execute, dt.date(2026, 10, 2), dt.date(2026, 10, 2))
    assert snapshot(con) == before

    print("all checks passed")
    for name, res in zip(["Q1", "Q1c", "Q2", "Q2b", "Q2c", "Q3"], results):
        print(f"{name}: {res}")


if __name__ == "__main__":
    main()
