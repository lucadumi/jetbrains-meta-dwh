# Meta DWH: usage statistics for the Trino / Iceberg lakehouse

A star schema built from Trino's query-completion events. It answers "who used table X" and
"what did team Y consume", and keeps the history so we can see how those numbers change over time.

```
sql/ddl/          00_sources.sql      assumed inputs (raw events + reference data)
                  01_meta_tables.sql  Meta DWH tables
sql/transform/    10..60_*.sql        transformations, run in order for a window of days
sql/checks/       90_checks.sql       data-quality checks (run fails if any returns rows)
sql/queries/      examples.sql        the questions from the task, and a few more
pipeline/run.py   runs DDL -> transforms -> checks against Trino for a date window
tests/            end-to-end test: same SQL transpiled to DuckDB, on a hand-built scenario
```

## 1. Source and assumptions

**Raw data.** Every Trino coordinator has an event listener (Kafka or HTTP) that emits a
`QueryCompletedEvent` for every query, successful or failed. These land in the Iceberg table
`raw.trino_query_events`, partitioned by `day(end_time)`. The fields I use are the ones Trino
already reports: user, client `source` and `client_tags`, state and error, timings, CPU, memory,
bytes read and written, plus `inputs` (every base table read, with the columns used and bytes read
per table) and `output` (the written table). Delivery is at-least-once, so duplicates are expected.

**Users → teams.**
- `ref.user_directory_snapshot` is a daily full export of the identity directory (Okta/LDAP). It is
  combined with a git-managed registry that maps each service account to an owning team.
  Each user has exactly one primary team on any given day.
- Shared service accounts (Airflow, BI tools) charge the team they run for through a client tag,
  e.g. `X-Trino-Client-Tags: team=ide-analytics`. Precedence: **valid tag > directory team as of
  the query date > `unknown`**. A tag naming a team that doesn't exist is ignored.
- Users who aren't in the directory land on an explicit `unknown` team, so their usage still shows
  up in totals. A check alerts when this exceeds 5% of a day's CPU.

**Queries touching several tables** get one bridge row per table. Each table counts the user as a
user of that table (so user counts are exact), but the query's resources are *split* by a weight
that sums to 1 per query:
- reads are weighted by the bytes Trino actually read from each table, so a 1 TB fact table joined
  to a 1 MB dimension charges the fact table, not 50/50; when no bytes were read, the split is equal;
- a write-only query (`INSERT … VALUES`) charges its target table;
- in `INSERT … SELECT` / CTAS the target gets weight 0, because the work was reading the inputs.

**Other assumptions**
- *Views*: Trino reports the base tables after expanding views, so a view's usage is counted
  against its underlying tables. Usage at the view level would need the view references from the
  event's query metadata, loaded into another bridge table.
- *Excluded*: `system.*`, `information_schema`, and Iceberg metadata tables (`t$snapshots`) aren't
  data usage. Queries that touch no table (`SELECT 1`, `SHOW`) keep their cost in `fct_query` as
  *unattributed*.
- *Table identity is the name* (`catalog.schema.table`). A table that is dropped and re-created
  under the same name counts as the same table.
- *Time*: UTC, and a query belongs to the day it **finished**. This matches how raw is partitioned,
  so reprocessing a set of days touches exactly those raw partitions.
- *Failed queries* count towards usage and resources (they used the cluster) and are flagged so
  they can be filtered out.

**Additional metadata needed:** the directory snapshot and service-account registry (above),
`ref.team` (HR org structure), `ref.table_owner` (owning team per schema, kept in git), and
`ref.cost_rate` (effective-dated price per CPU-hour and GB-hour, derived from the cluster bill).

## 2. Data model

```
                      dim_team (team_id)
                         ▲            ▲
       dim_user (SCD2)   │            │
       user_name, team_id│            │ team_id (as of query date)
       valid_from/to ────┘            │
              ▲ user_name    ┌────────┴─────────┐ query_id   ┌──────────────────┐
              └──────────────│    fct_query     │◄───────────│ fct_table_access │──► dim_table
                             │ 1 row / query    │   1 : N    │ 1 row / query ×  │   table_fqn
                             └────────┬─────────┘            │ table × access   │   owner_team_id
                                      │                      └────────┬─────────┘
                                      ▼                               ▼
                           agg_team_usage_daily              agg_table_usage_daily
                           1 row / day × team                1 row / day × table
```

| Table | Grain | Purpose |
|---|---|---|
| `fct_query` | one query | resources: CPU, wall, queued, peak and cumulative memory, bytes read/written, estimated cost; user and the team resolved **at query time** |
| `fct_table_access` | query × table × READ/WRITE | bridge for multi-table queries: columns read, bytes per table, `cost_weight`, attributed CPU and cost |
| `dim_user` | user × validity period (SCD2) | user → team history, so moving teams doesn't rewrite last year's numbers |
| `dim_team` | team | org attributes, plus an `unknown` member |
| `dim_table` | table | owner team, first and last seen, used for "who depends on my tables" and to find unused tables |
| `agg_table_usage_daily` | day × table | read/write queries, distinct readers/writers/teams, HLL sketch of readers, attributed cost |
| `agg_team_usage_daily` | day × team | queries, failures, active users, summed resources, cost |

Design choices:
- **Natural keys** (`query_id`, `user_name`, `team_id`, `table_fqn`) instead of integer surrogate
  keys. Trino has no sequences, and the tables are rebuilt window by window, so surrogates would be
  extra state without much benefit. Time-dependent attributes are handled by an *as-of join* on
  `dim_user.valid_from/valid_to`.
- **The team is stored on the fact row**, resolved as of the query date. A query is charged to the
  team the user was in *when they ran it*, and dashboards don't have to repeat the SCD join.
- **Daily aggregates are kept forever**, while detail facts can be expired (e.g. after 13 months). The
  history the task asks for therefore costs very little to keep. Distinct counts don't add up
  across days, so the table aggregate also stores a **HyperLogLog sketch** of its readers. Merging
  the sketches gives distinct users over any period from the aggregate alone, with about 2% error.
  For exact numbers over recent periods, query `fct_table_access`.
- Facts are partitioned by `month(query_date)` and sorted by `team_id` / `table_fqn`. Daily
  partitions would create too many small files at our volume, and sorting lets Iceberg skip files
  using column min/max statistics.

## 3. Resource metrics

| Metric | Why |
|---|---|
| **CPU time** (summed over workers) | Main cost driver and the headline number. Unlike wall time it doesn't depend on concurrency, and it adds up across queries. |
| **Cumulative memory** (GB·s) | Memory held over time. A query holding 100 GB for an hour blocks capacity even with little CPU. |
| Peak memory | Capacity planning and spotting OOM risk. Not additive, so it is aggregated with `max`. |
| Physical input bytes | S3 scan volume, and a sign of missing partition pruning. Reported as is rather than priced: S3 request costs scale with files, not bytes. |
| Written bytes | Storage growth caused by the team. |
| Queued / wall time | Contention and user experience, not consumption. Kept for SLA analysis, not for cost. |
| **Estimated cost** | `cpu_hours × $/CPU-h + memory_GB_hours × $/GB-h`, using the rate valid on the query date. Rates are effective-dated, so a price change doesn't rewrite history, and costs can be recomputed if the cost model changes. |

## 4. How the data is updated

- **Nightly**, an Airflow task runs `pipeline/run.py` after raw ingestion. By default it reprocesses
  the **last 3 days**, which absorbs late events and listener retries.
- **Every step replaces its window**: `DELETE WHERE query_date BETWEEN start AND end`, then `INSERT`
  the recomputed rows, with duplicates removed by `query_id`. Running the same window twice gives
  the same result, and a **backfill** is the same command with a wider window
  (`--start 2026-01-01 --end 2026-06-30`). The same command fixes a bad mapping or a cost-model
  change.
- `dim_user` and `dim_team` are **fully rebuilt** from all snapshots on each run. They are small,
  and rebuilding avoids incremental-state bugs: fixing a snapshot and rerunning corrects the
  history. `dim_table` is updated with `MERGE` (first/last seen, current owner).
- **Data-quality checks** run after the load and fail it if any of these hold: duplicate
  `query_id`; cost weights not summing to 1; overlapping SCD2 ranges; too much unmapped CPU.
- Trade-off: Trino can't make the DELETE and the INSERT one atomic Iceberg commit, so readers could
  briefly see a window empty during the nightly run. If that matters, the job can write to an
  Iceberg branch and publish it in one step (write-audit-publish).
- Maintenance: weekly `ALTER TABLE … EXECUTE optimize` and `expire_snapshots` on the Meta DWH tables.

## 5. Example queries (full set in `sql/queries/examples.sql`)

```sql
-- How many distinct users queried table X during a period?
SELECT count(DISTINCT user_name) AS distinct_users
FROM meta.fct_table_access
WHERE table_fqn = 'iceberg.analytics.ide_events'
  AND access_type = 'READ'
  AND query_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31';

-- Same question for a long period, from the daily aggregate only (approximate)
SELECT cardinality(merge(CAST(readers_hll AS HyperLogLog))) AS approx_distinct_users
FROM meta.agg_table_usage_daily
WHERE table_fqn = 'iceberg.analytics.ide_events'
  AND usage_date BETWEEN DATE '2025-01-01' AND DATE '2026-09-30';

-- How many resources did team Y consume during that period?
SELECT sum(queries) AS queries,
       sum(cpu_time_ms) / 3.6e6 AS cpu_hours,
       sum(memory_gb_seconds) / 3600 AS memory_gb_hours,
       sum(physical_input_bytes) / 1e12 AS tb_scanned,
       sum(est_cost_usd) AS est_cost_usd
FROM meta.agg_team_usage_daily
WHERE team_id = 'ide-analytics'
  AND usage_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-31';
```

`examples.sql` also includes the history views: weekly distinct readers of a table, and each
team's monthly CPU and share of the cluster. It also has which tables a team's compute went to,
and which other teams depend on a team's tables.

## 6. Testing

`tests/test_pipeline.py` runs the real SQL end to end. It transpiles it from Trino to DuckDB with
sqlglot and executes it on a hand-built scenario: a duplicated event, a user changing team, a user
leaving and returning, a service account with a valid tag and one with a bogus tag, an unknown
user, a failed query, a query finishing after midnight, a multi-table join, an `INSERT … SELECT`,
an `INSERT … VALUES`, and `information_schema` / `$snapshots` reads. It asserts the team charged
for every query, every cost weight, the SCD2 ranges, the example answers, and that rerunning the
window, or a single day of it, leaves every table unchanged. Trino-only features (Iceberg table
properties, HLL, row-expanding `UNNEST`) get small DuckDB stand-ins in the test.

```
pip install duckdb sqlglot && python tests/test_pipeline.py
```

## 7. Possible extensions

- **Storage per table and team**: a daily snapshot of each table's Iceberg metadata (`$snapshots`
  summary, `$files`): bytes, file count, small-file ratio. This adds storage to compute in the
  cost picture.
- **Unused tables**: seed `dim_table` from `information_schema.tables` so tables that are *never*
  read also show up as candidates for deprecation.
- **Column-level usage** from `columns_read`, to find columns that are safe to drop.
