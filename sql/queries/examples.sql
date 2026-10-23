-- Example questions answered from the Meta DWH. Table, team and dates are placeholders.

-- Q1. How many distinct users queried table X during a period? (exact, from the bridge fact)
SELECT count(DISTINCT user_name) AS distinct_users
FROM meta.fct_table_access
WHERE table_fqn = 'iceberg.analytics.ide_events'
  AND access_type = 'READ'
  AND query_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-03';

-- Q1b. Same question for a long period, answered from the daily aggregate only (approximate,
-- ~2% standard error): merging HyperLogLog sketches avoids rescanning years of detail rows.
SELECT cardinality(merge(CAST(readers_hll AS HyperLogLog))) AS approx_distinct_users
FROM meta.agg_table_usage_daily
WHERE table_fqn = 'iceberg.analytics.ide_events'
  AND usage_date BETWEEN DATE '2025-01-01' AND DATE '2026-09-30';

-- Q1c. Historical perspective: weekly distinct readers of table X and the teams they come from.
SELECT date_trunc('week', query_date) AS week,
       count(DISTINCT user_name) AS distinct_users,
       count(DISTINCT team_id) AS distinct_teams,
       count(DISTINCT query_id) AS queries
FROM meta.fct_table_access
WHERE table_fqn = 'iceberg.analytics.ide_events'
  AND access_type = 'READ'
  AND query_date >= DATE '2026-04-01'
GROUP BY 1
ORDER BY 1;

-- Q2. How many resources did team Y consume during a period?
SELECT t.team_name,
       sum(a.queries) AS queries,
       sum(a.failed_queries) AS failed_queries,
       sum(a.cpu_time_ms) / 3.6e6 AS cpu_hours,
       sum(a.memory_gb_seconds) / 3600 AS memory_gb_hours,
       sum(a.physical_input_bytes) / 1e12 AS tb_scanned,
       sum(a.written_bytes) / 1e12 AS tb_written,
       max(a.max_peak_memory_bytes) / 1e9 AS max_peak_memory_gb,
       sum(a.est_cost_usd) AS est_cost_usd
FROM meta.agg_team_usage_daily AS a
JOIN meta.dim_team AS t ON t.team_id = a.team_id
WHERE a.team_id = 'ide-analytics'
  AND a.usage_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-03'
GROUP BY t.team_name;

-- Q2b. Historical perspective: monthly CPU per team, with each team's share of the cluster.
SELECT date_trunc('month', usage_date) AS month,
       team_id,
       sum(cpu_time_ms) / 3.6e6 AS cpu_hours,
       sum(cpu_time_ms) * 1.0e0 / sum(sum(cpu_time_ms)) OVER (PARTITION BY date_trunc('month', usage_date)) AS cpu_share
FROM meta.agg_team_usage_daily
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

-- Q2c. Where did team Y's compute go? Top tables by attributed cost.
SELECT table_fqn,
       count(DISTINCT query_id) AS queries,
       sum(attributed_cpu_ms) / 3.6e6 AS cpu_hours,
       sum(attributed_cost_usd) AS cost_usd
FROM meta.fct_table_access
WHERE team_id = 'ide-analytics'
  AND query_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-03'
GROUP BY table_fqn
ORDER BY cost_usd DESC
LIMIT 10;

-- Q3. Who depends on the tables team Y owns? (cross-team consumers, useful before a schema change)
SELECT d.table_fqn,
       a.team_id AS consumer_team,
       count(DISTINCT a.user_name) AS users,
       count(DISTINCT a.query_id) AS queries
FROM meta.fct_table_access AS a
JOIN meta.dim_table AS d ON d.table_fqn = a.table_fqn
WHERE d.owner_team_id = 'ide-analytics'
  AND a.team_id <> d.owner_team_id
  AND a.access_type = 'READ'
  AND a.query_date >= DATE '2026-07-01'
GROUP BY 1, 2
ORDER BY queries DESC;
