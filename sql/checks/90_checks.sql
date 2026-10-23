-- Data-quality checks run after every load. Each query returns the offending rows; the run fails
-- if any check returns anything.

-- fct_query grain: one row per query_id.
SELECT 'duplicate query_id' AS failed_check, query_id
FROM meta.fct_query
WHERE query_date BETWEEN ${start_date} AND ${end_date}
GROUP BY query_id
HAVING count(*) > 1;

-- Attribution must be additive: weights sum to 1 for every query that touched a table.
SELECT 'cost weights do not sum to 1' AS failed_check, query_id
FROM meta.fct_table_access
WHERE query_date BETWEEN ${start_date} AND ${end_date}
GROUP BY query_id
HAVING abs(sum(cost_weight) - 1) > 1e-9;

-- dim_user validity ranges must not overlap, or queries would be charged to two teams.
SELECT 'overlapping dim_user versions' AS failed_check, a.user_name
FROM meta.dim_user AS a
JOIN meta.dim_user AS b
  ON a.user_name = b.user_name
 AND a.valid_from < b.valid_from
 AND b.valid_from < a.valid_to;

-- Unmapped usage is expected to be small; alert when more than 5% of a day's CPU has no team.
SELECT 'over 5% of CPU unmapped' AS failed_check, CAST(usage_date AS varchar) AS usage_date
FROM meta.agg_team_usage_daily
WHERE usage_date BETWEEN ${start_date} AND ${end_date}
GROUP BY usage_date
HAVING sum(CASE WHEN team_id = 'unknown' THEN cpu_time_ms ELSE 0 END) > 0.05 * sum(cpu_time_ms);
