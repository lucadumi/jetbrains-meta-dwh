-- Failed queries are included: they consumed the cluster all the same.
DELETE FROM meta.agg_team_usage_daily
WHERE usage_date BETWEEN ${start_date} AND ${end_date};

INSERT INTO meta.agg_team_usage_daily
SELECT query_date AS usage_date,
       team_id,
       count(*) AS queries,
       count_if(query_state = 'FAILED') AS failed_queries,
       count(DISTINCT user_name) AS active_users,
       CAST(approx_set(user_name) AS varbinary) AS users_hll,
       sum(cpu_time_ms) AS cpu_time_ms,
       sum(wall_time_ms) AS wall_time_ms,
       sum(queued_time_ms) AS queued_time_ms,
       sum(memory_gb_seconds) AS memory_gb_seconds,
       max(peak_memory_bytes) AS max_peak_memory_bytes,
       sum(physical_input_bytes) AS physical_input_bytes,
       sum(written_bytes) AS written_bytes,
       sum(est_cost_usd) AS est_cost_usd
FROM meta.fct_query
WHERE query_date BETWEEN ${start_date} AND ${end_date}
GROUP BY query_date, team_id;
