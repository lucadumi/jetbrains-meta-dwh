DELETE FROM meta.agg_table_usage_daily
WHERE usage_date BETWEEN ${start_date} AND ${end_date};

INSERT INTO meta.agg_table_usage_daily
SELECT query_date AS usage_date,
       table_fqn,
       count(DISTINCT CASE WHEN access_type = 'READ' THEN query_id END) AS read_queries,
       count(DISTINCT CASE WHEN access_type = 'WRITE' THEN query_id END) AS write_queries,
       count(DISTINCT CASE WHEN query_state = 'FAILED' THEN query_id END) AS failed_queries,
       count(DISTINCT CASE WHEN access_type = 'READ' THEN user_name END) AS distinct_readers,
       count(DISTINCT CASE WHEN access_type = 'WRITE' THEN user_name END) AS distinct_writers,
       count(DISTINCT team_id) AS distinct_teams,
       CAST(approx_set(CASE WHEN access_type = 'READ' THEN user_name END) AS varbinary) AS readers_hll,
       sum(physical_input_bytes) AS physical_input_bytes,
       sum(attributed_cpu_ms) AS attributed_cpu_ms,
       sum(attributed_cost_usd) AS attributed_cost_usd
FROM meta.fct_table_access
WHERE query_date BETWEEN ${start_date} AND ${end_date}
GROUP BY query_date, table_fqn;
