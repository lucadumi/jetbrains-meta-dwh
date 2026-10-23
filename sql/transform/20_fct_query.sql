-- Replace the [start_date, end_date] window. Rerunning a window is idempotent; a backfill is just a
-- wider window. query_date = date(end_time), which is also how raw is partitioned, so a window of
-- query dates maps onto exactly the same raw partitions.
DELETE FROM meta.fct_query
WHERE query_date BETWEEN ${start_date} AND ${end_date};

INSERT INTO meta.fct_query
WITH events AS (
    SELECT e.*,
           CAST(e.end_time AS date) AS query_date,
           -- Shared service accounts (Airflow, BI tools) tag queries with the team they run for.
           nullif(regexp_extract(array_join(e.client_tags, ','), 'team=([^,]+)', 1), '') AS tag_team_id,
           -- The event listener delivers at least once: keep one copy per query.
           row_number() OVER (PARTITION BY e.query_id ORDER BY e.ingested_at DESC) AS copy_no
    FROM raw.trino_query_events AS e
    WHERE e.end_time >= CAST(${start_date} AS timestamp(6))
      AND e.end_time <  CAST(${end_date} + INTERVAL '1' DAY AS timestamp(6))
)
SELECT q.query_id,
       q.query_date,
       q.create_time,
       q.end_time,
       q.user_name,
       -- Charge precedence: explicit (and valid) client tag > directory team as of the query date > unknown.
       coalesce(tag.team_id, u.team_id, 'unknown') AS team_id,
       CASE WHEN tag.team_id IS NOT NULL THEN 'client_tag'
            WHEN u.team_id IS NOT NULL THEN 'directory'
            ELSE 'unknown' END AS team_source,
       q.source,
       q.query_type,
       q.query_state,
       q.error_code,
       q.resource_group_id,
       q.cpu_time_ms,
       q.wall_time_ms,
       q.queued_time_ms,
       q.peak_user_memory_bytes AS peak_memory_bytes,
       q.cumulative_memory_byte_seconds / 1e9 AS memory_gb_seconds,
       q.physical_input_bytes,
       q.physical_input_rows,
       q.output_bytes,
       q.written_bytes,
       CAST(cardinality(q.inputs) AS integer) AS tables_read,
       q.cpu_time_ms / 3.6e6 * r.usd_per_cpu_hour
         + q.cumulative_memory_byte_seconds / 1e9 / 3600 * r.usd_per_memory_gb_hour AS est_cost_usd
FROM events AS q
LEFT JOIN meta.dim_team AS tag
       ON tag.team_id = q.tag_team_id
LEFT JOIN meta.dim_user AS u
       ON u.user_name = q.user_name
      AND q.query_date >= u.valid_from
      AND q.query_date <  u.valid_to
LEFT JOIN ref.cost_rate AS r
       ON q.query_date >= r.valid_from
      AND q.query_date <  r.valid_to
WHERE q.copy_no = 1;
