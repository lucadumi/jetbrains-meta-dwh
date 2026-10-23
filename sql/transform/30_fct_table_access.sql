-- Explode each query into one row per table it read or wrote, and split its cost across them.
--
-- Multi-table queries: every table gets a row, so "distinct users of table X" counts the user
-- once per table they touched. Resources are NOT copied to each table: cost_weight splits them.
--   * reads, weighted by the bytes Trino actually read from each table (a 1 TB fact joined to a
--     1 MB dimension charges the fact, not 50/50); equal split when no bytes were read
--   * a write-only query (INSERT ... VALUES) charges its target table
--   * otherwise the written table gets weight 0 (the work was reading the inputs)
-- Queries that touch no table (SELECT 1, SHOW, system/information_schema) produce no rows here;
-- their cost stays in fct_query as unattributed.
DELETE FROM meta.fct_table_access
WHERE query_date BETWEEN ${start_date} AND ${end_date};

INSERT INTO meta.fct_table_access
WITH events AS (
    SELECT e.query_id,
           e.inputs,
           e.output,
           row_number() OVER (PARTITION BY e.query_id ORDER BY e.ingested_at DESC) AS copy_no
    FROM raw.trino_query_events AS e
    WHERE e.end_time >= CAST(${start_date} AS timestamp(6))
      AND e.end_time <  CAST(${end_date} + INTERVAL '1' DAY AS timestamp(6))
),
reads AS (
    SELECT e.query_id,
           i.catalog_name || '.' || i.schema_name || '.' || i.table_name AS table_fqn,
           'READ' AS access_type,
           i.columns,
           i.physical_input_bytes
    FROM events AS e
    CROSS JOIN UNNEST(e.inputs) AS i (catalog_name, schema_name, table_name, columns, physical_input_bytes, physical_input_rows)
    WHERE e.copy_no = 1
      AND i.catalog_name <> 'system'
      AND i.schema_name <> 'information_schema'
      AND strpos(i.table_name, '$') = 0          -- Iceberg metadata tables, e.g. "events$snapshots"
),
writes AS (
    SELECT e.query_id,
           e.output.catalog_name || '.' || e.output.schema_name || '.' || e.output.table_name AS table_fqn,
           'WRITE' AS access_type,
           CAST(NULL AS array(varchar)) AS columns,
           CAST(NULL AS bigint) AS physical_input_bytes
    FROM events AS e
    WHERE e.copy_no = 1
      AND e.output IS NOT NULL
),
accesses AS (
    -- A table scanned twice in one query (self-join) is still one access.
    SELECT query_id,
           table_fqn,
           access_type,
           array_distinct(flatten(array_agg(coalesce(columns, ARRAY[])))) AS columns_read,
           sum(physical_input_bytes) AS physical_input_bytes
    FROM (SELECT * FROM reads UNION ALL SELECT * FROM writes) AS a
    GROUP BY query_id, table_fqn, access_type
),
weighted AS (
    SELECT a.*,
           sum(CASE WHEN access_type = 'READ' THEN 1 ELSE 0 END) OVER (PARTITION BY query_id) AS n_reads,
           sum(CASE WHEN access_type = 'WRITE' THEN 1 ELSE 0 END) OVER (PARTITION BY query_id) AS n_writes,
           sum(CASE WHEN access_type = 'READ' THEN coalesce(physical_input_bytes, 0) ELSE 0 END)
               OVER (PARTITION BY query_id) AS read_bytes_total
    FROM accesses AS a
),
weights AS (
    SELECT w.*,
           CASE WHEN access_type = 'READ' AND read_bytes_total > 0
                    THEN CAST(coalesce(physical_input_bytes, 0) AS double) / read_bytes_total
                WHEN access_type = 'READ'
                    THEN 1.0e0 / n_reads
                WHEN n_reads = 0
                    THEN 1.0e0 / n_writes
                ELSE 0.0e0 END AS cost_weight
    FROM weighted AS w
)
SELECT f.query_id,
       q.query_date,
       f.table_fqn,
       f.access_type,
       q.user_name,
       q.team_id,
       q.query_state,
       f.columns_read,
       f.physical_input_bytes,
       f.cost_weight,
       q.cpu_time_ms * f.cost_weight AS attributed_cpu_ms,
       q.est_cost_usd * f.cost_weight AS attributed_cost_usd
FROM weights AS f
JOIN meta.fct_query AS q
  ON q.query_id = f.query_id
 AND q.query_date BETWEEN ${start_date} AND ${end_date};
