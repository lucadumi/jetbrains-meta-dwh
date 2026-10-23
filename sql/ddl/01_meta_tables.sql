-- Meta DWH tables that are maintained INCREMENTALLY (a window of days is replaced on every run).
-- meta.dim_team and meta.dim_user are small and fully rebuilt each run; see sql/transform/10_*, 11_*.

CREATE SCHEMA IF NOT EXISTS meta;

-- Grain: one row per query (query_id). The central fact; everything else hangs off it.
CREATE TABLE IF NOT EXISTS meta.fct_query (
    query_id              varchar,
    query_date            date,           -- date(end_time), UTC
    create_time           timestamp(6),
    end_time              timestamp(6),
    user_name             varchar,
    team_id               varchar,        -- team the query is charged to, resolved as of query_date
    team_source           varchar,        -- client_tag | directory | unknown
    source                varchar,
    query_type            varchar,
    query_state           varchar,
    error_code            varchar,
    resource_group_id     varchar,
    cpu_time_ms           bigint,
    wall_time_ms          bigint,
    queued_time_ms        bigint,
    peak_memory_bytes     bigint,
    memory_gb_seconds     double,
    physical_input_bytes  bigint,
    physical_input_rows   bigint,
    output_bytes          bigint,
    written_bytes         bigint,
    tables_read           integer,
    est_cost_usd          double
)
WITH (partitioning = ARRAY['month(query_date)'], sorted_by = ARRAY['team_id']);

-- Grain: one row per (query, table, access type). Bridge between queries and tables, so a query
-- touching N tables has N rows. cost_weight sums to 1 per query that touches any table, which
-- keeps resource attribution additive (no double counting) while user counts stay exact.
CREATE TABLE IF NOT EXISTS meta.fct_table_access (
    query_id              varchar,
    query_date            date,
    table_fqn             varchar,        -- catalog.schema.table
    access_type           varchar,        -- READ | WRITE
    user_name             varchar,
    team_id               varchar,
    query_state           varchar,
    columns_read          array(varchar),
    physical_input_bytes  bigint,
    cost_weight           double,
    attributed_cpu_ms     double,
    attributed_cost_usd   double
)
WITH (partitioning = ARRAY['month(query_date)'], sorted_by = ARRAY['table_fqn']);

-- Grain: one row per table ever seen in a query.
CREATE TABLE IF NOT EXISTS meta.dim_table (
    table_fqn        varchar,
    catalog_name     varchar,
    schema_name      varchar,
    table_name       varchar,
    owner_team_id    varchar,
    first_seen_date  date,
    last_seen_date   date
);

-- Grain: one row per (day, table). Kept forever, so history survives detail-data retention.
-- readers_hll is a HyperLogLog sketch of the reading users: merging sketches gives distinct
-- users over any period without going back to the detail rows (distinct counts are not additive).
CREATE TABLE IF NOT EXISTS meta.agg_table_usage_daily (
    usage_date            date,
    table_fqn             varchar,
    read_queries          bigint,
    write_queries         bigint,
    failed_queries        bigint,
    distinct_readers      bigint,
    distinct_writers      bigint,
    distinct_teams        bigint,
    readers_hll           varbinary,
    physical_input_bytes  bigint,
    attributed_cpu_ms     double,
    attributed_cost_usd   double
)
WITH (partitioning = ARRAY['year(usage_date)']);

-- Grain: one row per (day, team).
CREATE TABLE IF NOT EXISTS meta.agg_team_usage_daily (
    usage_date             date,
    team_id                varchar,
    queries                bigint,
    failed_queries         bigint,
    active_users           bigint,
    users_hll              varbinary,
    cpu_time_ms            bigint,
    wall_time_ms           bigint,
    queued_time_ms         bigint,
    memory_gb_seconds      double,
    max_peak_memory_bytes  bigint,
    physical_input_bytes   bigint,
    written_bytes          bigint,
    est_cost_usd           double
)
WITH (partitioning = ARRAY['year(usage_date)']);
