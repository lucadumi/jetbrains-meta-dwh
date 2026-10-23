-- Assumed SOURCE tables. The Meta DWH pipeline reads these but does not own them;
-- they are declared here so the transformations can be read (and tested) against a concrete shape.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS ref;

-- One row per Trino QueryCompletedEvent (successful AND failed queries), emitted by the
-- coordinator's event listener (Kafka/HTTP listener) and landed in Iceberg. Delivery is
-- at-least-once, so the same query_id can appear more than once.
CREATE TABLE IF NOT EXISTS raw.trino_query_events (
    query_id                        varchar,
    query_state                     varchar,          -- FINISHED | FAILED
    query_type                      varchar,          -- SELECT, INSERT, MERGE, DATA_DEFINITION, DESCRIBE, ...
    error_code                      varchar,
    user_name                       varchar,          -- session user the query ran as (after impersonation)
    source                          varchar,          -- client: DataGrip, superset, airflow, ...
    client_tags                     array(varchar),   -- e.g. ['team=ide-analytics', 'dag=daily_events']
    resource_group_id               varchar,
    create_time                     timestamp(6),     -- all timestamps UTC
    end_time                        timestamp(6),
    cpu_time_ms                     bigint,           -- summed over all workers
    wall_time_ms                    bigint,
    queued_time_ms                  bigint,
    peak_user_memory_bytes          bigint,
    cumulative_memory_byte_seconds  double,           -- integral of memory over time
    physical_input_bytes            bigint,           -- bytes read from storage (S3)
    physical_input_rows             bigint,
    output_bytes                    bigint,
    written_bytes                   bigint,
    -- Base tables read. Trino resolves views before execution, so views appear as their base tables.
    inputs  array(row(catalog_name varchar, schema_name varchar, table_name varchar,
                      columns array(varchar), physical_input_bytes bigint, physical_input_rows bigint)),
    -- Target of INSERT / CTAS / MERGE / UPDATE / DELETE; NULL for reads.
    output  row(catalog_name varchar, schema_name varchar, table_name varchar),
    ingested_at                     timestamp(6)
)
WITH (partitioning = ARRAY['day(end_time)']);

-- Daily full export of the identity directory (Okta/LDAP) UNION the git-managed registry of
-- service accounts and their owning team. One row per user per snapshot day.
CREATE TABLE IF NOT EXISTS ref.user_directory_snapshot (
    snapshot_date  date,
    user_name      varchar,   -- matches Trino's user_name
    display_name   varchar,
    user_type      varchar,   -- human | service
    team_id        varchar    -- primary team; exactly one per user per day
)
WITH (partitioning = ARRAY['year(snapshot_date)']);

-- Org structure from HR (current state only).
CREATE TABLE IF NOT EXISTS ref.team (
    team_id      varchar,
    team_name    varchar,
    department   varchar,
    cost_center  varchar
);

-- Data ownership: which team owns a schema. Maintained in git next to the table definitions.
CREATE TABLE IF NOT EXISTS ref.table_owner (
    catalog_name   varchar,
    schema_name    varchar,
    owner_team_id  varchar
);

-- Effective-dated prices used to turn resource usage into money. Derived from the cluster's
-- monthly bill divided by its capacity; valid_to is exclusive.
CREATE TABLE IF NOT EXISTS ref.cost_rate (
    valid_from              date,
    valid_to                date,
    usd_per_cpu_hour        double,
    usd_per_memory_gb_hour  double
);
