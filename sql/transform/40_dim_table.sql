-- Upsert every table accessed in the window. Ownership is refreshed on each touch so a schema
-- changing hands is picked up. Tables are identified by name: a dropped and re-created table with
-- the same name is treated as the same table.
MERGE INTO meta.dim_table AS t
USING (
    SELECT a.table_fqn,
           split_part(a.table_fqn, '.', 1) AS catalog_name,
           split_part(a.table_fqn, '.', 2) AS schema_name,
           split_part(a.table_fqn, '.', 3) AS table_name,
           min(o.owner_team_id) AS owner_team_id,
           min(a.query_date) AS first_seen_date,
           max(a.query_date) AS last_seen_date
    FROM meta.fct_table_access AS a
    LEFT JOIN ref.table_owner AS o
           ON o.catalog_name = split_part(a.table_fqn, '.', 1)
          AND o.schema_name = split_part(a.table_fqn, '.', 2)
    WHERE a.query_date BETWEEN ${start_date} AND ${end_date}
    GROUP BY a.table_fqn
) AS s
ON t.table_fqn = s.table_fqn
WHEN MATCHED THEN UPDATE SET
    owner_team_id = s.owner_team_id,
    first_seen_date = least(t.first_seen_date, s.first_seen_date),
    last_seen_date = greatest(t.last_seen_date, s.last_seen_date)
WHEN NOT MATCHED THEN INSERT
    (table_fqn, catalog_name, schema_name, table_name, owner_team_id, first_seen_date, last_seen_date)
    VALUES (s.table_fqn, s.catalog_name, s.schema_name, s.table_name, s.owner_team_id, s.first_seen_date, s.last_seen_date);
