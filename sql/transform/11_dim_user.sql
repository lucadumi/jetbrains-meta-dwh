-- SCD type 2 user -> team history, rebuilt from all directory snapshots on every run.
-- Rebuilding is cheap (users x team changes is small) and avoids incremental-state bugs:
-- fixing a bad snapshot and rerunning is enough to correct history.
--
-- A new version starts when the team changes or the user reappears after missing from a snapshot.
-- Gaps are measured in snapshots, not calendar days, so a failed export day does not split history.
-- valid_to is exclusive; the first version is stretched back to 1970 so queries from before the
-- first snapshot still get the earliest known team.
CREATE OR REPLACE TABLE meta.dim_user AS
WITH snapshot_days AS (
    SELECT snapshot_date,
           row_number() OVER (ORDER BY snapshot_date) AS snapshot_seq,
           coalesce(lead(snapshot_date) OVER (ORDER BY snapshot_date), DATE '9999-12-31') AS next_snapshot_date
    FROM (SELECT DISTINCT snapshot_date FROM ref.user_directory_snapshot) AS d
),
snap AS (
    SELECT s.user_name,
           s.display_name,
           s.user_type,
           coalesce(s.team_id, 'unknown') AS team_id,
           s.snapshot_date,
           d.snapshot_seq,
           d.next_snapshot_date,
           lag(coalesce(s.team_id, 'unknown')) OVER (PARTITION BY s.user_name ORDER BY s.snapshot_date) AS prev_team_id,
           lag(d.snapshot_seq) OVER (PARTITION BY s.user_name ORDER BY s.snapshot_date) AS prev_snapshot_seq
    FROM ref.user_directory_snapshot AS s
    JOIN snapshot_days AS d ON d.snapshot_date = s.snapshot_date
),
islands AS (
    SELECT *,
           sum(CASE WHEN prev_team_id IS NULL
                      OR prev_team_id <> team_id
                      OR prev_snapshot_seq <> snapshot_seq - 1
                    THEN 1 ELSE 0 END)
               OVER (PARTITION BY user_name ORDER BY snapshot_date) AS version
    FROM snap
),
versions AS (
    SELECT user_name,
           version,
           team_id,
           max_by(display_name, snapshot_date) AS display_name,
           max_by(user_type, snapshot_date) AS user_type,
           min(snapshot_date) AS first_day,
           -- 9999-12-31 when the user is present in the latest snapshot
           max_by(next_snapshot_date, snapshot_date) AS valid_to
    FROM islands
    GROUP BY user_name, version, team_id
)
SELECT user_name,
       display_name,
       user_type,
       team_id,
       CASE WHEN version = 1 THEN DATE '1970-01-01' ELSE first_day END AS valid_from,
       valid_to,
       valid_to = DATE '9999-12-31' AS is_current
FROM versions;
