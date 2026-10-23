-- Full rebuild (a few hundred rows). The 'unknown' member gives unmapped usage somewhere to land
-- instead of silently disappearing from team totals.
CREATE OR REPLACE TABLE meta.dim_team AS
SELECT team_id, team_name, department, cost_center
FROM ref.team
UNION ALL
SELECT 'unknown', 'Unmapped users', NULL, NULL;
