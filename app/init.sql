-- Drop if re-running from scratch
DROP TABLE IF EXISTS state_machine;

-- Main table
CREATE TABLE state_machine (
    id          BIGINT PRIMARY KEY,
    state       TEXT NOT NULL,
    payload     JSONB,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Optional index for more realistic workload
CREATE INDEX IF NOT EXISTS idx_state_machine_state ON state_machine(state);

-- Seed data: 1,000,000 rows
INSERT INTO state_machine (id, state, payload, updated_at)
SELECT
    g AS id,
    'INIT' AS state,
    jsonb_build_object('seq', g, 'flag', g % 5),
    now() - (g % 5000) * INTERVAL '1 second'
FROM generate_series(1, :rows) AS g;

-- Make sure planner stats are good
VACUUM ANALYZE state_machine;

