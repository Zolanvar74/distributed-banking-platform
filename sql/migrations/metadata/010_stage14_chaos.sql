-- Development-only, durable one-shot process crash injection for migration acceptance.
CREATE TABLE IF NOT EXISTS stage14_fault_experiments (
  experiment_id uuid PRIMARY KEY,
  job_id uuid NOT NULL REFERENCES migration_jobs(job_id),
  baseline_applied_groups integer NOT NULL DEFAULT 0 CHECK (baseline_applied_groups >= 0),
  point text NOT NULL CHECK (point IN
    ('SNAPSHOT_PERSISTED','DELTA_GROUP_APPLIED','FENCING_GATE_CLOSED','MAP_ACTIVATED')),
  state text NOT NULL DEFAULT 'ARMED' CHECK (state IN ('ARMED','FIRED','CANCELLED')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  fired_at timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS stage14_one_armed_per_job
  ON stage14_fault_experiments(job_id) WHERE state='ARMED';
