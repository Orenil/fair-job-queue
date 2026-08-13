-- Fair job queue schema.
--
-- Scheduling model: Self-Clocked Fair Queuing (SCFQ), Golestani 1994,
-- adapted from a single-server packet scheduler to a multi-worker job
-- queue. See README "Architecture" for the full derivation. In short:
--
--   virtual_start(job)  = max(tenant_cursor.last_finish, queue_state.virtual_time)
--   virtual_finish(job) = virtual_start(job) + cost / tenant.weight
--
-- Jobs are always dequeued in ascending virtual_finish order. The single
-- global virtual_time is advanced ("self-clocked") to the finish tag of
-- whatever job a worker just claimed, approximating "the finish tag of the
-- packet currently in service" from the original single-server algorithm.

CREATE TABLE IF NOT EXISTS tenants (
  tenant_id   TEXT PRIMARY KEY,
  weight      NUMERIC NOT NULL DEFAULT 1 CHECK (weight > 0),
  last_finish NUMERIC NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS queue_state (
  id           INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  virtual_time NUMERIC NOT NULL DEFAULT 0
);
INSERT INTO queue_state (id, virtual_time) VALUES (1, 0) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS jobs (
  id                 BIGSERIAL PRIMARY KEY,
  tenant_id          TEXT NOT NULL,
  job_type           TEXT NOT NULL,
  payload            JSONB NOT NULL DEFAULT '{}',
  cost               NUMERIC NOT NULL DEFAULT 1 CHECK (cost > 0),
  virtual_finish     NUMERIC NOT NULL,
  status             TEXT NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'in_progress', 'completed', 'dead_letter')),
  attempts           INT NOT NULL DEFAULT 0,
  max_attempts       INT NOT NULL DEFAULT 5,
  visible_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  locked_by          TEXT,
  failure_history    JSONB NOT NULL DEFAULT '[]',
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dequeue candidates (pending, or in_progress but past their visibility
-- timeout) ordered by virtual_finish. Partial index keeps the hot path fast
-- as completed/dead_letter jobs accumulate.
CREATE INDEX IF NOT EXISTS jobs_dequeue_idx ON jobs (virtual_finish)
  WHERE status IN ('pending', 'in_progress');

CREATE INDEX IF NOT EXISTS jobs_tenant_idx ON jobs (tenant_id);
