import { Pool, PoolClient } from 'pg';
import { ClaimedJob, EnqueueOptions, FailureRecord, Job, JobStatus } from './types';

export interface QueueCoreOptions {
  /** Seconds a claimed job stays invisible before it's reclaimable. Default 30. */
  visibilityTimeoutSeconds?: number;
}

interface JobRow {
  id: string;
  tenant_id: string;
  job_type: string;
  payload: Record<string, unknown>;
  cost: string;
  virtual_finish: string;
  status: JobStatus;
  attempts: number;
  max_attempts: number;
  visible_at: Date;
  locked_by: string | null;
  failure_history: FailureRecord[];
  created_at: Date;
  updated_at: Date;
}

function rowToJob(r: JobRow): Job {
  return {
    id: r.id,
    tenantId: r.tenant_id,
    jobType: r.job_type,
    payload: r.payload,
    cost: Number(r.cost),
    virtualFinish: Number(r.virtual_finish),
    status: r.status,
    attempts: r.attempts,
    maxAttempts: r.max_attempts,
    visibleAt: r.visible_at,
    lockedBy: r.locked_by,
    failureHistory: r.failure_history,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}

/**
 * Framework-agnostic queue core: Postgres-backed enqueue/claim/ack/fail
 * operations implementing weighted fair queuing (Self-Clocked Fair Queuing
 * virtual-time assignment) with visibility-timeout reclaim and dead-letter
 * after a bounded retry count. NestJS wraps this in QueueService; nothing
 * here depends on Nest.
 */
export class QueueCore {
  constructor(
    private readonly pool: Pool,
    private readonly opts: QueueCoreOptions = {},
  ) {}

  private get visibilityTimeoutSeconds(): number {
    return this.opts.visibilityTimeoutSeconds ?? 30;
  }

  /**
   * Sets (or updates) a tenant's fair-share weight. Higher weight => a
   * tenant's jobs accrue virtual finish time more slowly => served more
   * often relative to others. Defaults to weight 1 if never called.
   */
  async setTenantWeight(tenantId: string, weight: number): Promise<void> {
    await this.pool.query(
      `INSERT INTO tenants (tenant_id, weight) VALUES ($1, $2)
       ON CONFLICT (tenant_id) DO UPDATE SET weight = EXCLUDED.weight`,
      [tenantId, weight],
    );
  }

  /**
   * Enqueues a job. Assigns its virtual_finish under row locks on the
   * tenant's cursor and the global queue_state so concurrent enqueues from
   * different tenants serialize correctly (SCFQ requires a total order on
   * virtual-time assignment). This is the one intentionally serialized path
   * in the system -- see README "rejected tradeoffs" for why.
   */
  async enqueue(opts: EnqueueOptions): Promise<string> {
    const cost = opts.cost ?? 1;
    const maxAttempts = opts.maxAttempts ?? 5;
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO tenants (tenant_id) VALUES ($1) ON CONFLICT DO NOTHING`,
        [opts.tenantId],
      );
      const tenantRes = await client.query<{ weight: string; last_finish: string }>(
        `SELECT weight, last_finish FROM tenants WHERE tenant_id = $1 FOR UPDATE`,
        [opts.tenantId],
      );
      const stateRes = await client.query<{ virtual_time: string }>(
        `SELECT virtual_time FROM queue_state WHERE id = 1 FOR UPDATE`,
      );
      const weight = Number(tenantRes.rows[0].weight);
      const lastFinish = Number(tenantRes.rows[0].last_finish);
      const virtualTime = Number(stateRes.rows[0].virtual_time);

      const virtualStart = Math.max(lastFinish, virtualTime);
      const virtualFinish = virtualStart + cost / weight;

      await client.query(`UPDATE tenants SET last_finish = $1 WHERE tenant_id = $2`, [
        virtualFinish,
        opts.tenantId,
      ]);

      const insertRes = await client.query<{ id: string }>(
        `INSERT INTO jobs (tenant_id, job_type, payload, cost, virtual_finish, max_attempts)
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
        [opts.tenantId, opts.jobType, opts.payload ?? {}, cost, virtualFinish, maxAttempts],
      );
      await client.query('COMMIT');
      return insertRes.rows[0].id;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  /**
   * Claims the single globally-earliest-virtual-finish job that is either
   * pending, or in_progress with an expired visibility timeout (crashed
   * worker recovery). Uses FOR UPDATE SKIP LOCKED so concurrent workers
   * never contend on the same row and never double-claim.
   */
  async claim(workerId: string): Promise<ClaimedJob | null> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const res = await client.query<JobRow>(
        `SELECT * FROM jobs
         WHERE status = 'pending' OR (status = 'in_progress' AND visible_at <= now())
         ORDER BY virtual_finish ASC, id ASC
         FOR UPDATE SKIP LOCKED
         LIMIT 1`,
      );
      if (res.rows.length === 0) {
        await client.query('COMMIT');
        return null;
      }
      const row = res.rows[0];
      const updated = await client.query<JobRow>(
        `UPDATE jobs
         SET status = 'in_progress',
             attempts = attempts + 1,
             locked_by = $2,
             visible_at = now() + ($3 || ' seconds')::interval,
             updated_at = now()
         WHERE id = $1
         RETURNING *`,
        [row.id, workerId, this.visibilityTimeoutSeconds],
      );
      // Self-clock the global virtual time forward to the finish tag of the
      // job just put into service, approximating "finish tag of the packet
      // currently in service" from single-server SCFQ.
      await client.query(
        `UPDATE queue_state SET virtual_time = GREATEST(virtual_time, $1) WHERE id = 1`,
        [row.virtual_finish],
      );
      await client.query('COMMIT');
      return rowToJob(updated.rows[0]) as ClaimedJob;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  /** Marks a claimed job completed. Idempotent: no-op if already completed. */
  async ack(jobId: string, workerId: string): Promise<boolean> {
    const res = await this.pool.query(
      `UPDATE jobs SET status = 'completed', updated_at = now()
       WHERE id = $1 AND locked_by = $2 AND status = 'in_progress'`,
      [jobId, workerId],
    );
    return (res.rowCount ?? 0) > 0;
  }

  /**
   * Records a failed attempt. If attempts have reached max_attempts, the
   * job moves to dead_letter with its full failure history intact;
   * otherwise it goes back to pending (immediately reclaimable, still
   * ordered by its original virtual_finish so retries don't jump the
   * fairness queue).
   */
  async fail(jobId: string, workerId: string, error: string): Promise<'retrying' | 'dead_letter'> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const res = await client.query<JobRow>(
        `SELECT * FROM jobs WHERE id = $1 AND locked_by = $2 FOR UPDATE`,
        [jobId, workerId],
      );
      if (res.rows.length === 0) {
        await client.query('ROLLBACK');
        throw new Error(`Job ${jobId} not found or not locked by ${workerId}`);
      }
      const row = res.rows[0];
      const failure: FailureRecord = { attempt: row.attempts, error, failedAt: new Date().toISOString() };
      const history = [...row.failure_history, failure];
      const isDead = row.attempts >= row.max_attempts;
      await client.query(
        `UPDATE jobs
         SET status = $2, failure_history = $3::jsonb, locked_by = NULL, updated_at = now(),
             visible_at = now()
         WHERE id = $1`,
        [jobId, isDead ? 'dead_letter' : 'pending', JSON.stringify(history)],
      );
      await client.query('COMMIT');
      return isDead ? 'dead_letter' : 'retrying';
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }

  async getJob(jobId: string): Promise<Job | null> {
    const res = await this.pool.query<JobRow>(`SELECT * FROM jobs WHERE id = $1`, [jobId]);
    return res.rows.length ? rowToJob(res.rows[0]) : null;
  }

  async stats(): Promise<Record<JobStatus, number>> {
    const res = await this.pool.query<{ status: JobStatus; count: string }>(
      `SELECT status, count(*) FROM jobs GROUP BY status`,
    );
    const out: Record<JobStatus, number> = { pending: 0, in_progress: 0, completed: 0, dead_letter: 0 };
    for (const r of res.rows) out[r.status] = Number(r.count);
    return out;
  }

  /** Used only by tests to reset state between runs / seed deterministic scenarios. */
  async _withTransaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn(client);
      await client.query('COMMIT');
      return result;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }
}
