import { Pool } from 'pg';
import { newCore, resetDb, testPool } from './test-utils';

describe('poison job dead-letter transition', () => {
  let pool: Pool;

  beforeAll(() => {
    pool = testPool();
  });

  afterAll(async () => {
    await pool.end();
  });

  beforeEach(async () => {
    await resetDb(pool);
  });

  it('moves a repeatedly-failing job to dead_letter after the configured retry threshold, with full failure history', async () => {
    const core = newCore(pool);
    await core.setTenantWeight('t1', 1);
    const maxAttempts = 3;
    const jobId = await core.enqueue({ tenantId: 't1', jobType: 'poison', cost: 1, maxAttempts });

    const outcomes: string[] = [];
    for (let i = 0; i < maxAttempts; i++) {
      const job = await core.claim('worker-x');
      expect(job).not.toBeNull();
      expect(job?.id).toBe(jobId);
      const outcome = await core.fail(jobId, 'worker-x', `boom attempt ${i + 1}`);
      outcomes.push(outcome);
    }

    expect(outcomes).toEqual(['retrying', 'retrying', 'dead_letter']);

    const final = await core.getJob(jobId);
    expect(final?.status).toBe('dead_letter');
    expect(final?.attempts).toBe(maxAttempts);
    expect(final?.failureHistory).toHaveLength(maxAttempts);
    final?.failureHistory.forEach((record, i) => {
      expect(record.attempt).toBe(i + 1);
      expect(record.error).toBe(`boom attempt ${i + 1}`);
      expect(typeof record.failedAt).toBe('string');
      expect(new Date(record.failedAt).toString()).not.toBe('Invalid Date');
    });

    // Dead-lettered jobs must never be reclaimed by the normal dequeue path.
    const shouldBeNull = await core.claim('worker-y');
    expect(shouldBeNull).toBeNull();
  });

  it('keeps a job retryable (does not dead-letter early) while under the threshold', async () => {
    const core = newCore(pool);
    await core.setTenantWeight('t1', 1);
    const jobId = await core.enqueue({ tenantId: 't1', jobType: 'flaky', cost: 1, maxAttempts: 5 });

    for (let i = 0; i < 2; i++) {
      const job = await core.claim('worker-x');
      const outcome = await core.fail(jobId!, 'worker-x', `transient ${i}`);
      expect(outcome).toBe('retrying');
      void job;
    }

    const job = await core.claim('worker-x');
    expect(job?.id).toBe(jobId);
    const ok = await core.ack(jobId, 'worker-x');
    expect(ok).toBe(true);

    const final = await core.getJob(jobId);
    expect(final?.status).toBe('completed');
    expect(final?.failureHistory).toHaveLength(2);
  });
});
