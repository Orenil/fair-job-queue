import { Pool } from 'pg';
import { newCore, resetDb, testPool } from './test-utils';

describe('queue core sanity', () => {
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

  it('enqueues and claims a single job end to end', async () => {
    const core = newCore(pool);
    await core.setTenantWeight('t1', 1);
    const id = await core.enqueue({ tenantId: 't1', jobType: 'noop', payload: { x: 1 }, cost: 2 });

    const claimed = await core.claim('w1');
    expect(claimed?.id).toBe(id);
    expect(claimed?.status).toBe('in_progress');
    expect(claimed?.payload).toEqual({ x: 1 });

    const ok = await core.ack(id, 'w1');
    expect(ok).toBe(true);

    const stats = await core.stats();
    expect(stats.completed).toBe(1);
  });

  it('never lets two concurrent workers claim the same job (FOR UPDATE SKIP LOCKED)', async () => {
    const core = newCore(pool);
    await core.setTenantWeight('t1', 1);
    const ids: string[] = [];
    for (let i = 0; i < 20; i++) {
      ids.push(await core.enqueue({ tenantId: 't1', jobType: 'noop', cost: 1 }));
    }

    // Fire 8 concurrent claim() calls per round; with SKIP LOCKED each
    // in-flight transaction skips rows other transactions currently hold,
    // so every claim in a round must return a distinct job.
    const claimedIds = new Set<string>();
    for (let round = 0; round < 20; round += 8) {
      const results = await Promise.all(
        Array.from({ length: 8 }, (_, i) => core.claim(`worker-${round}-${i}`)),
      );
      for (const r of results) {
        if (!r) continue;
        expect(claimedIds.has(r.id)).toBe(false);
        claimedIds.add(r.id);
      }
    }
    expect(claimedIds.size).toBe(20);
    expect([...claimedIds].sort()).toEqual([...ids].sort());
  });

  it('rejects ack from a worker that does not hold the lock', async () => {
    const core = newCore(pool);
    await core.setTenantWeight('t1', 1);
    const id = await core.enqueue({ tenantId: 't1', jobType: 'noop', cost: 1 });
    await core.claim('owner');
    const ok = await core.ack(id, 'intruder');
    expect(ok).toBe(false);
    const job = await core.getJob(id);
    expect(job?.status).toBe('in_progress');
  });
});
