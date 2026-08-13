import { Pool } from 'pg';
import { newCore, resetDb, testPool } from './test-utils';

/** Deterministic PRNG (mulberry32) so the benchmark is reproducible across runs. */
function mulberry32(seed: number): () => number {
  let a = seed;
  return function () {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function shuffle<T>(arr: T[], rng: () => number): T[] {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

interface Arrival {
  tenant: string;
  cost: number;
  arrivalIndex: number;
}

/**
 * Independent reference implementation of the same SCFQ virtual-finish
 * formula used by QueueCore, computed purely in JS from the enqueue
 * sequence. Because this test enqueues everything before any claim occurs,
 * the global virtual_time term never advances during enqueue (it only
 * advances inside claim()), so each job's virtual_finish depends only on
 * the running cost total of its own tenant -- exactly mirroring what
 * QueueCore will compute against Postgres.
 */
function computeTargetOrder(arrivals: Arrival[], weights: Record<string, number>): Arrival[] {
  const lastFinish: Record<string, number> = {};
  const withFinish = arrivals.map((a) => {
    const start = lastFinish[a.tenant] ?? 0;
    const finish = start + a.cost / weights[a.tenant];
    lastFinish[a.tenant] = finish;
    return { ...a, finish };
  });
  return [...withFinish].sort((x, y) => x.finish - y.finish || x.arrivalIndex - y.arrivalIndex);
}

function positionsOf(seq: string[], tenant: string): number[] {
  const out: number[] = [];
  seq.forEach((t, i) => {
    if (t === tenant) out.push(i + 1);
  });
  return out;
}

function average(nums: number[]): number {
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

describe('fairness benchmark: skewed multi-tenant load', () => {
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

  it('serves a 1x-volume tenant proportionally to its equal weight despite a 100x-volume co-tenant', async () => {
    const core = newCore(pool);
    await core.setTenantWeight('big', 1);
    await core.setTenantWeight('small', 1);

    const rng = mulberry32(42);
    const BIG_COUNT = 400;
    const SMALL_COUNT = 4; // 100x volume skew

    let arrivalIndex = 0;
    const bigArrivals: Arrival[] = Array.from({ length: BIG_COUNT }, () => ({
      tenant: 'big',
      cost: 1 + Math.floor(rng() * 5), // varying job cost, 1..5
      arrivalIndex: arrivalIndex++,
    }));
    // small tenant's own internal arrival order must be preserved when interleaving
    const smallArrivalsRaw: Omit<Arrival, 'arrivalIndex'>[] = Array.from({ length: SMALL_COUNT }, () => ({
      tenant: 'small',
      cost: 1 + Math.floor(rng() * 5),
    }));

    // Interleave arrival order randomly (skewed real traffic doesn't arrive
    // in two neat blocks), while preserving each tenant's own submission order.
    const interleavedTenants = shuffle(
      [...Array(BIG_COUNT).fill('big'), ...Array(SMALL_COUNT).fill('small')],
      rng,
    );
    let bigCursor = 0;
    let smallCursor = 0;
    const arrivals: Arrival[] = interleavedTenants.map((tenant) => {
      if (tenant === 'big') {
        const a = bigArrivals[bigCursor++];
        return { ...a, arrivalIndex: 0 }; // reassigned below
      }
      const a = smallArrivalsRaw[smallCursor++];
      return { tenant: a.tenant, cost: a.cost, arrivalIndex: 0 };
    });
    arrivals.forEach((a, i) => (a.arrivalIndex = i));

    const target = computeTargetOrder(arrivals, { big: 1, small: 1 });
    const targetTenantSeq = target.map((a) => a.tenant);

    // Real enqueue against Postgres, same sequence & costs.
    for (const a of arrivals) {
      await core.enqueue({ tenantId: a.tenant, jobType: 'bench', cost: a.cost });
    }

    const actualOrder: { tenant: string; cost: number; claimedAtMs: number }[] = [];
    const t0 = Date.now();
    for (;;) {
      const job = await core.claim('bench-worker');
      if (!job) break;
      actualOrder.push({ tenant: job.tenantId, cost: job.cost, claimedAtMs: Date.now() - t0 });
      await core.ack(job.id, 'bench-worker');
    }
    const actualTenantSeq = actualOrder.map((j) => j.tenant);

    expect(actualOrder.length).toBe(arrivals.length);

    // 1) The queue's real dequeue order must exactly match the
    //    independently-computed WFQ target order.
    expect(actualTenantSeq).toEqual(targetTenantSeq);

    // 2) Numeric fairness comparison against naive baselines.
    const avgPositionWFQ = average(positionsOf(actualTenantSeq, 'small'));
    const avgLatencyWFQms = average(
      actualOrder.filter((j) => j.tenant === 'small').map((j) => j.claimedAtMs),
    );

    const fifoTenantSeq = arrivals.map((a) => a.tenant); // naive FIFO = raw arrival order
    const avgPositionFIFO = average(positionsOf(fifoTenantSeq, 'small'));

    // naive strict tenant-priority queue: whichever tenant is "bigger" (by
    // volume) is always served to exhaustion before the other -- a common
    // naive design (ORDER BY tenant_priority) that starves small tenants.
    const priorityTenantSeq = [...arrivals]
      .sort((x, y) => (x.tenant === y.tenant ? x.arrivalIndex - y.arrivalIndex : x.tenant === 'big' ? -1 : 1))
      .map((a) => a.tenant);
    const avgPositionPriority = average(positionsOf(priorityTenantSeq, 'small'));

    // eslint-disable-next-line no-console
    console.log(
      `\n[fairness benchmark] total jobs=${arrivals.length} (big=${BIG_COUNT}, small=${SMALL_COUNT}, ${(BIG_COUNT / SMALL_COUNT).toFixed(0)}x volume skew), equal tenant weight 1:1\n` +
        `  small-tenant avg dequeue position -- WFQ (ours): ${avgPositionWFQ.toFixed(1)} | naive FIFO: ${avgPositionFIFO.toFixed(1)} | naive strict-priority-by-volume: ${avgPositionPriority.toFixed(1)}  (out of ${arrivals.length} jobs)\n` +
        `  small-tenant avg claim latency (WFQ, wall clock): ${avgLatencyWFQms.toFixed(2)}ms\n` +
        `  WFQ improvement over naive FIFO: ${(avgPositionFIFO / avgPositionWFQ).toFixed(1)}x earlier service\n` +
        `  WFQ improvement over naive priority-by-volume: ${(avgPositionPriority / avgPositionWFQ).toFixed(1)}x earlier service\n`,
    );

    // WFQ must serve the small tenant far earlier than either naive baseline.
    expect(avgPositionWFQ).toBeLessThan(avgPositionFIFO * 0.3);
    expect(avgPositionWFQ).toBeLessThan(avgPositionPriority * 0.1);
  }, 60000);

  it('gives a higher-weight tenant a proportionally larger share of early service', async () => {
    const core = newCore(pool);
    await core.setTenantWeight('gold', 4); // 4x weight
    await core.setTenantWeight('standard', 1);

    const rng = mulberry32(7);
    const arrivals: Arrival[] = [];
    let idx = 0;
    for (let i = 0; i < 100; i++) arrivals.push({ tenant: 'gold', cost: 1 + Math.floor(rng() * 3), arrivalIndex: idx++ });
    for (let i = 0; i < 100; i++) arrivals.push({ tenant: 'standard', cost: 1 + Math.floor(rng() * 3), arrivalIndex: idx++ });
    const interleaved = shuffle(arrivals, rng).map((a, i) => ({ ...a, arrivalIndex: i }));

    for (const a of interleaved) {
      await core.enqueue({ tenantId: a.tenant, jobType: 'bench', cost: a.cost });
    }

    // Drain the first 40 jobs only (a snapshot of "early service") and
    // measure how much cost-weighted work each tenant received.
    const firstN = 40;
    const served: { tenant: string; cost: number }[] = [];
    for (let i = 0; i < firstN; i++) {
      const job = await core.claim('bench-worker-2');
      if (!job) break;
      served.push({ tenant: job.tenantId, cost: job.cost });
      await core.ack(job.id, 'bench-worker-2');
    }

    const goldCost = served.filter((s) => s.tenant === 'gold').reduce((a, b) => a + b.cost, 0);
    const standardCost = served.filter((s) => s.tenant === 'standard').reduce((a, b) => a + b.cost, 0);
    const ratio = goldCost / standardCost;

    // eslint-disable-next-line no-console
    console.log(
      `\n[weighted fairness] first ${firstN} dequeues: gold(weight 4) cost served=${goldCost}, standard(weight 1) cost served=${standardCost}, ratio=${ratio.toFixed(2)} (target ~4.0)\n`,
    );

    // Should approximate the 4:1 weight ratio, not the (roughly even) volume ratio.
    expect(ratio).toBeGreaterThan(2.5);
  }, 60000);
});
