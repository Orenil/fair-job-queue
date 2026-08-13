import { ChildProcessWithoutNullStreams, spawn } from 'child_process';
import { join } from 'path';
import { Pool } from 'pg';
import { newCore, resetDb, testPool } from './test-utils';

const VISIBILITY_TIMEOUT_S = 2;

/** Spawns a real OS worker process and resolves once it prints "CLAIMED <id>". */
function spawnWorkerAndWaitForClaim(
  workerId: string,
  workDelayMs: number,
): Promise<{ child: ChildProcessWithoutNullStreams; jobId: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(
      join(__dirname, '../node_modules/.bin/ts-node'),
      [join(__dirname, '../src/worker/run-worker.ts')],
      {
        env: {
          ...process.env,
          WORKER_ID: workerId,
          VISIBILITY_TIMEOUT_S: String(VISIBILITY_TIMEOUT_S),
          WORK_DELAY_MS: String(workDelayMs),
        },
      },
    );
    let resolved = false;
    let out = '';
    child.stdout.on('data', (chunk) => {
      out += chunk.toString();
      const match = out.match(/CLAIMED (\d+)/);
      if (match && !resolved) {
        resolved = true;
        resolve({ child, jobId: match[1] });
      }
    });
    child.stderr.on('data', (chunk) => {
      // eslint-disable-next-line no-console
      console.error(`[worker stderr] ${chunk.toString()}`);
    });
    child.on('exit', (code) => {
      if (!resolved) reject(new Error(`worker exited (code ${code}) before claiming a job`));
    });
    setTimeout(() => {
      if (!resolved) reject(new Error('timed out waiting for worker to claim a job'));
    }, 15000);
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

describe('crash recovery: visibility-timeout reclaim', () => {
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

  it('reclaims a visibility-timed-out job after a worker is SIGKILLed mid-job, and completes it exactly once', async () => {
    const core = newCore(pool, VISIBILITY_TIMEOUT_S);
    await core.setTenantWeight('t1', 1);
    const jobId = await core.enqueue({ tenantId: 't1', jobType: 'crash-test', cost: 1 });

    // Worker 1 claims the job, then simulate a hard crash (SIGKILL) before it
    // can ack -- this is not a graceful shutdown, so no cleanup runs.
    const { child, jobId: claimedId } = await spawnWorkerAndWaitForClaim('worker-1', 8000);
    expect(claimedId).toBe(jobId);

    const afterClaim = await core.getJob(jobId);
    expect(afterClaim?.status).toBe('in_progress');
    expect(afterClaim?.lockedBy).toBe('worker-1');
    expect(afterClaim?.attempts).toBe(1);

    child.kill('SIGKILL');
    await sleep(300);
    // Confirm the process is really gone (no graceful ack could have run).
    expect(child.killed).toBe(true);

    // Immediately after the crash the job is still (correctly) invisible --
    // no other worker should be able to claim it yet.
    const tooSoon = await core.claim('worker-2');
    expect(tooSoon).toBeNull();

    // Wait past the visibility timeout.
    await sleep(VISIBILITY_TIMEOUT_S * 1000 + 500);

    // Worker 2 should now be able to reclaim the same job via SKIP LOCKED scan.
    const reclaimed = await core.claim('worker-2');
    expect(reclaimed).not.toBeNull();
    expect(reclaimed?.id).toBe(jobId);
    expect(reclaimed?.attempts).toBe(2); // claimed twice total: crashed worker + reclaimer
    expect(reclaimed?.lockedBy).toBe('worker-2');

    // The dead worker's identity no longer owns the lock, so a stale ack
    // attempt using its id must be rejected -- this is what keeps completion
    // exactly-once even if a "zombie" process somehow resumed.
    const staleAck = await core.ack(jobId, 'worker-1');
    expect(staleAck).toBe(false);

    const properAck = await core.ack(jobId, 'worker-2');
    expect(properAck).toBe(true);

    const final = await core.getJob(jobId);
    expect(final?.status).toBe('completed');
  }, 30000);
});
