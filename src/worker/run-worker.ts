/**
 * Standalone single-shot worker process, used by test/crash-recovery.spec.ts
 * to spawn a real OS process that can be SIGKILLed mid-job (a poll-loop
 * running inside the test process itself couldn't simulate a hard crash the
 * same way -- killing it would kill the test runner too).
 *
 * Env vars:
 *   DATABASE_URL          - as produced by scripts/db-start.sh
 *   WORKER_ID             - identifies this worker for locked_by
 *   VISIBILITY_TIMEOUT_S  - visibility timeout in seconds
 *   WORK_DELAY_MS         - how long to "work" before acking (default 5000)
 *
 * Prints "CLAIMED <jobId>" to stdout as soon as a job is claimed, so the
 * test knows it's safe to SIGKILL. Prints "DONE <jobId>" after ack.
 */
import { createPool } from '../queue/db';
import { QueueCore } from '../queue/queue-core';

async function main(): Promise<void> {
  const workerId = process.env.WORKER_ID ?? `worker-${process.pid}`;
  const visibilityTimeoutSeconds = Number(process.env.VISIBILITY_TIMEOUT_S ?? '2');
  const workDelayMs = Number(process.env.WORK_DELAY_MS ?? '5000');

  const pool = createPool();
  const core = new QueueCore(pool, { visibilityTimeoutSeconds });

  const job = await core.claim(workerId);
  if (!job) {
    // eslint-disable-next-line no-console
    console.log('NO_JOB');
    await pool.end();
    return;
  }
  // eslint-disable-next-line no-console
  console.log(`CLAIMED ${job.id}`);
  await new Promise((resolve) => setTimeout(resolve, workDelayMs));
  await core.ack(job.id, workerId);
  // eslint-disable-next-line no-console
  console.log(`DONE ${job.id}`);
  await pool.end();
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});
