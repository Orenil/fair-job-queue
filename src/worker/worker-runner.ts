import { QueueCore } from '../queue/queue-core';
import { ClaimedJob } from '../queue/types';

export type JobHandler = (job: ClaimedJob) => Promise<void>;

export interface WorkerRunnerOptions {
  workerId: string;
  pollIntervalMs?: number;
  /** Stop polling once the queue reports no claimable job this many times in a row. */
  stopWhenIdle?: boolean;
}

/**
 * Framework-agnostic poll loop: claim -> handle -> ack/fail. Runs
 * standalone (see run-worker.ts, used by the crash-recovery test to spawn a
 * real killable OS process) or wrapped by Nest's WorkerService for DI.
 */
export class WorkerRunner {
  private stopped = false;

  constructor(
    private readonly core: QueueCore,
    private readonly handler: JobHandler,
    private readonly opts: WorkerRunnerOptions,
  ) {}

  stop(): void {
    this.stopped = true;
  }

  async run(): Promise<void> {
    const pollIntervalMs = this.opts.pollIntervalMs ?? 100;
    while (!this.stopped) {
      const job = await this.core.claim(this.opts.workerId);
      if (!job) {
        if (this.opts.stopWhenIdle) return;
        await sleep(pollIntervalMs);
        continue;
      }
      try {
        await this.handler(job);
        await this.core.ack(job.id, this.opts.workerId);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        await this.core.fail(job.id, this.opts.workerId, message);
      }
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
