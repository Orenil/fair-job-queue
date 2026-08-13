import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { createPool } from '../queue/db';
import { QueueCore } from '../queue/queue-core';
import { JobHandler, WorkerRunner } from './worker-runner';

/**
 * Nest-lifecycle wrapper that lets an application register a job handler
 * and start a WorkerRunner poll loop under Nest DI, without the poll loop
 * itself knowing anything about Nest.
 */
@Injectable()
export class WorkerService implements OnModuleDestroy {
  private runner: WorkerRunner | null = null;
  private runPromise: Promise<void> | null = null;
  private readonly pool = createPool();
  private readonly core = new QueueCore(this.pool, {
    visibilityTimeoutSeconds: Number(process.env.VISIBILITY_TIMEOUT_S ?? '30'),
  });

  start(handler: JobHandler, workerId: string, pollIntervalMs = 250): void {
    if (this.runner) throw new Error('Worker already started');
    this.runner = new WorkerRunner(this.core, handler, { workerId, pollIntervalMs });
    this.runPromise = this.runner.run();
  }

  async stop(): Promise<void> {
    this.runner?.stop();
    if (this.runPromise) await this.runPromise;
    this.runner = null;
  }

  async onModuleDestroy(): Promise<void> {
    await this.stop();
    await this.pool.end();
  }
}
