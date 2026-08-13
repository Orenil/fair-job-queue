import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Pool } from 'pg';
import { createPool } from './db';
import { QueueCore } from './queue-core';
import { ClaimedJob, EnqueueOptions, Job, JobStatus } from './types';

/**
 * Thin NestJS-injectable wrapper around the framework-agnostic QueueCore.
 * Keeps the queue's actual logic (WFQ virtual time, SKIP LOCKED claim,
 * visibility timeout, dead-letter transition) independent of Nest so it can
 * be unit-tested or reused outside a Nest app; this class just gives it a
 * DI lifecycle (pool creation/teardown).
 */
@Injectable()
export class QueueService implements OnModuleDestroy {
  private readonly pool: Pool;
  private readonly core: QueueCore;

  constructor() {
    this.pool = createPool();
    this.core = new QueueCore(this.pool, {
      visibilityTimeoutSeconds: Number(process.env.VISIBILITY_TIMEOUT_S ?? '30'),
    });
  }

  setTenantWeight(tenantId: string, weight: number): Promise<void> {
    return this.core.setTenantWeight(tenantId, weight);
  }

  enqueue(opts: EnqueueOptions): Promise<string> {
    return this.core.enqueue(opts);
  }

  claim(workerId: string): Promise<ClaimedJob | null> {
    return this.core.claim(workerId);
  }

  ack(jobId: string, workerId: string): Promise<boolean> {
    return this.core.ack(jobId, workerId);
  }

  fail(jobId: string, workerId: string, error: string): Promise<'retrying' | 'dead_letter'> {
    return this.core.fail(jobId, workerId, error);
  }

  getJob(jobId: string): Promise<Job | null> {
    return this.core.getJob(jobId);
  }

  stats(): Promise<Record<JobStatus, number>> {
    return this.core.stats();
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end();
  }
}
