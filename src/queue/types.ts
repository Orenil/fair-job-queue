export interface EnqueueOptions {
  tenantId: string;
  jobType: string;
  payload?: Record<string, unknown>;
  /** Relative cost of this job, used as the denominator weight input. Default 1. */
  cost?: number;
  maxAttempts?: number;
}

export interface FailureRecord {
  attempt: number;
  error: string;
  failedAt: string;
}

export type JobStatus = 'pending' | 'in_progress' | 'completed' | 'dead_letter';

export interface Job {
  id: string;
  tenantId: string;
  jobType: string;
  payload: Record<string, unknown>;
  cost: number;
  virtualFinish: number;
  status: JobStatus;
  attempts: number;
  maxAttempts: number;
  visibleAt: Date;
  lockedBy: string | null;
  failureHistory: FailureRecord[];
  createdAt: Date;
  updatedAt: Date;
}

export interface ClaimedJob extends Job {
  status: 'in_progress';
  lockedBy: string;
}
