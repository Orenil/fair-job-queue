import { Pool } from 'pg';
import { createPool } from '../src/queue/db';
import { QueueCore } from '../src/queue/queue-core';

export function testPool(): Pool {
  return createPool();
}

export async function resetDb(pool: Pool): Promise<void> {
  await pool.query('TRUNCATE jobs RESTART IDENTITY');
  await pool.query('TRUNCATE tenants');
  await pool.query('UPDATE queue_state SET virtual_time = 0 WHERE id = 1');
}

export function newCore(pool: Pool, visibilityTimeoutSeconds = 30): QueueCore {
  return new QueueCore(pool, { visibilityTimeoutSeconds });
}
