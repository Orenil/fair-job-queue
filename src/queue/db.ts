import { Pool, PoolConfig } from 'pg';

/**
 * Resolves Postgres connection options from DATABASE_URL, expecting the
 * `?host=<unix-socket-dir>` query param produced by scripts/db-start.sh
 * (unix-socket-only cluster, no TCP listener).
 */
export function poolConfigFromEnv(): PoolConfig {
  const url = process.env.DATABASE_URL;
  if (!url) {
    throw new Error('DATABASE_URL is not set. Run `npm run db:start` and export it, e.g.\n' +
      '  export DATABASE_URL=$(npm run -s db:start | tail -1 | cut -d= -f2-)');
  }
  const parsed = new URL(url);
  const host = parsed.searchParams.get('host') ?? parsed.hostname;
  return {
    host,
    user: parsed.username || 'postgres',
    database: parsed.pathname.replace(/^\//, '') || 'fair_job_queue',
    max: 10,
  };
}

export function createPool(config?: PoolConfig): Pool {
  return new Pool(config ?? poolConfigFromEnv());
}
