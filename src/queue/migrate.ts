import { readFileSync } from 'fs';
import { join } from 'path';
import { createPool } from './db';

async function main(): Promise<void> {
  const pool = createPool();
  const sql = readFileSync(join(__dirname, 'schema.sql'), 'utf8');
  await pool.query(sql);
  await pool.end();
  // eslint-disable-next-line no-console
  console.log('Migration applied.');
}

if (require.main === module) {
  main().catch((err) => {
    // eslint-disable-next-line no-console
    console.error(err);
    process.exit(1);
  });
}

export { main as migrate };
