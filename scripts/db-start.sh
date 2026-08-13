#!/usr/bin/env bash
# Starts the project-local Postgres cluster (unix socket only) and creates the
# fair_job_queue database if it doesn't exist yet.
set -euo pipefail
cd "$(dirname "$0")/.."

PGDATA_DIR="$(pwd)/.pgdata/data"
SOCKET_DIR="$(pwd)/.pgdata/socket"
LOG_FILE="$(pwd)/.pgdata/postgres.log"

if [ ! -d "$PGDATA_DIR" ]; then
  bash scripts/db-init.sh
fi

if pg_ctl -D "$PGDATA_DIR" status >/dev/null 2>&1; then
  echo "Postgres already running."
else
  pg_ctl -D "$PGDATA_DIR" -l "$LOG_FILE" -o "-h ''" start
fi

# Wait for the socket to accept connections.
for i in $(seq 1 30); do
  if psql -h "$SOCKET_DIR" -U postgres -d postgres -c 'select 1' >/dev/null 2>&1; then
    break
  fi
  sleep 0.3
done

psql -h "$SOCKET_DIR" -U postgres -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'fair_job_queue'" | grep -q 1 \
  || psql -h "$SOCKET_DIR" -U postgres -d postgres -c "CREATE DATABASE fair_job_queue"

echo "Postgres ready. socket_dir=$SOCKET_DIR db=fair_job_queue"
echo "DATABASE_URL=postgres://postgres@localhost/fair_job_queue?host=$SOCKET_DIR"
