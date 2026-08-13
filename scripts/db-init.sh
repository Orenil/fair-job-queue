#!/usr/bin/env bash
# Initializes a standalone local Postgres cluster dedicated to this project.
# Uses a unix socket only (no TCP) inside .pgdata/ so it can't collide with
# any other Postgres instance (this machine or a concurrent session) on port 5432.
set -euo pipefail
cd "$(dirname "$0")/.."

PGDATA_DIR="$(pwd)/.pgdata/data"
SOCKET_DIR="$(pwd)/.pgdata/socket"

if [ -d "$PGDATA_DIR" ]; then
  echo "Cluster already initialized at $PGDATA_DIR"
  exit 0
fi

mkdir -p "$SOCKET_DIR"
initdb -D "$PGDATA_DIR" -U postgres --auth=trust --no-locale --encoding=UTF8 >/dev/null

# Disable TCP listening entirely; unix socket only, in our own socket dir.
{
  echo "listen_addresses = ''"
  echo "unix_socket_directories = '$SOCKET_DIR'"
} >> "$PGDATA_DIR/postgresql.conf"

echo "Initialized Postgres cluster at $PGDATA_DIR (unix socket only, dir: $SOCKET_DIR)"
