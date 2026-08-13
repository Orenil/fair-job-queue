#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PGDATA_DIR="$(pwd)/.pgdata/data"
pg_ctl -D "$PGDATA_DIR" stop -m fast
