#!/usr/bin/env sh
set -eu

DB_PATH="${ARCHIVE_INDEX_DB:-/data/archive_index.db}"
PORT_VALUE="${PORT:-8787}"
SHARED_DIR_VALUE="${ARCHIVE_SHARED_DIR:-/data/share}"
NODE_URL_VALUE="${ARCHIVE_NODE_URL:-}"
ADMIN_TOKEN_VALUE="${ARCHIVE_ADMIN_TOKEN:-}"
READ_TOKEN_VALUE="${ARCHIVE_READ_TOKEN:-}"
MESH_TOKEN_VALUE="${ARCHIVE_MESH_TOKEN:-}"
PRODUCTION_VALUE="${ARCHIVE_PRODUCTION:-true}"

mkdir -p "$(dirname "$DB_PATH")"
mkdir -p "$SHARED_DIR_VALUE"
if [ "$PRODUCTION_VALUE" = "true" ] || [ "$PRODUCTION_VALUE" = "1" ]; then
  exec python3 archive_index.py --db "$DB_PATH" serve \
    --host 0.0.0.0 --port "$PORT_VALUE" \
    --shared-dir "$SHARED_DIR_VALUE" \
    --node-url "$NODE_URL_VALUE" \
    --admin-token "$ADMIN_TOKEN_VALUE" \
    --read-token "$READ_TOKEN_VALUE" \
    --mesh-token "$MESH_TOKEN_VALUE" \
    --production
fi

exec python3 archive_index.py --db "$DB_PATH" serve \
  --host 0.0.0.0 --port "$PORT_VALUE" \
  --shared-dir "$SHARED_DIR_VALUE" \
  --node-url "$NODE_URL_VALUE" \
  --admin-token "$ADMIN_TOKEN_VALUE" \
  --read-token "$READ_TOKEN_VALUE" \
  --mesh-token "$MESH_TOKEN_VALUE"
