# peerBay (Production Ready Baseline)

peerBay is now hardened for internet deployment with strict production checks.

## What is hardened

- Token auth separation:
  - `ARCHIVE_ADMIN_TOKEN` for admin actions
  - `ARCHIVE_READ_TOKEN` for listing/downloads
  - `ARCHIVE_MESH_TOKEN` for peer mesh routes
  - per-user read/mesh tokens via self-signup/login
- Strict production startup validation (`--production` / `ARCHIVE_PRODUCTION=true`):
  - requires all tokens
  - requires `ARCHIVE_NODE_URL`
  - requires HTTPS node URL
  - enforces minimum token length
- Security headers:
  - `Content-Security-Policy`
  - `X-Frame-Options: DENY`
  - `X-Content-Type-Options: nosniff`
  - `Referrer-Policy: no-referrer`
  - `Strict-Transport-Security` in production mode
- Request safety:
  - JSON body size cap
  - basic rate limiting for writes + downloads
- Path privacy:
  - client and peer listing responses do not expose absolute local paths
- Shared-folder lock in production:
  - profile can update username
  - `shared_dir` is locked to startup config

## Run local (dev mode)

```bash
cd /Users/dylanyoung/Documents/New\ project/p2p-archive-index
python3 archive_index.py --db ./archive_index.db serve --host 0.0.0.0 --port 8787 --node-url http://localhost:8787
```

## Run production mode

```bash
python3 archive_index.py --db ./archive_index.db serve \
  --host 0.0.0.0 --port 8787 \
  --shared-dir ./shared \
  --node-url https://your-domain.example \
  --admin-token "replace-with-strong-admin-token" \
  --read-token "replace-with-strong-read-token" \
  --mesh-token "replace-with-strong-mesh-token" \
  --production
```

## Environment variables

- `ARCHIVE_INDEX_DB`
- `ARCHIVE_SHARED_DIR`
- `ARCHIVE_USERNAME`
- `ARCHIVE_NODE_URL`
- `ARCHIVE_BOOTSTRAP_PEERS`
- `ARCHIVE_AUTO_SYNC_INTERVAL`
- `ARCHIVE_SYNC_TIMEOUT`
- `ARCHIVE_SYNC_LIMIT`
- `ARCHIVE_ADMIN_TOKEN`
- `ARCHIVE_READ_TOKEN`
- `ARCHIVE_MESH_TOKEN`
- `ARCHIVE_INDEX_SECRET`
- `ARCHIVE_PRODUCTION`
- `ARCHIVE_ALLOW_SIGNUP` (default true)

## Core routes

- `/` UI
- `/health`
- `/api/profile`
- `/api/entries`
- `/api/browse?path=<relative-folder>`
- `/api/signup`
- `/api/login`
- `/api/rescan`
- `/api/peers`
- `/api/announce`
- `/entries` (peer export)
- `/files/<entry_id>`

## Deploy checklist

1. Deploy behind HTTPS.
2. Set `ARCHIVE_PRODUCTION=true`.
3. Set strong random tokens (16+ chars).
4. Set `ARCHIVE_NODE_URL` to your public HTTPS URL.
5. Set `ARCHIVE_BOOTSTRAP_PEERS` for initial discovery (if multi-node).
6. Keep shared folder limited to intended public content.
