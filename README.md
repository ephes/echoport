# Echoport

## Overview

Backup orchestration service for homelab deployments. Triggers service-owned backup/restore workflows through FastDeploy and stores artifacts in MinIO.

## Quick Start

```bash
# Clone and install
git clone https://github.com/your-repo/echoport.git
cd echoport
uv sync

# Configure environment
cp .env.example .env
chmod 600 .env
# Edit .env with your settings

# Initialize database
just migrate
just devdata  # optional: creates test targets

# Run development server
just dev
```

## Architecture

```
User (Dashboard/Cron)
        │
        ▼
┌───────────────┐
│   Echoport    │ ← Orchestration layer
│   (Django)    │
└───────┬───────┘
        │ HTTP API
        ▼
┌───────────────┐
│  FastDeploy   │ ← Execution layer (runs on macmini)
└───────┬───────┘
        │
   ┌────┴────┐
   ▼         ▼
service scripts  MinIO
   │
   ▼
Services (SQLite + PostgreSQL targets)
```

Echoport triggers backup and restore deployments via FastDeploy's API. Backup logic lives in service scripts (generic SQLite or dedicated service-owned implementations such as PostgreSQL flows), and artifacts are uploaded to MinIO. See [PRD](specs/2026-01-27_initial_prd.md) for detailed architecture.

Backup target model note:
- Targets use `target_mode` with two contracts:
  - `generic_paths`: requires `service_name` plus at least one source (`db_path` or `backup_files`)
  - `service_owned`: service script determines sources; `db_path`/`backup_files` may be empty
- Default allowed source prefixes include `/home/`, `/opt/`, `/var/lib/`, and `/mnt/cryptdata/`.

## Safe Usage

- **SQLite backups are safe**: service scripts use `sqlite3 .backup` for live SQLite snapshots. Prefer low-traffic windows for large databases.
- **Restore stops services**: Restore operations stop the target service before overwriting files.
- **Checksum verification**: Backups include SHA256 checksums. Restore verifies checksum before applying.
- **Verify backup contents**: `tar -tzf <backup>.tar.gz` to list files.
- **Secrets not backed up**: `.env` files are excluded. Regenerate via `just deploy-one <service>` from ops-control.

## Secrets & Environment

### Required

| Variable | Description |
|----------|-------------|
| `DJANGO_SECRET_KEY` | Django secret key |
| `FASTDEPLOY_BASE_URL` | FastDeploy API URL |
| `FASTDEPLOY_SERVICE_TOKEN` | Token for FastDeploy auth |

### Optional

| Variable | Description |
|----------|-------------|
| `ECHOPORT_CACHE_DIR` | Lock file location (default: system temp) |

### MinIO Configuration

MinIO credentials are configured via `mc alias` on the server, not in Echoport's `.env`:

```bash
mc alias set myminio https://minio.example.com ACCESS_KEY SECRET_KEY
```

### Security

```bash
chmod 600 .env  # Restrict .env permissions
```

## Management Commands

| Command | Description |
|---------|-------------|
| `backup <target>` | Run manual backup for a target |
| `run_scheduled_backups` | Check and run due scheduled backups (cron) |
| `cleanup_old_backups` | Delete backups older than retention_days |
| `create_devdata` | Create development backup targets |
| `ensure_superuser` | Create/update admin user (deployment) |

Run via: `cd src/django && uv run python manage.py <command>`

Or use Justfile shortcuts: `just backup <target>`, `just devdata`

## Troubleshooting

| Problem | Solution |
|---------|----------|
| **Backup stuck in PENDING** | Check FastDeploy logs, verify `FASTDEPLOY_SERVICE_TOKEN` |
| **MinIO upload failed** | Check `mc alias` configuration and bucket permissions |
| **Restore blocked** | Restore requires valid checksum. Re-run backup if checksum missing |
| **Scheduled backups not running** | Check cron/service logs at `/home/echoport/logs/scheduler.log` |
| **Permission denied** | Verify backup script is root-owned, check sudoers config |

## Limitations

- PostgreSQL is supported via dedicated service-owned scripts, not a shared generic PostgreSQL schema in Echoport
- No client-side encryption (relies on MinIO server security)
- Single admin user (no multi-user access control)

## Deployment

Echoport is deployed via the `echoport_deploy` role in [ops-library](https://github.com/your-repo/ops-library).

```bash
# From ops-control
just deploy-one echoport
```

See [PRD](specs/2026-01-27_initial_prd.md) for deployment architecture details.
