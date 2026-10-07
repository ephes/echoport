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

## Dashboard

- **Dashboard** (`/`): one card per target with the last backup and a "Backup Now" button.
- **Target page** (`/targets/<id>/`): configuration, the last 50 backups and the last 20 restores. Each restore row links to its restore page and to the backup it was restored from, with status, start time, duration and who triggered it.
- **Backup run page** (`/runs/<id>/`): result, logs and the last 20 restores made from that backup. Staff users also see the "Restore Now" action for successful backups.
- **Restore page** (`/restores/<id>/`): status (polled while running), source backup, logs and errors.

The restore lists are read-only; restores are still started only from a backup run page or with `manage.py restore`.

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
| `ECHOPORT_STALE_RUN_GRACE_SECONDS` | Grace added to a target's `timeout_seconds` before a still pending/running run is reaped as stale (default: `900`) |
| `ECHOPORT_LATE_RESULT_WINDOW_SECONDS` | How long after it started a timed-out backup run is checked for a late archive (default: `86400`) |
| `ECHOPORT_HEALTH_OVERDUE_GRACE_MINUTES` | Minutes after the first missed cron time before `/api/health/` reports a target without a successful run as `overdue` (default: `60`; `0` restores immediate overdue). See [Health Response Contract](docs/adding-backup-targets.md#health-response-contract) |

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
| `run_scheduled_backups` | Reap stale runs, record late archives of timed-out backups, then check and run due scheduled backups (cron); `--reap-only` only reaps |
| `cleanup_old_backups` | Delete backups older than retention_days |
| `check_service_tokens` | Report FastDeploy service tokens that are legacy, expiring, expired or unusable; exits 1 if any need attention (`--warn-days`, default 21) |
| `create_devdata` | Create development backup targets |
| `ensure_superuser` | Create/update admin user (deployment) |

Run via: `cd src/django && uv run python manage.py <command>`

Target names are unique regardless of case, but commands look them up exactly
(`backup Nyxmon` does not run `nyxmon`); a near miss is reported with the
correctly cased name.

Restoring Echoport's own target is blocked in the web UI because the restore
stops the running service; use `manage.py restore <target> <backup_run_id>`.
The UI recognizes the self target by name (`echoport`, any case), by the
FastDeploy service `echoport-self-backup`, or by the systemd unit
`echoport.service`.

Or use Justfile shortcuts: `just backup <target>`, `just devdata`

### Stale runs

Only one backup and one restore can be active (pending/running) per target.
The process that starts a run also polls it, so a deploy, gunicorn restart,
OOM kill or killed cron run can leave a run active forever, which would block
that target's scheduled backups and UI restores. Echoport therefore reaps a
run that is still pending/running when it is older than the target's
`timeout_seconds` plus `ECHOPORT_STALE_RUN_GRACE_SECONDS` (default 15 min):
it is marked `timeout` with an error message saying it was reaped. Reaping
happens at the start of every `run_scheduled_backups` pass (not with
`--dry-run`), with `run_scheduled_backups --reap-only`, and before a backup or
restore of the same target starts (CLI, scheduler or UI). A reaped run never
flips back to success or failure if its original process turns out to be
alive and finishes later. Keep the grace above the longest real poll overrun.

### Late results of timed-out backups

FastDeploy has no cancel API, so marking a backup run `timeout` (by the
polling engine or by the reaper) does not stop its deployment. If that
deployment finishes later, every normal `run_scheduled_backups` pass (not
`--dry-run` or `--reap-only`) collects its outcome while the run is younger
than `ECHOPORT_LATE_RESULT_WINDOW_SECONDS` (default 24 h). The run's logs get
the deployment's step output, prefixed with `[late result]`, and the run is
not checked again. If the deployment reported a successful upload (even if a later step failed), the
archive's storage key, size, checksum and file count are recorded on the run
and its error message says so, so the archive is not left unreferenced. The
run stays `timeout`. A deployment still running, or one FastDeploy cannot be
reached for, is retried on the next pass.

### Service token expiry

Every backup authenticates to FastDeploy with a service token (a JWT): the
target's own `Service token`; else, when the target has a FastDeploy endpoint
key, that endpoint's `service_tokens` entry or default `token`; else (blank
endpoint key only) `FASTDEPLOY_SERVICE_TOKEN`. A named endpoint never falls
back to the global token. FastDeploy rejects a token
after its `exp`, and rejects a legacy token (one without `jti`, minted outside
FastDeploy's token registry) once its `LEGACY_SERVICE_TOKENS_ACCEPTED_UNTIL`
grace ends. Either way every backup using that token fails with HTTP 401.

`manage.py check_service_tokens [--warn-days 21]` lists the token each
non-disabled target would use, plus the global token, with its source,
expiry, legacy flag and status:

- `ok`: has a `jti` and does not expire within the warning window
- `expiring`: expires within `--warn-days`
- `expired`: already past `exp`
- `legacy`: no `jti`; re-issue it through FastDeploy's token registry
- `undecodable`: not a JWT Echoport can read
- `missing`: no token resolves for the target (or its endpoint config is invalid)

The global token shows `unset` when it is empty; that only matters for targets
that fall back to it, and those show `missing`. The command exits 1 when any
row needs attention, so it can run from cron or a monitor. The admin target
list has a matching "Token expires" column with a badge for any status other
than `ok`. Tokens are decoded without signature verification and only `exp`,
the presence of `jti` and the `service` claim are read; token values are never
printed or rendered.

## Tests

Run `just test` (or `.venv/bin/pytest`); `just check` adds lint and type checking.
Lint uses the ruff version locked in the dev dependencies (`uv run ruff check .`).
GitHub Actions (`.github/workflows/ci.yml`) runs the same lint, mypy and pytest
steps on every push and pull request; no secrets or services are needed.
Tests never contact FastDeploy: the `fake_fastdeploy` fixture in `tests/conftest.py`
replaces the client in both engines with a scripted fake (deployment id or start
error, a sequence of statuses or poll errors) and records poll sleeps instead of
sleeping. View tests replace `threading.Thread` and capture `on_commit` callbacks,
so no background backup or restore thread is started.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| **Backup stuck in PENDING** | Check FastDeploy logs, verify `FASTDEPLOY_SERVICE_TOKEN` |
| **Run stuck pending/running after a restart ("backup already in progress")** | The process polling it was killed. It is reaped as `timeout` once older than the target timeout plus `ECHOPORT_STALE_RUN_GRACE_SECONDS` (the next scheduler pass, or `manage.py run_scheduled_backups --reap-only`, does this without waiting for a due backup). Runs younger than that are left alone |
| **Timed-out backup run shows a storage key** | Its deployment finished after the timeout and uploaded an archive; the scheduler recorded it (see "Late results of timed-out backups"). The run stays `timeout`; raise the target's `timeout_seconds` if this happens regularly |
| **Backups fail with HTTP 401** | The FastDeploy service token expired or is a legacy token FastDeploy no longer accepts. Run `manage.py check_service_tokens` and re-issue the flagged tokens |
| **MinIO upload failed** | Check `mc alias` configuration and bucket permissions |
| **Restore blocked** | Restore requires valid checksum. Re-run backup if checksum missing |
| **Scheduled backups not running** | Check cron/service logs at `/home/echoport/logs/scheduler.log` |
| **Health reports `missing_schedule`** | A target marked `schedule_required` lost its cron schedule; restore the declared target configuration |
| **Required target reports `invalid_schedule`** | Its cron expression is malformed and overall health is `unhealthy`; restore a valid declared schedule |
| **Health reports `paused_required`** | Resume the target; planned pauses require an acknowledged monitor maintenance window. For permanent removal follow the [reviewed retirement procedure](docs/adding-backup-targets.md#reviewed-retirement-procedure) |
| **Health reports `inactive_required`** | A future non-disabled lifecycle state prevents scheduled operation; inspect `target_status`, restore the active declared state, and update monitoring/runbooks for that lifecycle state |
| **Health reports `overdue`** | No successful run since the last cron time, the grace window (`ECHOPORT_HEALTH_OVERDUE_GRACE_MINUTES`) has passed and no run for this cycle is in progress. Check the scheduler log and the run history; `overdue_hours` counts from the end of the grace window. A target that never succeeded is always overdue |
| **Required target reports `last_failed`** | Trigger a manual backup after fixing the cause; health returns from `unhealthy` only after a successful run |
| **`migrate` fails: names differ only by case** | Migration 0009 adds case-insensitive name uniqueness and refuses to run while targets such as `Echoport` and `echoport` coexist. Rename or delete the listed duplicates, then run `migrate` again |
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
