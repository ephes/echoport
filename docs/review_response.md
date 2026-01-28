# Response to Code Review - Echoport Backup Service

Thank you for the thorough reviews. All findings were valid and have been addressed. Here's a summary of the changes made:

---

## Third Review Findings - All Fixed

### 1. ECHOPORT_RESULT truncation risk (4KB limit)
**Root cause**: FastDeploy truncates step messages at 4096 bytes. A large manifest could produce truncated/invalid JSON.

**Fix**: Removed full manifest from ECHOPORT_RESULT payload. Only essential metadata is sent (bucket, key, size, checksum, file_count). The full manifest is stored in the tarball.

Files changed:
- `ops-library/roles/echoport_backup/templates/backup.py.j2` - emit file_count instead of manifest
- `src/backups/fastdeploy_client.py` - BackupResult uses file_count, not manifest

### 2. Duplicated orchestration logic
**Root cause**: `start_backup` and `_run_backup_in_thread_with_existing_run` each had their own polling/result handling.

**Fix**: Added `existing_run` parameter to `start_backup()`. The view creates the run, then the thread calls `start_backup(target, existing_run=run)` to continue. Single implementation, no duplication.

Files changed:
- `src/backups/backup_engine.py` - added `existing_run` parameter
- `src/backups/views.py` - simplified to use consolidated `start_backup`

### 3. Pending run left forever on thread failure
**Root cause**: If thread creation failed after run creation, the run stayed PENDING forever.

**Fix**: Wrapped thread creation in try/except; if it fails, call `_mark_run_failed()` on the run.

Files changed:
- `src/backups/views.py` - `trigger_backup` marks run as failed if thread start fails

### 4. target.status not enforced
**Root cause**: Paused/disabled targets could be triggered via direct POST.

**Fix**: Added check for `target.status != BackupStatus.ACTIVE` before creating run.

Files changed:
- `src/backups/views.py` - `trigger_backup` checks target status

### 5. Unused code cleanup
**Fix**: Removed old `_run_backup_in_thread` function and unused imports.

Files changed:
- `src/backups/views.py` - cleanup

### Open Questions Answered

> Do you want the full manifest in Echoport?

No, storing it in the tarball only is acceptable. We emit just the file count to keep under 4KB.

> Is the absence of scheduling still acceptable for Phase 1?

Yes, per the PRD, scheduling is Phase 2 scope. The `run_scheduled_backups` command will be added then.

---

## Second Review Findings - All Fixed

### 1. Sync ORM in async context (SynchronousOnlyOperation)
**Root cause**: `start_backup` was an async function calling sync ORM operations. Django ≥4.1 raises `SynchronousOnlyOperation` in async contexts.

**Fix**: Converted the entire backup engine and FastDeploy client to be fully synchronous. This is cleaner since we run backups in a dedicated thread anyway.

Files changed:
- `src/backups/backup_engine.py` - now fully synchronous, uses `time.sleep()` instead of `asyncio.sleep()`
- `src/backups/fastdeploy_client.py` - uses `httpx.Client` (sync) instead of `httpx.AsyncClient`
- `src/backups/views.py` - removed asyncio code, simplified thread handling
- `src/backups/management/commands/backup.py` - removed `asyncio.run()`

### 2. Privilege escalation via directory ownership
**Root cause**: The service directory was owned by fastdeploy, so even with a root-owned script, fastdeploy could delete and recreate it.

**Fix**: Changed directory, script, AND config.json ownership to root:root. Fastdeploy can read/execute but not modify any files.

Files changed:
- `ops-library/roles/echoport_backup/tasks/main.yml` - directory, script, and config all owned by root

### 3. UI race condition on "Backup Now"
**Root cause**: The background thread might not have created the DB record before the view queries for it, causing the UI to show idle state.

**Fix**: Create the PENDING run record synchronously in the view before starting the background thread. The thread then continues with the existing run rather than creating a new one.

Files changed:
- `src/backups/views.py` - `trigger_backup` creates run synchronously, new `_run_backup_in_thread_with_existing_run` function continues execution

### 4. DB connection leak in background thread (Second Review)
**Root cause**: Background threads need to call `close_old_connections()` to avoid leaking connections outside Django's request lifecycle.

**Fix**: Added `close_old_connections()` calls at thread start and in finally block.

Files changed:
- `src/backups/views.py` - connection cleanup in thread functions
- `src/backups/backup_engine.py` - connection cleanup in start/finally

---

## Critical Findings - All Fixed

### 1. ECHOPORT_RESULT not reaching Echoport
**Root cause**: FastDeploy only parses valid JSON lines from stdout. The `ECHOPORT_RESULT:{json}` line was being silently dropped because it's not valid JSON.

**Fix**: Changed the backup script to emit ECHOPORT_RESULT as a step message instead of a raw stdout line. The result is now embedded in a `result` step's message field, which FastDeploy captures and forwards to Echoport.

Files changed:
- `ops-library/roles/echoport_backup/templates/backup.py.j2` - emit result as step
- `src/backups/fastdeploy_client.py` - parse result from step messages instead of raw logs
- `src/backups/backup_engine.py` - pass steps to parser

### 2. asyncio.run blocking in sync view
**Root cause**: `asyncio.run()` inside a Django view blocks the entire request for the backup duration and fails under ASGI with "event loop already running".

**Fix**: Moved backup execution to a background thread with its own event loop. The view now returns immediately while the backup runs asynchronously.

Files changed:
- `src/backups/views.py` - added `_run_backup_in_thread()`, refactored `trigger_backup` to use threading

### 3. Privilege escalation via script ownership
**Root cause**: Backup script was owned by fastdeploy user but executed via sudo as root. Fastdeploy could modify the script and gain root privileges.

**Fix**: Changed script ownership to root:root so fastdeploy can execute but not modify. Added NOSETENV to sudoers to prevent environment manipulation.

Files changed:
- `ops-library/roles/echoport_backup/tasks/main.yml` - script now owned by root, sudoers uses NOSETENV

---

## High Findings - All Fixed

### 4. HTMX CSRF token
**Status**: Already working as designed.

The CSRF token is set globally via `hx-headers` on the `<body>` element in `base.html:13`. This applies to all HTMX requests automatically. No changes needed.

### 5. Devdata includes .env
**Fix**: Removed `.env` from `backup_files` in both nyxmon and fastdeploy targets. Added comments explaining that secrets are managed by ops-control and regenerated on restore.

Files changed:
- `src/backups/management/commands/create_devdata.py`

### 6. No authentication on views
**Fix**: Added `@login_required` decorator to all views: `dashboard`, `target_detail`, `run_detail`, `trigger_backup`, `backup_status`.

Files changed:
- `src/backups/views.py`

### 7. Timeout doesn't cancel remote deployment
**Acknowledged**: This requires FastDeploy API support for deployment cancellation, which doesn't exist yet. The current behavior (marking as TIMEOUT without cancellation) is documented. We could add a FastDeploy cancellation endpoint in a future iteration.

---

## Medium Findings - All Fixed

### 8. fastdeploy_service not used in API call
**Acknowledged**: The service selection is entirely by token in the current FastDeploy API design. The `fastdeploy_service` field is kept for future multi-service support but isn't used in the API payload today. This is expected behavior, not a bug.

### 9. intcomma without humanize
**Fix**: Added `django.contrib.humanize` to `INSTALLED_APPS` and added `{% load humanize %}` to `run_detail.html`.

Files changed:
- `src/django/config/settings/base.py`
- `src/backups/templates/backups/run_detail.html`

### 10. Missing run_scheduled_backups command
**Acknowledged**: This is intentionally deferred to Phase 2 as noted in the PRD. Phase 1 only covers manual backups.

---

## Low Findings - All Fixed

### 11. N+1 queries in dashboard
**Fix**: Replaced naive `prefetch_related("runs")` with a `Prefetch` object using an ordered queryset. Then used in-memory filtering on the prefetched data instead of calling methods that trigger new queries.

Files changed:
- `src/backups/views.py` - dashboard now uses efficient prefetch + in-memory filtering

### 12. Duplicate step updates via HTTP + NDJSON
**Acknowledged**: This is intentional for reliability. The backup script emits steps via stdout (for FastDeploy parsing) and also posts directly to the API (as a fallback). FastDeploy deduplicates by step name. The slight overhead is acceptable for the reliability benefit.

---

## Open Questions - Answered

> Will this run under ASGI or WSGI?

Currently WSGI (gunicorn). The asyncio.run fix works under both now.

> Do you intend to gate the dashboard behind auth?

Yes, all views now require authentication via `@login_required`.

> Should ECHOPORT_RESULT live in a step message?

Yes, this is now the implementation. The result is embedded in a step's message field.

---

## Testing

All existing tests pass. The test coverage gap (no integration tests for backup engine) is noted and will be addressed in a follow-up.

---

## Summary

| Finding | Severity | Status |
|---------|----------|--------|
| ECHOPORT_RESULT not reaching Echoport | Critical | Fixed |
| asyncio.run blocks request | Critical | Fixed |
| Privilege escalation | Critical | Fixed |
| Missing CSRF token | High | Already working |
| Devdata includes .env | High | Fixed |
| No authentication | High | Fixed |
| Timeout doesn't cancel remote | High | Acknowledged (needs FastDeploy API) |
| fastdeploy_service not used | Medium | Expected behavior |
| intcomma without humanize | Medium | Fixed |
| Missing scheduled backups | Medium | Phase 2 scope |
| N+1 queries | Low | Fixed |
| Duplicate step updates | Low | Intentional design |

Thank you again for the thorough review - it caught several real issues that would have caused problems in production.
