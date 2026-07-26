# Adding Backup Targets (Django Admin)

Concise operator guide for configuring new backup targets in Django Admin.

## Quick Start

1. Open Django Admin and go to `Backup Targets` -> `Add`.
2. Set `Target mode` (`generic_paths` or `service_owned`) and fill minimal fields for that mode.
3. Set a `Schedule` and enable `Schedule required` for targets that must run
   automatically. Leave both unset only for intentionally manual targets.
4. Save.

For remote targets (services on other hosts), see "Adding a Remote Backup Target" below — you cannot just add a BackupTarget in Admin.

Minimal example (SQLite-only, local target):
```
Name: nyxmon
Target mode: generic_paths
FastDeploy service: echoport-backup
Database path: /home/nyxmon/data/db.sqlite3
Service name: nyxmon.service
Schedule: 0 2 * * *
```

## Validation Rules

Read this before entering values to avoid validation errors.
- `Target mode` controls required source fields:
  - `generic_paths`: requires `Service name` and at least one of `Database path` or `Backup files`.
  - `service_owned`: allows both `Database path` and `Backup files` to be empty.
- All paths must be absolute and under the allowlist `ECHOPORT_ALLOWED_PATH_PREFIXES` (default: `/home/`, `/opt/`, `/var/lib/`, `/mnt/cryptdata/`). Paths outside the allowlist are rejected.
- `Backup files` must be a list of paths; in Admin you enter one path per line. For remote targets, each entry must be a directory (not an individual file) — the remote backup script validates this and fails with a clear error otherwise.
- `Schedule` must be a valid cron expression; leave blank for no scheduled runs.
- `Schedule required` rejects a blank schedule while a target is active. If
  direct database changes bypass validation, the health endpoint reports the
  active target as `missing_schedule` and the overall service as `unhealthy`.
- An invalid cron on a target with `Schedule required` also makes overall
  health `unhealthy`; the target status remains `invalid_schedule`.
- The save-time schedule contract is enforced while the target is active. A
  paused required target may be edited, but remains visible as
  `paused_required` and makes health unhealthy. Use disabled—not paused—for
  intentional retirement. A disabled target may clear its schedule while
  retaining the flag as audit history; re-enabling it without restoring a
  valid schedule is rejected.
- Once enabled, `Schedule required` cannot be unchecked on an active or paused
  target through normal model/Admin saves. Disable the target first as part of
  the reviewed retirement procedure.
- `FastDeploy endpoint key` must match a key in `FASTDEPLOY_ENDPOINTS`. The endpoint must have `base_url` and a resolvable token: either a `token` (default), a matching `service_tokens[fastdeploy_service]` entry, or a `Service token` set on the target. Leave blank to use the default FastDeploy endpoint.
- `Service token` overrides all other token sources. If set, the endpoint only needs `base_url` (no `token` or `service_tokens` required).
- Deletion is blocked in Admin. Use `Status = Disabled` to retire a target.

## Field Reference

| Field (Admin Label) | Type | Required | Default | Constraints / Notes | Example |
| --- | --- | --- | --- | --- | --- |
| Name | Text | Yes | None | Unique, max 100 chars | `nyxmon` |
| Description | Text | No | Blank | Human-readable description | `NYXMON production backups` |
| Icon | Text | No | Blank | Emoji or icon identifier, max 50 chars | `📊` |
| Status | Choice | No | `Active` | `Active`, `Paused`, `Disabled`. Disabled is the retirement mechanism (deletion blocked). | `Active` |
| Target mode | Choice | No | `generic_paths` | `generic_paths` (requires restore-safe service/source fields) or `service_owned` (service script owns source logic) | `service_owned` |
| FastDeploy service | Text | Yes | None | FastDeploy service name, max 100 chars | `nyxmon` |
| FastDeploy endpoint key | Text | No | Blank | Max 50 chars. Must exist in `FASTDEPLOY_ENDPOINTS` if set; blank uses default endpoint | `staging` |
| Service token | Text | No | Blank | JWT token for FastDeploy; overrides endpoint/default token if set | (paste JWT) |
| Service name | Text | No | Blank | Systemd service to stop during restore, max 100 chars. Required for `generic_paths` targets. | `nyxmon.service` |
| Restore owner | Text | No | Blank | `user:group` to chown restored files, max 100 chars | `marina:marina` |
| Database path | Text | No | Blank | Absolute path under allowlist, max 500 chars. Required for `generic_paths` only when `Backup files` is empty. | `/home/nyxmon/data/db.sqlite3` |
| Backup files | List (one path per line) | No | Empty list | Absolute paths under allowlist. Required for `generic_paths` only when `Database path` is empty. Remote targets: must be directories. | `/home/nyxmon/uploads` |
| Schedule | Text | No | Blank | Valid cron expression, max 100 chars | `0 2 * * *` |
| Schedule required | Boolean | No | `False` | Enable for targets that must never become manual-only | `True` |
| Retention days | Integer | No | `30` | Days to keep backups | `14` |
| Timeout seconds | Integer | No | `600` | Max time to wait for backup | `900` |
| Storage bucket | Text | No | `backups` | MinIO bucket name, max 100 chars | `backups` |
| Created at | Timestamp | Read-only | Auto | Audit field | (auto) |
| Updated at | Timestamp | Read-only | Auto | Audit field | (auto) |

## Health Response Contract

For each active target, plus each paused target with
`schedule_required=true`, `/api/health/` includes the lifecycle field
`target_status`, `schedule`, `schedule_required`, and one of the computed
health statuses `ok`, `overdue`, `missing_schedule`, `paused_required`,
`invalid_schedule`, or `last_failed`. `inactive_required` is reserved for a
future non-disabled, non-active lifecycle state and cannot be emitted by the
current three-state model. Consumers must use `target_status` rather than
assume every listed target is active. Any future non-disabled lifecycle state
automatically remains visible when the schedule contract is required; only
`disabled` is treated as retirement.

- `missing_schedule` is always unhealthy because it can only occur for a
  required target.
- `invalid_schedule` is unhealthy for required targets and degraded for
  intentionally optional/manual targets.
- `paused_required` is unhealthy. Paused optional and disabled targets are
  absent. Paused required targets have no `next_scheduled` and are not marked
  overdue, because no run can occur until they are resumed. Their historical
  failures are also suppressed from `recent_failures` so the lifecycle
  violation is the sole reported reason; inspect run history before deciding
  to resume.
- A latest failed/timeout run is unhealthy for required targets. Historical
  failures followed by success remain a degraded audit signal.
- Monitoring for a declared Tier 1 target must require four named
  assertions: `targets_by_name.<name>.status == "ok"`,
  `targets_by_name.<name>.next_scheduled != null`, and
  `targets_by_name.<name>.schedule_required == true`, plus
  `targets_by_name.<name>.schedule == "<declared cron>"`. The exact cadence
  assertion detects a valid but widened cron; the others detect a missing,
  deleted, disabled, contract-cleared, or unscheduled row even when generic
  overall health cannot infer that the target was expected.
  Retiring such a target therefore requires removing its declarative target
  and named monitoring assertion in the same reviewed operations change;
  merely disabling it intentionally leaves that expected-target monitor red.

The production Nyxmon evaluator's absent-key behavior was exercised on
2026-07-26 by substituting a nonexistent target name into a copy of the live
four-check configuration while polling the live Echoport response. All four
paths resolved to null, all four critical assertions failed, and the evaluator
returned `error`/`threshold_failed`. The live check itself was not modified.

### Changing an approved schedule

1. Change the declarative target schedule and the exact named-monitor
   assertion in the same reviewed ops-control change.
2. Deploy the registration playbook so it applies both values together.
3. Verify the named row reports the new `schedule` and non-null
   `next_scheduled`, then verify Nyxmon is green. Never loosen the exact
   assertion merely to accommodate an unreviewed cadence change.

### Reviewed retirement procedure

1. Prepare and review the ops-control change that retires the declarative
   target registration and its named monitoring assertions; do not deploy it
   yet. Freeze all target registration runs and keep them frozen until that
   ops-control change is deployed in step 4.
2. In one save, set the target to `disabled`. The named monitor turning red is
   expected and must remain acknowledged during this short window.
3. In a separate save, clear `schedule_required` and the schedule while the
   target remains disabled.
4. Deploy the prepared ops-control change so automation cannot reactivate the
   target, then verify the replacement/retirement monitoring state. Retain
   backup artifacts according to the applicable retention decision.

When introducing the field to the current deployment, deploy Echoport to apply
the schema migration, then, from the ops-control checkout, immediately run
`just register-vaultwarden-backup`, then verify the named Nyxmon monitor. The
field defaults false so existing
intentional manual-only targets do not become scheduled by an application
migration. A failed or skipped upsert must leave the named monitor red and
block completion of the rollout.

Rollback order is: first freeze registration runs and deploy the reviewed
ops-control rollback that removes/holds both the Vaultwarden target declaration
and its named monitoring assertions; then deploy reverted model/view/admin code
while intentionally retaining migration file
`0008_backuptarget_schedule_required` in that revert commit (or copy the file
into the deployed tree before migration); then run
`migrate backups 0007` to drop the now-unused column; and only then remove the
migration file. Keep registration frozen until the ops-control rollback is
deployed; otherwise its direct model assignment can appear to succeed after
the field is gone without persisting a contract. Never run old application code
before the column exists, or new application code after it is dropped.
Reversing the migration permanently discards active and retired-target audit
flags; take/verify the Echoport database backup and record affected targets
before doing so. Also freeze target creation while old code runs against the
still-present 0008 column: old INSERTs omit its required value and fail. After
reversing to 0007, do not run any deploy or automatic `migrate` until migration
file 0008 has been removed, or Django will reapply it.

The application health view is intentionally unauthenticated for monitoring,
so deployments must bind the application port to loopback and put any ingress
behind authentication. The production ops-control deployment asserts
`echoport_app_host=127.0.0.1`, and the Vaultwarden Nyxmon check polls that
loopback listener. Do not expose the application port directly to an untrusted
network; the response includes target names, lifecycle state, backup
timestamps, and declared cron expressions.

The save-time contract is an application plus monitoring invariant rather than
a database check constraint. Out-of-band `QuerySet.update()`, fixture loading,
and direct SQL are prohibited for target configuration; they can bypass
`clean()`. The health response and named fail-closed monitor deliberately retain
drift states so restored legacy databases or unauthorized writes become
visible instead of being mistaken for healthy configuration.

Required targets are not silently pausable. For planned maintenance, establish
an acknowledged monitoring maintenance window first; pausing the target then
intentionally reports `paused_required`/unhealthy until it is resumed. Disabled
means retirement, not temporary maintenance.

## Common Configurations

SQLite app (DB + files):
```
Name: nyxmon
Target mode: generic_paths
FastDeploy service: echoport-backup
Database path: /home/nyxmon/data/db.sqlite3
Service name: nyxmon.service
Backup files:
/home/nyxmon/uploads
/home/nyxmon/media
Schedule: 0 2 * * *
```

Files-only backup:
```
Name: assets
Target mode: generic_paths
FastDeploy service: echoport-backup
Service name: assets.service
Backup files:
/opt/assets/shared
/var/lib/assets/uploads
Schedule: 30 1 * * *
```

Custom FastDeploy endpoint:
```
Name: nyxmon-staging
Target mode: generic_paths
FastDeploy service: nyxmon-staging-backup
FastDeploy endpoint key: staging
Database path: /home/nyxmon/data/db.sqlite3
Service name: nyxmon.service
Schedule: 0 3 * * *
```

FastDeploy local PostgreSQL target (service-owned script):
```
Name: fastdeploy
Target mode: service_owned
FastDeploy service: fastdeploy-backup
Service name: fastdeploy
Database path: (leave empty)
Backup files: (leave empty)
Schedule: 0 3 * * *
Timeout seconds: 1200
```

## FastDeploy Endpoint Configuration

FastDeploy uses JWT tokens that are service-specific. Each token is authorized for a particular service. Configure `FASTDEPLOY_ENDPOINTS` in settings with per-service tokens:

```python
FASTDEPLOY_ENDPOINTS = {
    "staging": {
        "base_url": "https://deploy.staging.example.com",
        "token": "default-fallback-token",  # Optional fallback
        "service_tokens": {
            "marina-staging-backup": "token-for-marina-backup",
            "nyxmon-staging-backup": "token-for-nyxmon-backup",
        }
    }
}
```

Token lookup order (when `FastDeploy endpoint key` is set):
1. If `Service token` is set on the target, use that
2. Else if endpoint has `service_tokens[fastdeploy_service]`, use that
3. Else use the endpoint's `token` field
4. If none found, the backup will fail with an endpoint configuration error

Token lookup order (when `FastDeploy endpoint key` is blank — default endpoint):
1. If `Service token` is set on the target, use that
2. Else use the `FASTDEPLOY_SERVICE_TOKEN` setting

## Understanding Local vs Remote Backup Targets

There are two types of backup targets:

1. **Local targets** (e.g., nyxmon, echoport, fastdeploy) - Services running ON macmini
   - Most local SQLite targets use `echoport-backup`.
   - Some local PostgreSQL targets use dedicated service-owned scripts (for example `fastdeploy-backup`).
   - Local scripts access files directly via filesystem (no SSH required).

2. **Remote targets** (e.g., marina-staging) - Services running on OTHER hosts
   - Require a DEDICATED FastDeploy service (e.g., `marina-staging-backup`)
   - Backup script runs on macmini but uses SSH/SCP/rsync to access remote files
   - Remote host has NO backup tools - all logic runs on macmini
   - `Backup files` must be directories (not individual files)

**CRITICAL**: For remote targets, you cannot just add a BackupTarget in Django Admin. You must FIRST register a FastDeploy service that knows how to SSH to the remote host. See "Adding a Remote Backup Target" below.

## Adding a Remote Backup Target (Complete Workflow)

Adding backup/restore for a service on a remote host requires multiple steps:

1. **Create the backup script template** in ops-library:
   - Template: `ops-library/roles/echoport_backup/templates/<service>_backup.py.j2`
   - Must handle SSH/SCP/rsync to the remote host
   - Example: `marina_staging_backup.py.j2`

2. **Create the registration playbook** in ops-control:
   - Playbook: `ops-control/playbooks/register-<service>-backup.yml`
   - Must configure:
     - SSH access (user, known_hosts)
     - mc (MinIO client) alias for the user running the script
     - Sudoers if needed
     - Remote hostname (use FQDN, not short names)

3. **Register the FastDeploy service**:
   ```bash
   cd ops-control
   just register-one <service>-backup
   # or use the shortcut if one exists, e.g.:
   just register-marina-staging-backup
   ```
   After registration, trigger a FastDeploy service sync so it picks up the new service:
   ```bash
   curl -X POST https://deploy.home.xn--wersdrfer-47a.de/services/sync
   ```

4. **Verify SSH connectivity** (pre-flight check):
   The backup script runs as the local user (e.g., `deploy`) on macmini and SSHes to the remote host as the configured remote user (e.g., `root`). Test this before proceeding:
   ```bash
   sudo -u deploy ssh -o StrictHostKeyChecking=yes -o BatchMode=yes root@<fqdn> "echo connected"
   ```
   If this fails, fix SSH key access and known_hosts before continuing.

5. **Generate a service token** for the new FastDeploy service:
   - Via FastDeploy admin UI, or
   - Via API: `POST /api/service-token {"service": "<service>-backup"}`
   - **CRITICAL**: The token MUST be for the correct service name

6. **Add BackupTarget in Echoport Django Admin**:
   - `FastDeploy service`: the registered service name (e.g., `marina-staging-backup`)
   - `Service token`: paste the JWT from step 5 (NOT in `FastDeploy endpoint key`)
   - `Database path`: absolute path on the remote host (e.g., `/home/marina/site/db.sqlite3`)
   - `Backup files`: directories only, one per line (e.g., `/home/marina/site/uploads`)
   - `Service name`: systemd unit to stop/start on the remote host (e.g., `marina.service`)
   - `Restore owner`: `user:group` for chown after restore (e.g., `marina:marina`)
   - `Schedule`, retention, timeout as needed

7. **Test the backup** via Echoport UI

## Common Mistakes When Adding Remote Targets

| Mistake | Symptom | Fix |
|---------|---------|-----|
| Using wrong service token | Wrong backup script runs (error message format differs) | Generate token for correct service name |
| Token pasted in wrong field | "Unknown endpoint key 'eyJhbG...'" | Paste in `Service token`, NOT `FastDeploy endpoint key` |
| Short hostname instead of FQDN | "Cannot resolve db_path" or SSH fails | Use full hostname (e.g., `staging.wersdoerfer.de` not `staging`) |
| mc alias not configured | "Insufficient permissions" on MinIO upload | Add mc alias setup to registration playbook |
| Local user lacks SSH access to remote host | "Cannot resolve db_path" | Ensure the local user (e.g., deploy) has an SSH key authorized on the remote host for the configured remote_user (e.g., root) |
| Missing known_hosts entry | SSH fails with host key error | Add known_hosts setup to registration playbook |
| Backup files entry is a file, not a directory | "backup_files must be directories, not files" | Remote backup scripts require directories; use the parent directory instead |

## How to Identify Which Script is Running

If backup fails, check the error message format to identify which script ran:

- `echoport-backup` (backup.py.j2): Messages say "Configuration loaded for <target-name>"
- Remote backup scripts: Messages say "Configuration validated" (no target name)

If you see the wrong format, your service token is for the wrong FastDeploy service.

## Troubleshooting

- Error: `At least one of 'Database path' or 'Backup files' must be specified.`
Fix: Provide a `Database path`, `Backup files`, or both.
- Error: `Path must be absolute` or `Path must be under one of: ...`
Fix: Use absolute paths and ensure they live under `ECHOPORT_ALLOWED_PATH_PREFIXES`. If needed, adjust the allowlist in settings and redeploy.
- Error: `Invalid cron expression: ...`
Fix: Use a standard 5-field cron expression (e.g., `0 2 * * *`).
- Error: `Unknown endpoint key '...'`
Fix: Use a key configured in `FASTDEPLOY_ENDPOINTS`, or leave blank for the default endpoint. If the error shows a JWT token, you pasted the token in the wrong field.
- Error: `Endpoint '...' is incomplete: missing ...`
Fix: The error lists which fields are missing (e.g., `base_url`, `token`, or `token (no default and no service_tokens['<service>'])`). Ensure the endpoint has `base_url` and a resolvable token — either a `token` default, a `service_tokens` entry for your service, or a `Service token` on the target.
- Error: `Cannot resolve db_path: ...`
Fix: SSH connection to remote host failed. Check: (1) Use FQDN not short hostname, (2) the local user (e.g., deploy) has an SSH key authorized on the remote host for the configured remote_user (e.g., root), (3) known_hosts is configured. Test with: `sudo -u deploy ssh -o BatchMode=yes root@<fqdn> "echo connected"`
- Error: `Failed to backup database: ...` with wrong script
Fix: Your service token is for the wrong FastDeploy service. Generate a new token for the correct service.
- Error: `Insufficient permissions` on MinIO upload
Fix: The user running the backup script needs mc configured. Add mc alias setup to the registration playbook.
- Error: `backup_files must be directories, not files: ...`
Fix: Remote backup scripts only support directories. Use the parent directory path instead of individual file paths.
- Can't delete a target
Fix: Deletion is blocked to preserve audit history. Set `Status = Disabled` instead.

## Reference: Existing Remote Backup Implementations

For a complete example of a remote backup setup, see:
- Playbook: `ops-control/playbooks/register-marina-staging-backup.yml`
- Script: `ops-library/roles/echoport_backup/templates/marina_staging_backup.py.j2`
- Docs: `ops-control/docs/marina-staging-backup.md`
