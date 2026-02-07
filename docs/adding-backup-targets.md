**Adding Backup Targets (Django Admin)**
Concise operator guide for configuring new backup targets in Django Admin.

**Quick Start**
1. Open Django Admin and go to `Backup Targets` -> `Add`.
2. Fill the minimal fields: `Name`, `FastDeploy service`, and either `Database path` or `Backup files`.
3. Optionally set a `Schedule` and adjust retention/timeout.
4. Save.

Minimal example (SQLite-only):
```
Name: nyxmon
FastDeploy service: nyxmon
Database path: /home/nyxmon/data/db.sqlite3
Schedule: 0 2 * * *
```

**Validation Rules**
Read this before entering values to avoid validation errors.
- At least one of `Database path` or `Backup files` is required.
- All paths must be absolute and under the allowlist `ECHOPORT_ALLOWED_PATH_PREFIXES` (default: `/home/`, `/opt/`, `/var/lib/`). Paths outside the allowlist are rejected.
- `Backup files` must be a list of paths; in Admin you enter one path per line.
- `Schedule` must be a valid cron expression; leave blank for no scheduled runs.
- `FastDeploy endpoint key` must match a key in `FASTDEPLOY_ENDPOINTS`. The endpoint must have `base_url` and a resolvable token: either a `token` (default), a matching `service_tokens[fastdeploy_service]` entry, or a `Service token` set on the target. Leave blank to use the default FastDeploy endpoint.
- `Service token` overrides all other token sources. If set, the endpoint only needs `base_url` (no `token` or `service_tokens` required).
- Deletion is blocked in Admin. Use `Status = Disabled` to retire a target.

**Field Reference**
| Field (Admin Label) | Type | Required | Default | Constraints / Notes | Example |
| --- | --- | --- | --- | --- | --- |
| Name | Text | Yes | None | Unique, max 100 chars | `nyxmon` |
| Description | Text | No | Blank | Human-readable description | `NYXMON production backups` |
| Icon | Text | No | Blank | Emoji or icon identifier, max 50 chars | `📊` |
| Status | Choice | No | `Active` | `Active`, `Paused`, `Disabled`. Disabled is the retirement mechanism (deletion blocked). | `Active` |
| FastDeploy service | Text | Yes | None | FastDeploy service name, max 100 chars | `nyxmon` |
| FastDeploy endpoint key | Text | No | Blank | Must exist in `FASTDEPLOY_ENDPOINTS` if set; blank uses default endpoint | `staging` |
| Service token | Text | No | Blank | JWT token for FastDeploy; overrides endpoint/default token if set | (paste JWT) |
| Service name | Text | No | Blank | Systemd service to stop during restore | `nyxmon.service` |
| Restore owner | Text | No | Blank | `user:group` to chown restored files | `marina:marina` |
| Database path | Text | No | Blank | Absolute path under allowlist | `/home/nyxmon/data/db.sqlite3` |
| Backup files | List (one path per line) | No | Empty list | Absolute paths under allowlist | `/home/nyxmon/uploads` |
| Schedule | Text | No | Blank | Valid cron expression | `0 2 * * *` |
| Retention days | Integer | No | `30` | Days to keep backups | `14` |
| Timeout seconds | Integer | No | `600` | Max time to wait for backup | `900` |
| Storage bucket | Text | No | `backups` | MinIO bucket name | `backups` |
| Created at | Timestamp | Read-only | Auto | Audit field | (auto) |
| Updated at | Timestamp | Read-only | Auto | Audit field | (auto) |

**Common Configurations**
SQLite app (DB + files):
```
Name: nyxmon
FastDeploy service: nyxmon
Database path: /home/nyxmon/data/db.sqlite3
Backup files:
/home/nyxmon/uploads
/home/nyxmon/media
Schedule: 0 2 * * *
```

Files-only backup:
```
Name: assets
FastDeploy service: assets
Backup files:
/opt/assets/shared
/var/lib/assets/uploads
Schedule: 30 1 * * *
```

Custom FastDeploy endpoint:
```
Name: nyxmon-staging
FastDeploy service: nyxmon-staging-backup
FastDeploy endpoint key: staging
Database path: /home/nyxmon/data/db.sqlite3
Schedule: 0 3 * * *
```

**FastDeploy Endpoint Configuration**
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

**Understanding Local vs Remote Backup Targets**

There are two types of backup targets:

1. **Local targets** (e.g., nyxmon, fastdeploy, echoport) - Services running ON macmini
   - Use the standard `echoport-backup` FastDeploy service
   - Backup script accesses files directly via filesystem
   - No SSH required

2. **Remote targets** (e.g., marina-staging) - Services running on OTHER hosts
   - Require a DEDICATED FastDeploy service (e.g., `marina-staging-backup`)
   - Backup script runs on macmini but uses SSH/SCP/rsync to access remote files
   - Remote host has NO backup tools - all logic runs on macmini

**CRITICAL**: For remote targets, you cannot just add a BackupTarget in Django Admin. You must FIRST register a FastDeploy service that knows how to SSH to the remote host. See "Adding a Remote Backup Target" below.

**Adding a Remote Backup Target (Complete Workflow)**

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
   ```

4. **Generate a service token** for the new FastDeploy service:
   - Via FastDeploy admin UI, or
   - Via API: `POST /api/service-token {"service": "<service>-backup"}`
   - **CRITICAL**: The token MUST be for the correct service name

5. **Add BackupTarget in Echoport Django Admin**:
   - Set `FastDeploy service` to the registered service name
   - Paste the service token into `Service token` field
   - Configure paths, schedule, etc.

6. **Test the backup** via Echoport UI

**Common Mistakes When Adding Remote Targets**

| Mistake | Symptom | Fix |
|---------|---------|-----|
| Using wrong service token | Wrong backup script runs (error message format differs) | Generate token for correct service name |
| Token pasted in wrong field | "Unknown endpoint key 'eyJhbG...'" | Paste in `Service token`, NOT `FastDeploy endpoint key` |
| Short hostname instead of FQDN | "Cannot resolve db_path" or SSH fails | Use full hostname (e.g., `staging.wersdoerfer.de` not `staging`) |
| mc alias not configured | "Insufficient permissions" on MinIO upload | Add mc alias setup to registration playbook |
| SSH user lacks access | "Cannot resolve db_path" | Use a user with SSH key access (e.g., deploy user) |
| Missing known_hosts entry | SSH fails with host key error | Add known_hosts setup to registration playbook |

**How to Identify Which Script is Running**

If backup fails, check the error message format to identify which script ran:

- `echoport-backup` (backup.py.j2): Messages say "Configuration loaded for <target-name>"
- Remote backup scripts: Messages say "Configuration validated" (no target name)

If you see the wrong format, your service token is for the wrong FastDeploy service.

**Troubleshooting**
- Error: `At least one of 'Database path' or 'Backup files' must be specified.`
Fix: Provide a `Database path`, `Backup files`, or both.
- Error: `Path must be absolute` or `Path must be under one of: ...`
Fix: Use absolute paths and ensure they live under `ECHOPORT_ALLOWED_PATH_PREFIXES`. If needed, adjust the allowlist in settings and redeploy.
- Error: `Invalid cron expression: ...`
Fix: Use a standard 5-field cron expression (e.g., `0 2 * * *`).
- Error: `Unknown endpoint key '...'`
Fix: Use a key configured in `FASTDEPLOY_ENDPOINTS`, or leave blank for the default endpoint. If the error shows a JWT token, you pasted the token in the wrong field.
- Error: `Endpoint '...' is incomplete: missing base_url/token`
Fix: Ensure the endpoint has `base_url` and either a `token` (default) or a `service_tokens` entry for your service.
- Error: `Cannot resolve db_path: ...`
Fix: SSH connection to remote host failed. Check: (1) Use FQDN not short hostname, (2) SSH user has key access, (3) known_hosts is configured.
- Error: `Failed to backup database: ...` with wrong script
Fix: Your service token is for the wrong FastDeploy service. Generate a new token for the correct service.
- Error: `Insufficient permissions` on MinIO upload
Fix: The user running the backup script needs mc configured. Add mc alias setup to the registration playbook.
- Can't delete a target
Fix: Deletion is blocked to preserve audit history. Set `Status = Disabled` instead.

**Reference: Existing Remote Backup Implementations**

For a complete example of a remote backup setup, see:
- Playbook: `ops-control/playbooks/register-marina-staging-backup.yml`
- Script: `ops-library/roles/echoport_backup/templates/marina_staging_backup.py.j2`
- Docs: `ops-control/docs/marina-staging-backup.md`
