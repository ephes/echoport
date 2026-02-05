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
- `FastDeploy endpoint key` must match a key in `FASTDEPLOY_ENDPOINTS`. The endpoint must have `base_url` and either a `token` (default) or a matching entry in `service_tokens`. Leave blank to use the default FastDeploy endpoint.
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

Token lookup order:
1. If `service_tokens[fastdeploy_service]` exists, use that token
2. Otherwise, fall back to the `token` field
3. If neither exists, the backup will fail with an endpoint configuration error

**Troubleshooting**
- Error: `At least one of 'Database path' or 'Backup files' must be specified.`
Fix: Provide a `Database path`, `Backup files`, or both.
- Error: `Path must be absolute` or `Path must be under one of: ...`
Fix: Use absolute paths and ensure they live under `ECHOPORT_ALLOWED_PATH_PREFIXES`. If needed, adjust the allowlist in settings and redeploy.
- Error: `Invalid cron expression: ...`
Fix: Use a standard 5-field cron expression (e.g., `0 2 * * *`).
- Error: `Unknown endpoint key '...'`
Fix: Use a key configured in `FASTDEPLOY_ENDPOINTS`, or leave blank for the default endpoint.
- Error: `Endpoint '...' is incomplete: missing base_url/token`
Fix: Ensure the endpoint has `base_url` and either a `token` (default) or a `service_tokens` entry for your service.
- Can't delete a target
Fix: Deletion is blocked to preserve audit history. Set `Status = Disabled` instead.
