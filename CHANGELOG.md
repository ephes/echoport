# Changelog

## Unreleased

- Update the locked certifi CA bundle to 2026.10.19 for the eMudhra/emSign
  CA compromise
  ([GHSA-6q48-6c7w-9gw2](https://github.com/certifi/python-certifi/security/advisories/GHSA-6q48-6c7w-9gw2)).
  Lock-only change. Re-sync deployed environments and restart long-running
  processes so they load the new bundle.
- Retain at least the newest three completed recorded SUCCESS backups per target
  beyond age retention. Configure with `ECHOPORT_RETENTION_MIN_SUCCESS` (default
  `3`); failures and deletion-pending records do not displace retained successes.
- Commit durable deletion intent before removing storage, share atomic restore
  reservation across UI and CLI, and retry interrupted/ambiguous deletes without
  making the backup restorable again. Deletion-pending backups no longer count as
  the last successful backup in the dashboard or `/api/health`. Storage remains deleted before metadata;
  restore references still exclude cleanup. Adds migration
  `0011_backuprun_deletion_pending_at`; stop cleanup during mixed-version rollout
  and retain guarded restore admission until intents are reconciled on rollback.
