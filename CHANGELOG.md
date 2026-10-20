# Changelog

## Unreleased

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
