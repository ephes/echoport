# Changelog

## Unreleased

- Retention chooses the object store per target: `BackupTarget.storage_alias`
  (migration `0012_backuptarget_storage_alias`, default `MINIO_ALIAS`) names
  the mc alias `cleanup_old_backups` lists and deletes that target's runs
  through; it must be one of the new `MINIO_ALIASES` setting (default
  `minio,rustfs`), and empty pauses retention for the target. The alias is
  read from the locked target row and is part of the deletion intent (a
  change before finalization keeps the row). `backups.minio_client`'s
  `list_prefix`, `delete_object` and `delete_prefix` take the alias
  explicitly and refuse one outside the allowlist. New
  `manage.py set_storage_alias <target> <alias|paused>` is the only way to
  change it: it holds cleanup's lock and refuses while a deletion of the
  target is pending. "Object does not exist" on the target's store still
  counts as deleted. `just test-s3-backends` adds MinIO and RustFS side by
  side (`tests_integration/test_retention_per_store.py`).
- The object-store client works unchanged against RustFS as well as MinIO:
  new `just test-s3-backends` (`tests_integration/`, Docker) runs
  `backups.minio_client` against pinned MinIO and RustFS 1.0.3 containers with
  mc RELEASE.2025-08-13. No code change was needed; the README notes that the
  `MINIO_*` settings name an mc alias for any S3 store.
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
