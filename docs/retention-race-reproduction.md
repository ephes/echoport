# Baseline-only SQLite retention race reproduction

This is deterministic diagnostic evidence at Echoport
`51678035ef9c2b1299b4229220118ff7e7fbb7eb`, not a runtime fix or a policy change.
The helper verifies source hashes before importing the actual baseline modules.
It configures Django without project settings/.env, creates an owned temporary
file-backed SQLite database, and removes it after closing all actor connections.
Each scenario runs in a fresh subprocess with separate SQLite actor connections.

```sh
.venv/bin/pytest -q tests/test_retention_race_reproduction.py
.venv/bin/python tests/reproductions/retention_race/probe.py --pathway ui --ordering race
.venv/bin/python tests/reproductions/retention_race/probe.py --pathway direct --ordering race
```

Use already installed repository dependencies; no dependency download or live
settings, host, account, storage or database is required. UI requests use invented
staff properties and the real decorated endpoint; its on_commit worker-start
callback executes, but the worker itself is suppressed. Direct start_restore uses
the actual reservation logic; a fake config boundary waits after committed PENDING,
then intentionally refuses deployment, producing FAILED after the observation.
All storage is an in-memory invented object map. Network/deployment seams fail
if attempted, and the attempt count is asserted zero. No MinIO command runs.

The cleanup actor calls actual Command._delete_backup: this avoids touching the
global cleanup flock file, which excludes only other cleanup commands. Initial
eligibility is checked by the real get_backups_to_delete selector. An explicit
Event barrier at fake delete_object entry follows the actual final reference
check. Restore commits while deletion is paused; a separate observer sees PENDING
before allowing fake deletion. PROTECT then preserves metadata, but the object is
absent. Both UI and direct paths reproduce this dangling SUCCESS record.

Restore-first and cleanup-first controls prove the barriers distinguish order:
reference before final check skips deletion; cleanup before reservation removes
the row and prevents a new reference. Six cases run without sleeps or live I/O.
Each barrier/join and whole child process has a timeout; failures release barriers,
join actors and close connections. Summaries include only invented aggregate state,
not raw request bodies or credentials.

Tests deliberately assert **current unsafe baseline behavior**; they are not a
repair acceptance gate and must not be used to preserve the bug in future changes.
Future repair tests should invert the unsafe assertion against the actual archived
baseline and then test the repaired admission/deletion ordering. Do not weaken the
source hashes or replace production logic with a mutant to claim reproduction.
A corrected source checkout intentionally fails the baseline pin guard.

No migration, survivor-floor default, code serialization or crash-safe deletion
claim is introduced. The smallest enabled-cleanup correction remains centralized
UI/CLI fresh reservation plus durable deletion intent under a short SQLite writer
transaction; policy floor selection is separate. This reproduction does not test
other database backends, process crashes, actual object storage or repaired code.
