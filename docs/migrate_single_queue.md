# Migrate multiple queues to separate databases

This procedure migrates a shared LiteQueue database to the single-queue architecture.
It creates one SQLite database for each queue table.
It does not change the source database.

This migration is required before you use LiteQueue 0.10 or later with version 0.9 databases.
Complete this migration before you start an application that uses LiteQueue 0.10 or later.

The procedure preserves these message fields:

- `data`.
- `message_id`.
- `status`.
- `in_time`.
- `lock_time`.
- `done_time`.

The procedure also preserves the optional `maxsize` setting.
The new database uses the fixed table name `Queue`.

## Requirements

Use this procedure for a database created by LiteQueue 0.9.
The source queue tables must use the six columns listed above.

Install the `sqlite3` command-line tool before you start.
Make sufficient free disk space available for the backup and all destination databases.

Stop all applications that use the source database.
Keep the applications stopped until all verification steps are successful.

> **CAUTION:** Do not open the shared database with the new LiteQueue release.
> The new release rejects unsupported tables, but a separate migration gives better control.

## Example names

The examples use these names:

| Item                 | Example                                    |
| -------------------- | ------------------------------------------ |
| Source database      | `/srv/app/queues.sqlite3`                  |
| Backup database      | `/srv/app/queues.pre-single-queue.sqlite3` |
| Source queue table   | `EmailQueue`                               |
| New queue name       | `email`                                    |
| Destination folder   | `/srv/app/new-queues`                      |
| Destination database | `/srv/app/new-queues/email.queue.sqlite3`  |

Replace every example path and table name with your values.
Use one unique destination database for each source queue table.

## Message status values

The migration copies each status without a change:

| Value | Status |
| ----: | ------ |
|   `0` | Ready  |
|   `1` | Locked |
|   `2` | Done   |
|   `3` | Failed |

Locked messages stay locked after the migration.
Use the application recovery process if you must retry a locked message.

## Migration procedure

### 1. Record the queue mapping

Create a mapping from each source table to one new queue name.
The queue name controls the destination filename.

For example, the queue name `email` creates `email.queue.sqlite3`.
Do not put a directory path in the queue name.

### 2. Prepare the destination folder

Create the destination folder:

```sh
mkdir -p /srv/app/new-queues
```

Make sure that the first destination file does not exist:

```sh
test ! -e /srv/app/new-queues/email.queue.sqlite3
```

Do this check for each destination file.
LiteQueue can open only a database that contains one `Queue` table.

### 3. Checkpoint and back up the source database

Make sure that the backup path does not exist:

```sh
test ! -e /srv/app/queues.pre-single-queue.sqlite3
```

Run a WAL checkpoint and an integrity check.
Then create a SQLite backup:

```sh
sqlite3 /srv/app/queues.sqlite3 <<'SQL'
.timeout 5000
PRAGMA wal_checkpoint(TRUNCATE);
PRAGMA integrity_check;
.backup '/srv/app/queues.pre-single-queue.sqlite3'
SQL
```

Make sure that `PRAGMA integrity_check` returns `ok`.
Do not continue if SQLite reports an error.

Keep the backup until the new applications operate correctly.
Do not use a normal file copy when WAL files can contain committed data.

### 4. List the source tables

List all application tables:

```sh
sqlite3 /srv/app/queues.sqlite3 <<'SQL'
.headers on
.mode column
SELECT name
FROM sqlite_schema
WHERE type = 'table'
  AND name NOT GLOB 'sqlite_*'
ORDER BY name;
SQL
```

Identify each LiteQueue table in the result.
Move unrelated application tables to databases that LiteQueue does not open.
The new LiteQueue release does not permit unrelated tables in a queue database.

### 5. Verify each source schema

Examine one source queue table:

```sh
sqlite3 /srv/app/queues.sqlite3 <<'SQL'
.headers on
.mode column
PRAGMA table_info("EmailQueue");
SQL
```

Make sure that the table has these columns in this order:

1. `data TEXT NOT NULL`.
2. `message_id TEXT NOT NULL`.
3. `status INTEGER NOT NULL`.
4. `in_time INTEGER NOT NULL`.
5. `lock_time INTEGER`.
6. `done_time INTEGER`.

If the table has `message` or `task_id`, stop this procedure.
First migrate that table through the applicable older LiteQueue schema changes.

Do this schema check for each source queue table.

### 6. Record the queue capacity

Examine the trigger for each source queue table:

```sh
sqlite3 /srv/app/queues.sqlite3 <<'SQL'
.headers on
.mode column
SELECT name, sql
FROM sqlite_schema
WHERE type = 'trigger'
  AND tbl_name = 'EmailQueue';
SQL
```

No result means that the queue has unlimited capacity.
A `maxsize_control_EmailQueue` result contains `Max queue length reached: N`.
Record the integer `N` for use in step 9.

### 7. Check the source messages

Record the row count for each status:

```sh
sqlite3 /srv/app/queues.sqlite3 <<'SQL'
.headers on
.mode column
SELECT status, COUNT(*) AS message_count
FROM "EmailQueue"
GROUP BY status
ORDER BY status;

SELECT message_id, COUNT(*) AS duplicate_count
FROM "EmailQueue"
GROUP BY message_id
HAVING COUNT(*) > 1;
SQL
```

The second query must return no rows.
Repair duplicate identifiers before you continue.
The destination database requires a unique `message_id` value.

### 8. Copy one queue

Run this SQL once for each source queue table.
Change the source table and destination path for each run.

```sh
sqlite3 /srv/app/queues.sqlite3 <<'SQL'
.bail on
.timeout 5000
ATTACH DATABASE '/srv/app/new-queues/email.queue.sqlite3' AS target;
BEGIN IMMEDIATE;

CREATE TABLE target."Queue"
(
    data       TEXT NOT NULL,
    message_id TEXT NOT NULL,
    status     INTEGER NOT NULL,
    in_time    INTEGER NOT NULL,
    lock_time  INTEGER,
    done_time  INTEGER
);

INSERT INTO target."Queue"
    (data, message_id, status, in_time, lock_time, done_time)
SELECT
    data, message_id, status, in_time, lock_time, done_time
FROM main."EmailQueue";

CREATE UNIQUE INDEX target."Queue_message_id_unique_idx"
ON "Queue"(message_id);

CREATE INDEX target."Queue_status_message_id_idx"
ON "Queue"(status, message_id);

COMMIT;
DETACH DATABASE target;
SQL
```

The transaction prevents a partial message copy.
The unique index also detects duplicate message identifiers.

If the command fails, keep the source database unchanged.
Move the failed destination file aside before another attempt.

### 9. Restore a limited capacity

Skip this step for a queue with unlimited capacity.

Replace both `5` values with the recorded `maxsize` value.
Run this command for the applicable destination database:

```sh
sqlite3 /srv/app/new-queues/email.queue.sqlite3 <<'SQL'
.bail on
CREATE TRIGGER "maxsize_control_Queue"
BEFORE INSERT ON "Queue"
WHEN (
    SELECT COUNT(*)
    FROM "Queue"
    WHERE status = 0
) >= 5
BEGIN
    SELECT RAISE(ABORT, 'Max queue length reached: 5');
END;
SQL
```

The trigger name and message text must use this format.
LiteQueue reads the capacity from this trigger.

### 10. Verify one destination database

Compare the source and destination data:

```sh
sqlite3 /srv/app/queues.sqlite3 <<'SQL'
.bail on
.headers on
.mode column
ATTACH DATABASE '/srv/app/new-queues/email.queue.sqlite3' AS target;

PRAGMA target.integrity_check;

SELECT name
FROM target.sqlite_schema
WHERE type = 'table'
  AND name NOT GLOB 'sqlite_*'
ORDER BY name;

SELECT status, COUNT(*) AS message_count
FROM target."Queue"
GROUP BY status
ORDER BY status;

SELECT COUNT(*) AS missing_from_target
FROM (
    SELECT data, message_id, status, in_time, lock_time, done_time
    FROM main."EmailQueue"
    EXCEPT
    SELECT data, message_id, status, in_time, lock_time, done_time
    FROM target."Queue"
);

SELECT COUNT(*) AS missing_from_source
FROM (
    SELECT data, message_id, status, in_time, lock_time, done_time
    FROM target."Queue"
    EXCEPT
    SELECT data, message_id, status, in_time, lock_time, done_time
    FROM main."EmailQueue"
);

SELECT type, name, tbl_name
FROM target.sqlite_schema
WHERE type IN ('index', 'trigger')
ORDER BY type, name;

DETACH DATABASE target;
SQL
```

Make sure that the integrity check returns `ok`.
Make sure that the only application table is `Queue`.
Make sure that the two difference counts are `0`.
Compare the status counts with the counts from step 7.

The destination must contain these indexes:

- `Queue_message_id_unique_idx`.
- `Queue_status_message_id_idx`.

A limited queue must also contain `maxsize_control_Queue`.
An unlimited queue must not contain that trigger.

Do this verification for each destination database.

### 11. Change the application configuration

Replace the old shared-database construction:

```python
email_queue = LiteQueue(
    "/srv/app/queues.sqlite3",
    queue_name="EmailQueue",
)
```

Use the new queue name and destination folder:

```python
from pathlib import Path

from litequeue import LiteQueue


email_queue = LiteQueue(
    name="email",
    folder=Path("/srv/app/new-queues"),
)
```

Change each application queue in the same way.
Do not pass `queue_name` to the new LiteQueue release.

Omit `maxsize` when you open a migrated queue.
Alternatively, pass the exact capacity value that you restored in step 9.
A different value causes a `ValueError`.

### 12. Start and monitor the applications

Start the applications after all queues pass verification.
Confirm that each application opens its intended destination database.

Examine the queue sizes and the next ready messages.
Do not remove the source database or its backup during the monitoring period.

## Rollback

Stop the applications before a rollback.
Restore the previous application release and its shared-database configuration.
Then open the unchanged source database.

Do not merge new destination writes into the source database with this procedure.
Plan a separate data merge if the new applications wrote messages before the rollback.
