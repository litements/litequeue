# Message claim IDs

Each `pop()` assigns a new integer claim ID to the selected message. The claim
ID identifies one delivery of that message.

The `message_id` stays constant for the stored message. The `claim_id`
increases after each return to `READY` and later delivery.

Together, these values identify one worker claim:

```text
(message_id, claim_id)
```

The claim ID is local to one message. Different messages can have the same
claim ID.

The claim ID is not an authentication credential. Control queue access
separately.

## Why claim IDs are necessary

A queue can retry work after a worker stops. The original worker can still
finish after the retry.

Without a claim ID, this sequence can complete the wrong delivery:

1. Worker A claims message M.
2. Recovery returns message M to `READY`.
3. Worker B claims message M.
4. Worker A calls `done()` for message M.
5. The queue completes worker B's active claim.

A status check does not prevent this race. Worker B changes the status to
`LOCKED` before worker A calls `done()`.

The claim ID changes when worker B calls `pop()`. Worker A has the old claim
ID, so its transition changes no row.

## Claim ID values

A new message has claim ID `0`. This value means that no worker has claimed
the message.

The first `pop()` changes the claim ID to `1`. Each later `pop()` adds `1` in
the same SQLite update that locks the message.

SQLite controls this increment. The claim order does not depend on a clock,
random value, or worker process.

For messages created with the new schema, the integer is also the delivery
count. A 64-bit SQLite integer stores the value.

## Complete a claim

Pass the two identifiers from the message returned by `pop()`:

```python
message = queue.pop()

if message is not None:
    updated = queue.done(
        message_id=message.message_id,
        claim_id=message.claim_id,
    )
```

`done()` returns `True` only when these conditions are true:

- The message exists.
- The message status is `LOCKED`.
- The stored claim ID equals the supplied claim ID.

`done()` returns `False` when one condition is false. A false result means that
the worker no longer owns the claim.

Do not retry the transition with a different claim ID. Discard the stale
result and stop work for that claim.

## Fail or retry a claim

Use the same identifiers with `mark_failed()`:

```python
updated = queue.mark_failed(
    message_id=message.message_id,
    claim_id=message.claim_id,
)
```

The message must have the `LOCKED` status. A stale claim cannot fail a current
claim.

Use `retry()` to return locked or failed work to `READY`:

```python
updated = queue.retry(
    message_id=message.message_id,
    claim_id=message.claim_id,
)
```

`retry()` clears `lock_time` and `done_time`. The next `pop()` assigns the next
claim ID.

## State transitions

LiteQueue permits these claim transitions:

| Operation         | Source state         | Destination state | Claim rule              |
| ----------------- | -------------------- | ----------------- | ----------------------- |
| `pop()`           | `READY`              | `LOCKED`          | Add `1` to the claim ID |
| `done()`          | `LOCKED`             | `DONE`            | Match the claim ID      |
| `mark_failed()`   | `LOCKED`             | `FAILED`          | Match the claim ID      |
| `retry()`         | `LOCKED` or `FAILED` | `READY`           | Match the claim ID      |
| `retry_expired()` | Expired `LOCKED`     | `READY`           | Use one atomic update   |

An invalid transition returns `False` and does not change the message.
`retry_expired()` returns the number of changed messages.

## Recover expired claims

Use `retry_expired()` to recover claims that are older than a threshold:

```python
retried_count = queue.retry_expired(threshold_seconds=30)
```

Supply the threshold in seconds. LiteQueue converts the threshold to integer
nanoseconds before it compares `lock_time` values.

The method updates all current expired claims in one SQL statement. It clears
the obsolete `lock_time` and `done_time` values.

A claim expires only when its age is greater than the threshold. A claim at the
exact threshold does not expire.

The atomic update prevents a check-then-update race. It retains a claim when
its current `lock_time` is not older than the computed cutoff.

`list_locked()` returns a snapshot for inspection. A message can change after
the method returns.

If you use that snapshot, pass each returned claim ID to `retry()`. The claim
check protects a newer delivery.

Prefer `retry_expired()` for automatic recovery. It does not create a stale
snapshot.

## Completion and recovery race

Completion and recovery can start at the same time. SQLite orders the two
updates.

If completion wins, recovery finds a `DONE` message and changes no row. If
recovery wins, completion finds a `READY` message and returns `False`.

The two operations cannot change the same claim successfully.

## Long-running workers

A worker can continue after its claim becomes expired. Recovery can give the
message to another worker.

The old worker must use its original claim ID. Its late queue transition then
returns `False`.

Claim IDs protect queue state. They do not cancel worker code or undo an
external action.

Use an adequate recovery threshold for long tasks. Use destination
idempotency when a task can cause an external action.

## Schema migration

The new schema adds these fields:

```sql
"idempotency_key" TEXT
"claim_id" INTEGER NOT NULL DEFAULT 0
```

Version 0.13 migration 2 converts `in_time`, `lock_time`, and `done_time` from
nanoseconds to seconds. Migration 3 converts these values back to integer Unix
nanoseconds. New message timestamps also use integer Unix nanoseconds.

For a database from version 0.12 or earlier, this round trip can change the
low-order nanoseconds. Migration 2 stores the timestamps as floating-point
seconds, which cannot represent every integer nanosecond exactly.

LiteQueue adds the fields when it opens an older fixed `Queue` table. Existing
messages receive a null key. Most messages receive claim ID `0`.

Old locked messages have no valid claim ID. The migration returns these
messages to `READY` and clears their obsolete timestamps.

Old failed messages receive claim ID `1`. This value lets the new `retry()`
API return a retained failed message to `READY`.

Migrated claim IDs do not preserve the number of deliveries before migration.

Use this upgrade procedure:

1. Stop all workers and producers that use the queue file.
2. Back up the SQLite database and its committed WAL data.
3. Install the LiteQueue release that supports claim IDs.
4. Open the queue with the new release.
5. Start only workers that pass claim IDs to state transitions.

Do not run old and new workers on the same queue file. An old worker does not
send a claim ID and can bypass the new protocol.

## Invalid claim IDs

Claim-owned methods accept integers from `1` through
`9223372036854775807`. They reject Boolean values and other types.

The methods raise `TypeError` for an invalid type. They raise `ValueError` for
an integer outside the permitted range.

Use only the claim ID that LiteQueue returns. Do not calculate or change this
value in application code.

## Related idempotency rules

A claim ID changes for each delivery. Thus, it cannot identify one logical
producer submission.

An idempotency key stays stable across producer retries. Thus, it cannot
identify the worker that owns the current delivery.

Use each field for its specified function. See
[Idempotent message submission](idempotency.md) for producer retry rules.
