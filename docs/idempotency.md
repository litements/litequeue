# Idempotent message submission

LiteQueue can assign one stored message to one caller idempotency key. This
feature makes an ambiguous `put()` retry safe.

An ambiguous retry occurs when the producer does not receive the first
response. The first `put()` can still commit before the connection fails.
Without a key, the next `put()` creates a second message.

## Key and message identity

The `message_id` and `idempotency_key` fields have different functions:

- `message_id` identifies one stored queue message.
- `idempotency_key` identifies one logical producer submission.
- LiteQueue generates each `message_id` as a UUIDv7 string.
- The producer supplies the optional `idempotency_key`.

The key is a case-sensitive string. LiteQueue treats all key characters as
opaque text.

One queue file defines the key scope. Two queue files can contain the same key.
The key is not an authentication credential. Control queue access separately.

## Submit a message

Supply the same key for each attempt of one logical submission:

```python
message = queue.put(
    '{"account": "A-123", "amount": 10}',
    idempotency_key="charge-2026-0001",
)
```

If the response is lost, submit the same data and key again:

```python
same_message = queue.put(
    '{"account": "A-123", "amount": 10}',
    idempotency_key="charge-2026-0001",
)

assert same_message.message_id == message.message_id
```

LiteQueue returns the stored message with its current state. The returned state
can be `READY`, `LOCKED`, `DONE`, or `FAILED`.

Do not create a new key for a retry of the same logical submission. A new key
requests a new queue message.

## Replay rules

`put()` applies these rules:

| Key condition | Data condition | Result                      |
| ------------- | -------------- | --------------------------- |
| No key        | All data       | Create a new message        |
| New key       | All data       | Create a new message        |
| Existing key  | Same data      | Return the stored message   |
| Existing key  | Different data | Raise `IdempotencyConflict` |

Data comparison is exact. A space, letter case, or JSON format change can cause
a conflict.

The conflict prevents one key from identifying two operations:

```python
from litequeue import IdempotencyConflict


queue.put("amount=10", idempotency_key="charge-2026-0001")

try:
    queue.put("amount=1000", idempotency_key="charge-2026-0001")
except IdempotencyConflict as error:
    print(error.idempotency_key)
```

## Concurrent producers

LiteQueue creates an immediate SQLite transaction for a keyed `put()` when no
explicit transaction is active. Other writers cannot change the key result
during this transaction.

A keyed `put()` joins `queue.transaction()` when the calling thread owns it.
The caller selects the transaction mode and controls its commit or rollback.

Use `queue.transaction(mode="IMMEDIATE")` for concurrent keyed submissions.
The default `DEFERRED` mode can raise `sqlite3.OperationalError` during a
write-lock upgrade. Roll back the outer transaction and retry the same data
and key after this error.

Concurrent producers can submit the same key outside an explicit transaction.
One producer creates the message, and the other producers receive that
message. The same rule applies inside explicit `IMMEDIATE` transactions.

A unique partial index also protects each non-null key. Messages without a key
retain the standard queue behavior.

An idempotent replay does not use more queue capacity. Thus, a replay can
succeed when the ready queue is full.

## Producer retry workflow

Use this procedure for an operation that can have an ambiguous response:

1. Create one stable key for the logical operation.
2. Store the key until the operation has a clear result.
3. Call `put()` with the operation data and key.
4. If the result is ambiguous, call `put()` again with the same data and key.
5. Use the returned `message_id` for later message operations.

Do not change the data between attempts. If the logical operation changes,
create a new key.

## Inbox and outbox workflow

An inbox producer can use its request identifier as the inbox key:

```text
request:2026-0001
```

An actor can derive an output key from that stable input identity:

```text
result:request:2026-0001
```

If the actor runs again, the outbox `put()` returns the original output
message. The queue does not store a duplicate output.

If the actor creates different output data, `put()` raises
`IdempotencyConflict`.

If the inbox has no caller key, use the inbox `message_id`:

```text
result:<inbox-message-id>
```

This pattern can support two durable queues without a transaction across their
database files. Keep the output message until the inbox claim and its retry
window are complete.

If output pruning can occur sooner, keep another durable receipt for the
output key. Otherwise, a later inbox retry can create a duplicate output.

LiteQueue permits only one queue in each database file. It does not provide
one transaction across separate inbox and outbox queue files.

Use another transactional design when atomic inbox and outbox changes are
required.

## Prune behavior

The key reservation exists while its message exists. `prune()` deletes the key
with each deleted message.

After deletion, a producer can use the key for new data:

```python
original = queue.put(
    "old operation",
    idempotency_key="charge-2026-0001",
)
claim = queue.pop()
assert claim is not None
assert claim.message_id == original.message_id
assert queue.done(
    message_id=claim.message_id,
    claim_id=claim.claim_id,
)

queue.prune()
replacement = queue.put(
    "new operation",
    idempotency_key="charge-2026-0001",
)
```

If `prune(include_failed=False)` keeps a failed message, its key remains
reserved. A replay returns that failed message.

LiteQueue does not keep idempotency history after deletion. Applications that
need permanent history must store receipts outside LiteQueue.

## Limits

Idempotent submission controls queue-message creation. It does not make an
external service idempotent.

An outbox consumer can complete an external action and then lose its
acknowledgment. The output message can run again after recovery.

Use the output key as the external idempotency key when the destination
supports that feature. Otherwise, the destination can receive the action more
than once.

Idempotency also does not identify the current worker claim. Use the message
claim ID for `done()`, `mark_failed()`, and `retry()`.

See [Message claim IDs](message_claim_ids.md) for claim and recovery rules.
