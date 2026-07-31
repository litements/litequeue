# litequeue

> Queue implemented on top of SQLite

## Why?

You can use this to implement a persistent queue. It also has extra timing
metrics for messages and tasks. The API lets you mark a current message claim
as **done**.

Since it's all based on SQLite / SQL, it is easily extendable.

Messages are always passed as strings, so you can use json data as messages.
Messages are interpreted as tasks, so after you `pop` a message, you need to
mark its current claim as done when you finish processing it. When you run the
`.prune()` method, it removes all finished tasks from the database.

Message IDs follow RFC 9562 UUIDv7. Optional idempotency keys make ambiguous
producer retries safe. Integer claim IDs prevent stale workers from changing
newer message claims.

## Installation

Install the package with uv:

```
uv add litequeue
```

Python 3.12 or newer and SQLite 3 are required.

## Quickstart

```python
from litequeue import LiteQueue

q = LiteQueue(filename="tasks.sqlite3")

q.put("hello", idempotency_key="greeting-1")
q.put("world")

# Message object used by LiteQueue
# Message(
#     data='hello',
#     message_id='063e95f1-3d9f-7547-8000-c3eb531fff93',
#     status=<MessageStatus.READY: 0>,
#     in_time=1676238611851409010,
#     lock_time=None,
#     done_time=None,
#     idempotency_key='greeting-1',
#     claim_id=0,
# )

task = q.pop()
assert task is not None

print(task)
# Message(
#     data='hello',
#     message_id='063e95f1-3d9f-7547-8000-c3eb531fff93',
#     status=<MessageStatus.LOCKED: 1>,
#     in_time=1676238611851409010,
#     lock_time=1676238623180543854,
#     done_time=None,
#     idempotency_key='greeting-1',
#     claim_id=1,
# )

updated = q.done(
    message_id=task.message_id,
    claim_id=task.claim_id,
)
assert updated

# Update methods return False for missing messages and stale claims.
assert not q.done(
    message_id="missing-message",
    claim_id=task.claim_id,
)

q.get(task.message_id)

# Message(
#     data='hello',
#     message_id='063e95f1-3d9f-7547-8000-c3eb531fff93',
#     status=<MessageStatus.DONE: 2>,
#     in_time=1676238611851409010,
#     lock_time=1676238623180543854,
#     done_time=1676238641276753673,
#     idempotency_key='greeting-1',
#     claim_id=1,
# )

```

## Idempotent submission

Pass an idempotency key when a producer must retry an ambiguous `put()`:

```python
message = q.put(
    "charge account A-123",
    idempotency_key="charge-2026-0001",
)
```

A replay with the same key and data returns the stored message. A replay with
different data raises `IdempotencyConflict`.

The key reservation exists until `prune()` deletes the message. See the
[idempotency guide](docs/idempotency.md) for concurrency, pruning, and
inbox/outbox examples.

## Message claims

Each `pop()` increments the selected message's integer `claim_id`. Pass that
claim ID to `done()`, `mark_failed()`, or `retry()`.

Each transition requires its permitted source state and the current claim ID.
This rule prevents a stale worker from changing a newer delivery.

Message timestamps use integer Unix nanoseconds. Public threshold parameters
use seconds and include `_seconds` in their names.

Use `retry_expired()` for atomic expired-claim recovery:

```python
retried_count = q.retry_expired(threshold_seconds=30)
```

See the [message claim guide](docs/message_claim_ids.md) for the state machine,
race examples, and migration procedure.

## Differences with a normal Python `queue.Queue`

- Persistence
- Optional idempotent message submission
- Claim-aware completion, failure, and retry operations
- Timing metrics. As long as tasks are still in the queue or not pruned, you can see how long they have been there or how long they took to finish.
- Easy to extend using SQL

## Queue size and capacity

`qsize()`, `empty()`, `full()`, and `maxsize` all use the ready backlog: messages
that are waiting to be claimed. A message stops contributing to that backlog as
soon as `pop()` locks it for a worker.

Use the explicit count methods when you need a different view:

- `ready_count()` counts messages waiting to be claimed.
- `locked_count()` counts messages currently claimed by workers.
- `done_count()` counts completed messages that have not been pruned.
- `failed_count()` counts failed messages that have not been pruned.
- `active_count()` counts ready plus locked messages.
- `stored_count()` counts every stored row regardless of status.

For example, active locked work does not make the ready backlog non-empty:

```python
queue = LiteQueue(filename="tasks.sqlite3", maxsize=1)
queue.put("resize image")
task = queue.pop()

assert task is not None
assert queue.qsize() == 0
assert queue.empty()
assert not queue.full()
assert queue.locked_count() == 1
assert queue.active_count() == 1
assert queue.stored_count() == 1
```

`maxsize` limits the number of ready messages and is stored as an immutable
property of the queue. Omit `maxsize` when reopening a queue to use its stored
limit, or pass the same value. Passing a conflicting value raises `ValueError`.
For a new queue, `None` means unlimited and `0` creates a queue that cannot
accept messages.

## Thread safety

A file-backed `LiteQueue` instance can be shared between threads. It uses one
connection for writes and explicit transactions, plus a fixed pool of ten
query-only connections for reads. A read checks out one connection for the
duration of the operation and always returns it afterward. Reads can continue
during a write transaction and see only committed data. A reentrant write lock
prevents concurrent consumers from claiming the same message or entering
another thread's transaction.

An explicit `queue.transaction()` excludes other writers until it commits or
rolls back. Reads in the transaction-owning thread use the write connection so
they can see their own uncommitted changes; pooled readers in other threads
continue and see the last committed state. `queue.close()` drains the read pool
and waits for active reads and writes to finish.

## SQLite connection options

Additional keyword arguments are forwarded to `sqlite3.connect()` for the
write connection and all ten read connections. Common options include
`timeout`, `detect_types`, `factory`, and `uri`:

```python
queue = LiteQueue(filename="tasks.sqlite3", timeout=30.0)
```

LiteQueue owns `database`, `isolation_level`, `check_same_thread`,
`cached_statements`, and `autocommit` because its persistence, transaction, and
threading guarantees depend on those settings. Passing one of these options
raises `ValueError`.

## Examples and benchmarks

The [`examples/producer.py`](examples/producer.py) and
[`examples/consumer.py`](examples/consumer.py) scripts show a producer and a
consumer that use the same persistent queue.

Run the producer from the repository root:

```sh
uv run examples/producer.py
```

Run the consumer in a second terminal:

```sh
uv run examples/consumer.py
```

The scripts share `tasks.sqlite3` in the current working directory. The
consumer tracks processing attempts separately from message claims. It retries
failed tasks and marks a task as failed after three attempts. The producer
supplies a stable idempotency key for each task number.

The `tests/` folder contains more usage scenarios.

The `benchmark.py` script contains benchmarks comparing `litequeue` to the
built-in Python `queue.Queue`. Run it with `make benchmark`.

## One queue per database

Each SQLite database file is one LiteQueue queue. Pass the exact file location
when you create the queue. LiteQueue does not add a suffix or derive the path
from a separate queue name. It stores messages in one fixed table named `Queue`.

```python
from pathlib import Path

import litequeue

queue_directory = Path("/var/lib/myapp/queues")
email_database = queue_directory / "email.sqlite3"
image_database = queue_directory / "images.sqlite3"

email_queue = litequeue.LiteQueue(filename=email_database)
image_queue = litequeue.LiteQueue(filename=image_database)

email_queue.put("send welcome email")
image_queue.put("resize profile photo")
```

The destination directory must exist. SQLite creates the database file when it
does not exist. Relative paths use the current working directory.

Databases containing custom queue tables, multiple queue tables, or unrelated
application tables are not supported. LiteQueue raises `ValueError` before
changing their schema. The old `queue_name`, `name`, and `folder` arguments are
no longer supported. Pass each queue's database file through `filename`.
LiteQueue does not automatically migrate shared or custom-table databases.

## Contributing

The only hard rules for the project are:

- No runtime dependencies allowed.
- Package code lives under `src/litequeue/`.
- Tests live under `tests/`.

## Development

Run `make install` to create the uv-managed environment and install the
development dependencies. Run the test suite with `make test`, static type
checks with `make typecheck`, and lint checks with `make lint`.

Publishing is intentionally local-only. Export `UV_PUBLISH_TOKEN`, then run
`make publish`. The target runs the tests, bumps the minor version, builds the
distributions, and uploads them with uv.

## Important changes

- In version 0.14:
  - Message timestamps use integer Unix nanoseconds again.
  - Public `threshold_seconds` values remain in seconds.
  - Migration 3 converts version 0.13 timestamps from seconds to nanoseconds.
- In version 0.13:
  - `put()` accepts an optional caller-provided `idempotency_key`.
  - Replays return the stored message, and conflicting data raises `IdempotencyConflict`.
  - `Message.claim_id` identifies one delivery of a message.
  - `done()`, `mark_failed()`, and `retry()` require the current claim ID.
  - `retry_expired()` atomically returns expired claims to `READY`.
  - Opening an older fixed `Queue` schema adds the new message fields.
  - Existing locked messages return to `READY` during that schema migration.
  - Message timestamps and duration comparisons use seconds.
  - Migration 2 converts old nanosecond timestamps to seconds.
  - `qsize()` now reports only the ready backlog instead of ready plus locked work.
  - `empty()`, `full()`, and `maxsize` use the same ready-backlog definition.
  - Explicit ready, locked, done, failed, active, and stored count methods are available.
- In version 0.10:
  - Each SQLite database can contain only one LiteQueue queue.
  - You must migrate version 0.9 queues before you use version 0.10 or later.
  - Follow the [single-queue migration guide](docs/migrate_single_queue.md).
  - Newly generated message IDs follow RFC 9562 UUIDv7.
  - Existing draft-format IDs remain supported without migration.
- In version 0.6:
  - The database schema has changed and the column `message` is now `data`.
  - Versions 0.6 through 0.12 stored timestamps as integer Unix nanoseconds.
  - Messages are represented as a frozen dataclass, not as a dictionary.
  - Message IDs are uuidv7 strings.
- In version 0.4 the database schema has changed and the column `task_id` is now `message_id`.

## Meta

Ricardo – [@ricardoanderegg](https://twitter.com/ricardoanderegg) –

- [ricardoanderegg.com](http://ricardoanderegg.com/)
- [github.com/polyrand](https://github.com/polyrand/)

Distributed under the MIT license. See `LICENSE` for more information.
