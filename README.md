# litequeue

> Queue implemented on top of SQLite

## Why?

You can use this to implement a persistent queue. It also has extra timing
metrics for the messages/tasks, and the api to set a message as **done** lets
you specifiy the `message_id` to be set as done.

Since it's all based on SQLite / SQL, it is easily extendable.

Messages are always passed as strings, so you can use json data as messages.
Messages are interpreted as tasks, so after you `pop` a message, you need to
mark it as done when you finish processing it. When you run the `.prune()`
method, it will remove all the finished tasks from the database.

Message IDs follow RFC 9562 UUIDv7: their first 48 bits contain the Unix
timestamp in milliseconds. Queues created by older LiteQueue versions may
contain IDs from an earlier UUIDv7 draft. Those IDs remain valid and can coexist
with RFC UUIDv7 IDs, but the queue sorts lexically by `message_id`. New RFC
UUIDv7 values can therefore sort before older draft-format IDs.

## Installation

Install the package with uv:

```
uv add litequeue
```

Python 3.12 or newer is required.

## Quickstart

```python
import sqlite3

from litequeue import LiteQueue

connection = sqlite3.connect(":memory:")
q = LiteQueue(conn=connection)

q.put("hello")
q.put("world")

# Message object used by LiteQueue
# Message(data='world', message_id=UUID('063e95f1-3d9f-7547-8000-c3eb531fff93'), status=<MessageStatus.READY: 0>, in_time=1676238611851409010, lock_time=None, done_time=None)

task = q.pop()

print(task)
# Message(
#     data='hello',
#     message_id='063e95f1-3d9e-7bbc-8000-a6a18a5f65d1',
#     status=1,
#     in_time=1676238611851279408,
#     lock_time=1676238623180543854,
#     done_time=None
# )

q.done(task.message_id)

q.get(task.message_id)

# Message(
#     data='hello',
#     message_id='063e95f1-3d9e-7bbc-8000-a6a18a5f65d1',
#     status=2,                <---- status is now 2 (DONE)
#     in_time=1676238611851279408,
#     lock_time=1676238623180543854,
#     done_time=1676238641276753673  <---- done_time contains timestamp now
# )

```

Check out [the docs page](https://litements.polyrand.net/queue/) for more.

## Differences with a normal Python `queue.Queue`

- Persistence
- Different API to set tasks as done (you tell it which `message_id` to set as done)
- Timing metrics. As long as tasks are still in the queue or not pruned, you can see how long they have been there or how long they took to finish.
- Easy to extend using SQL

## Queue capacity

`maxsize` limits the number of ready messages and is stored as an immutable
property of the queue. Omit `maxsize` when reopening a queue to use its stored
limit, or pass the same value. Passing a conflicting value raises `ValueError`.
For a new queue, `None` means unlimited and `0` creates a queue that cannot
accept messages.

## Examples and benchmarks

You can have a look at the `tests/` folder. The tests are short and showcase
different usage scenarios.

The `benchmark.py` script contains benchmarks comparing `litequeue` to the
built-in Python `queue.Queue`. Run it with `make benchmark`.

## Development

Run `make install` to create the uv-managed environment and install the
development dependencies. Run the test suite with `make test`.

Publishing is intentionally local-only. Export `UV_PUBLISH_TOKEN`, then run
`make publish`. The target runs the tests, bumps the minor version, builds the
distributions, and uploads them with uv.

## One queue per database

Each LiteQueue queue uses its own SQLite database. Pass `name="email"` to create
`email.queue.sqlite3`. LiteQueue stores messages in one fixed table named
`Queue`.

```python
from pathlib import Path

import litequeue

queue_folder = Path("/var/lib/myapp/queues")
email_queue = litequeue.LiteQueue(name="email", folder=queue_folder)
image_queue = litequeue.LiteQueue(name="images")

email_queue.put("send welcome email")
image_queue.put("resize profile photo")
```

`folder` must be an existing `Path`. When omitted, LiteQueue uses `Path.cwd()`.

Databases containing custom queue tables, multiple queue tables, or unrelated
application tables are not supported. LiteQueue raises `ValueError` before
changing their schema. The old `queue_name` argument is no longer supported;
create a separate `.queue.sqlite3` database for each queue instead. LiteQueue
does not automatically migrate shared or custom-table databases.

## Meta

Ricardo Ander-Egg Aguilar – [@ricardoanderegg](https://twitter.com/ricardoanderegg) –

- [ricardoanderegg.com](http://ricardoanderegg.com/)
- [github.com/polyrand](https://github.com/polyrand/)

Distributed under the MIT license. See `LICENSE` for more information.

## Important changes

- In version 0.10:
  - Newly generated message IDs follow RFC 9562 UUIDv7.
  - Existing draft-format IDs remain supported without migration.
- In version 0.6:
  - The database schema has changed and the column `message` is now `data`.
  - Time is still measured as an integer, but now it's using nanoseconds.
  - Messages are represented as a frozen dataclass, not as a dictionary.
  - Message IDs are uuidv7 strings.
- In version 0.4 the database schema has changed and the column `task_id` is now `message_id`.

## Contributing

The only hard rules for the project are:

- No runtime dependencies allowed.
- Package code lives under `src/litequeue/`.
- Tests live under `tests/`.
