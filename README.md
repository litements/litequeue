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

## Installation

Create a virtual environment if you are alredy not inside one and install the
package using pip:

```
python3 -m venv .venv
python3 -m pip --require-virtualenv install --upgrade litequeue
```

## Quickstart

```python
from litequeue import LiteQueue

q = LiteQueue(":memory:")

q.put("hello")
q.put("world")

# Message object used by LiteQueue
# Message(data='world', message_id=UUID('063e95f1-3d9f-7547-8000-c3eb531fff93'), status=<MessageStatus.READY: 0>, in_time=1676238611851409010, lock_time=None, done_time=None)

task = q.pop()

print(task)
# Message(data='hello', message_id='063e95f1-3d9e-7bbc-8000-a6a18a5f65d1', status=1, in_time=1676238611851279408, lock_time=1676238623180543854, done_time=None)

q.done(task.message_id)

q.get(task.message_id)

# Message(
#     data='hello',
#     message_id='063e95f1-3d9e-7bbc-8000-a6a18a5f65d1',
#     status=2,                <---- status is now 2 (DONE)
#     in_time=1676238611851279408,
#     lock_time=1676238623180543854,
#     done_time=1676238641276753673
# )

```

Check out [the docs page](https://litements.polyrand.net/queue/) for more.

## Differences with a normal Python `queue.Queue`

- Persistence
- Different API to set tasks as done (you tell it which `message_id` to set as done)
- Timing metrics. As long as tasks are still in the queue or not pruned, you can see how long they have been there or how long they took to finish.
- Easy to extend using SQL

## Examples and bechmarks and bechmarks

You can have a look at the `test.py` file. The tests are short and showcase
different usage scenarios.

The `benchmark.ipynb` file contains some benchmarks comparing `litequeue` to
the built-in Python `queue.Queue`.

## Multiple queues in the same DB file

In the `LiteQueue` class, the `filename_or_conn` parameter defines the SQLite
file that will be used to store the messages, the `queue_name` parameter is used
to define the table name that will be used to store the messages.

Multiple queues in the same SQLite is supported, but it's neither tested nor
recommended. But if you need it, you can use different `queue_name` values when
initializing the `LiteQueue` object to store multiple queues in the same DB
file.

```python
import tempfile
import litequeue


with tempfile.TemporaryDirectory() as tmpdirname:

    db_path = tmpdirname + "/test.sqlite3"

    q1 = litequeue.LiteQueue(db_path, queue_name="q1")
    q2 = litequeue.LiteQueue(db_path, queue_name="q2")

    q1.put("a")
    q1.put("b")

    print("Q1 size", q1.qsize())

    print("Q2 size", q2.qsize())

    q2.put("c")
    q2.put("d")

    print("Q2 size", q2.qsize())

    print(q1.pop())
    print(q1.peek())

    print(q2.peek())
    print(q2.pop())

```

## Meta

Ricardo Ander-Egg Aguilar – [@ricardoanderegg](https://twitter.com/ricardoanderegg) –

- [ricardoanderegg.com](http://ricardoanderegg.com/)
- [github.com/polyrand](https://github.com/polyrand/)

Distributed under the MIT license. See `LICENSE` for more information.

## Important changes

- In version 0.6:
  - The database schema has changed and the column `message` is now `data`.
  - Time is still measured as an integer, but now it's using nanoseconds.
  - Messages are represented as a frozen dataclass, not as a dictionary.
  - Message IDs are uuidv7 strings.
- In version 0.4 the database schema has changed and the column `task_id` is now `message_id`.

## Contributing

The only hard rules for the project are:

- No extra dependencies allowed
- No extra files, everything must be inside `litequeue.py` file.
- Tests must be inside the `test.py` file.
- Files must be formatted using `black` and `isort`, using one import per line.
