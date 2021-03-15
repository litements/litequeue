# litequeue

> Queue implemented on top of SQLite

## Why?

You can use this to implement a persistent queue. It also has extra timing metrics for the messages/tasks, and the api to set a message as **done** lets you specifiy the `task_id` to be set as done.

Since it's all based on SQLite / SQL, it is easily extendable.

Messages are always passed as strings, so you can use json data as messages. Messages are interpreted as tasks, so after you `pop` a message, you need to mark it as done when you finish processing it. When you run the `.prune()` method, it will remove all the finished tasks from the database.

## Installation

```
pip install litequeue
```

## Differences with a normal Python `queue.Queue`

* Persistence
* Different API to set tasks as done (you tell it which `message_id` to set as done)
* Timing metrics. As long as tasks are still in the queue or not pruned, you can see how long they have been there or how long they took to finish.
* Easy to extend using SQL

## Quickstart


```python
from litequeue import SQLQueue

q = SQLQueue(":memory:")

q.put("hello")
q.put("world")

# 2  <- ID of the last row modified

task = q.pop()

print(task)
# {'message': 'hello', 'message_id': '7da620ac542acd76c806dbcf00218426'}

q.done(task["message_id"])

q.get(task["message_id"])

#    {'message': 'hello',
#     'message_id': 'c9b9ef76e3a77cc66dd749d485613ec1',
#     'status': 2,               <---- status is now 2 (DONE)
#     'in_time': 1612711138,
#     'lock_time': 1612711138,
#     'done_time': 1612711138}
```

Check out [the docs page](https://litements.exampl.io/queue/) for more.

## Examples

The examples are mostly taken from the tests in [`tests.ipynb`](./tests.ipynb)


```python
from litequeue import SQLQueue

q = SQLQueue(":memory:")

q.put("hello")
q.put("world")
q.put("foo")
q.put("bar")
# 4  <- ID of the last row modified

q.pop()
# {'message': 'hello', 'message_id': '7da620ac542acd76c806dbcf00218426'}

print(q)


#    SQLQueue(dbname=':memory:', items=[{'done_time': None,
#      'in_time': 1612711137,
#      'lock_time': 1612711137,
#      'message': 'hello',
#      'status': 1,
#      'message_id': '7da620ac542acd76c806dbcf00218426'},
#       ...

# pop remaining
for _ in range(3):
    q.pop()


assert q.pop() is None

q.put("hello")
q.put("world")
q.put("foo")
q.put("bar")

# 8 <- ID of the last row modified

task = q.pop()

assert task["message"] == "hello"

# peek at next message
q.peek()

#    {'message': 'world',
#     'message_id': '44cbc85f12b62891aa596b91f14183e5',
#     'status': 0,
#     'in_time': 1612711138,
#     'lock_time': None,
#     'done_time': None}

# -> back to our previous task <-

task["message"], task["message_id"]

# ('hello', 'c9b9ef76e3a77cc66dd749d485613ec1')   

q.done(task["message_id"])

# 8 <- ID of the last row modified

q.get(task["message_id"])

#    {'message': 'hello',
#     'message_id': 'c9b9ef76e3a77cc66dd749d485613ec1',
#     'status': 2,   <---- status is now 2 (DONE)
#     'in_time': 1612711138,
#     'lock_time': 1612711138,
#     'done_time': 1612711138}


already_done = q.get(task["message_id"])

# stauts = 2 = done
assert already_done["status"] == 2

in_time = already_done["in_time"]
lock_time = already_done["lock_time"]
done_time = already_done["done_time"]

assert done_time >= lock_time >= in_time
print(
    f"Task {already_done['message_id']} took {done_time - lock_time} seconds to get done and was in the queue for {done_time - in_time} seconds"
)

# Task c9b9ef76e3a77cc66dd749d485613ec1 took 0 seconds to get done and was in the queue for 0 seconds

# the queue size ignores the finished items

assert q.qsize() == 7

next_one_msg = q.peek()["message"]
next_one_id = q.peek()["message_id"]

task = q.pop()

assert task["message"] == next_one_msg
assert task["message_id"] == next_one_id

# remove finished items
q.prune()

print(q)


#    SQLQueue(dbname=':memory:', items=[{'done_time': None,
#      'in_time': 1612711137,
#      'lock_time': 1612711137,
#      'message': 'hello',
#      'status': 1,
#      'message_id': '7da620ac542acd76c806dbcf00218426'},
#     {'done_time': None,
#      'in_time': 1612711137,
#      'lock_time': 1612711137,
#      'message': 'world',
#      'status': 1,
#      'message_id': 'a593292cfc8d2f3949eab857eafaf608'},
#     {'done_time': None,
#      'in_time': 1612711137,
#      'lock_time': 1612711137,
#      'message': 'foo',
#      'status': 1,
#      'message_id': '17e843a29770df8438ad72bbcf059bf5'},
#     ...

from string import ascii_lowercase, printable
from random import choice


def random_string(string_length=10, fuzz=False, space=False):
    """Generate a random string of fixed length """
    letters = ascii_lowercase
    letters = letters + " " if space else letters
    if fuzz:
        letters = printable
    return "".join(choice(letters) for i in range(string_length))

q = SQLQueue(":memory:", maxsize=50)

for i in range(50):

    q.put(random_string(20))

assert q.qsize() == 50
```

An error is raised when the queue has reached its size limit


```python
import sqlite3

try:
    q.put(random_string(20))
except sqlite3.IntegrityError:  # max len reached
    # make sure the `.full()` method returns True
    assert q.full() == True
    print("test pass")

# test pass

# if we pop an item, we get place for another one

q.pop()

#    {'message': 'aktabyjadzrsohlitnei',
#     'message_id': '08b201c31099a296ef37f23b5257e5b6'}

assert q.full() == False

q.put("hello")

# 51


# Check if a queue is empty
assert q.empty() == False

q2 = SQLQueue(":memory:")

assert q2.empty() == True
```

**Benchmarks**

Inserting items in the queue.


```python
import gc
```

In-memory SQL queue


```python
q = SQLQueue(":memory:", maxsize=None)

gc.collect()

# %%timeit -n10000 -r7

q.put(random_string(20))

# 40.2 µs ± 12 µs per loop (mean ± std. dev. of 7 runs, 10000 loops each)

q.qsize()

# 70000
```


Standard python queue.


```python
from queue import Queue

q = Queue()

gc.collect()

# %%timeit -n10000 -r7

q.put(random_string(20))

# 21.9 µs ± 3.57 µs per loop (mean ± std. dev. of 7 runs, 10000 loops each)
```

Persistent SQL queue


```python
q = SQLQueue("test.queue", maxsize=None)

gc.collect()

# %%timeit -n10000 -r7

q.put(random_string(20))

# 161 µs ± 5.36 µs per loop (mean ± std. dev. of 7 runs, 10000 loops each)

assert q.conn.isolation_level is None
```

Creating, popping and setting messages as done.


```python
q = Queue()

gc.collect()

# %%timeit -n10000 -r7

tid = random_string(20)

q.put(tid)

q.get()

q.task_done()

# 27 µs ± 3.69 µs per loop (mean ± std. dev. of 7 runs, 10000 loops each)

q = SQLQueue(":memory:", maxsize=None)

gc.collect()

# %%timeit -n10000 -r7

tid = random_string(20)

q.put(tid)

task = q.pop()

q.done(task["message_id"])

# 80.2 µs ± 4.02 µs per loop (mean ± std. dev. of 7 runs, 10000 loops each)
```

    
## Meta


Ricardo Ander-Egg Aguilar – [@ricardoanderegg](https://twitter.com/ricardoanderegg) –

- [ricardoanderegg.com](http://ricardoanderegg.com/)
- [github.com/polyrand](https://github.com/polyrand/)
- [linkedin.com/in/ricardoanderegg](http://linkedin.com/in/ricardoanderegg)

Distributed under the MIT license. See ``LICENSE`` for more information.

## Chagelog notices

* In version 0.4 the database schema has changed and the column `task_id` is now `message_id`.

## Contributing

The only hard rules for the project are:

* No extra dependencies allowed
* No extra files, everything must be inside the main module's `.py` file.
* Tests must be inside the `tests.ipynb` notebook.