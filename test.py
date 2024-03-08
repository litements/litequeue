import sqlite3
import tempfile
import time
from pathlib import Path
from typing import Tuple

import pytest
from litequeue import LiteQueue
from litequeue import MessageStatus

print(sqlite3.sqlite_version)

# https://docs.pytest.org/en/7.1.x/how-to/fixtures.html#parametrizing-fixtures


@pytest.fixture(scope="function", params=["_pop_transaction", "_pop_returning"])
def q(request) -> LiteQueue:
    _q = LiteQueue(":memory:")

    if _q.get_sqlite_version() > 35:
        _q.pop = getattr(_q, request.param)

    return _q


@pytest.fixture(scope="function", params=["_pop_transaction", "_pop_returning"])
def multi_queue(request) -> Tuple[LiteQueue, LiteQueue]:
    """
    Create a tmp dir, create a DB in it with multiple queues.
    """

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test-queue.sqlite3"
        q1 = LiteQueue(db_path, queue_name="queue1")
        q2 = LiteQueue(db_path, queue_name="queue2")

        for _q in [q1, q2]:
            if _q.get_sqlite_version() > 35:
                _q.pop = getattr(_q, request.param[1])

        return q1, q2


@pytest.fixture(scope="function")
def qd(q) -> LiteQueue:
    q.put("hello")
    q.put("world")
    q.put("foo")
    q.put("bar")
    return q


@pytest.fixture(scope="function")
def queue_with_priorities(q) -> LiteQueue:
    q.put('firstIn', priority=1)
    q.put('secondIn', priority=3)
    q.put('thirdIn', priority=2)
    q.put('fourthIn', priority=0)
    return q


@pytest.mark.parametrize(
    "kwargs",
    (
        {"filename_or_conn": sqlite3.connect(":memory:")},
        {"filename_or_conn": ":memory:"},
        {"memory": True},
    ),
)
def test_isolation_level(kwargs):
    q = LiteQueue(**kwargs)
    assert (
        q.conn.isolation_level is None
    ), f"Isolation level not set properly for connection '{kwargs}'"


def test_insert_pop(q):
    first = q.put("hello")
    q.put("world")
    q.put("foo")
    q.put("bar")

    task = q.pop()
    assert task.data == "hello"
    assert str(task.message_id) == str(first.message_id)
    assert task.status == MessageStatus.LOCKED
    assert task.done_time is None


def test_get_unknow(q):
    assert q.get("nothing") is None


def test_pop_all_locked(qd):
    # Lock every message
    for _ in range(4):
        qd.pop()

    assert qd.pop() is None


def test_basic_actions(qd):
    q = qd

    task = q.pop()
    assert task.data == "hello"

    # Peek next READY message
    assert q.peek().data == "world"
    assert q.peek().status == MessageStatus.READY

    q.done(task.message_id)

    already_done = q.get(task.message_id)
    assert already_done.status == MessageStatus.DONE

    in_time = already_done.in_time
    lock_time = already_done.lock_time
    done_time = already_done.done_time

    assert done_time >= lock_time >= in_time
    print(
        f"Task {already_done.message_id} took {done_time - lock_time} seconds to get done and was in the queue for {done_time - in_time} seconds"
    )


def test_queue_size(qd):
    q = qd

    assert q.qsize() == 4
    task = q.pop()
    q.put("x")
    q.put("y")
    assert q.qsize() == 6
    q.done(task.message_id)
    assert q.qsize() == 5


def test_prune(qd):
    q = qd
    while not q.empty():
        t = q.pop()
        q.done(t.message_id)
    q.prune()

    assert (
        q.conn.execute(
            f"SELECT * FROM Queue WHERE status = {MessageStatus.DONE.value}"
        ).fetchall()
        == []
    )


def test_max_size():
    q = LiteQueue(":memory:", maxsize=50)
    for i in range(50):
        q.put(f"data_{i}")
    assert q.qsize() == 50

    with pytest.raises(sqlite3.IntegrityError):
        q.put("new")

    assert q.full()

    q.pop()

    assert not q.full()


def test_empty(qd):
    assert qd.empty() is False

    q2 = LiteQueue(":memory:")
    assert q2.empty() is True


def test_list_locked(q):
    q.put("foo")

    task = q.pop()

    time.sleep(0.2)

    assert len(list(q.list_locked(threshold_seconds=0.1))) == 1
    assert len(list(q.list_locked(threshold_seconds=20))) == 0

    q.done(task.message_id)

    assert len(list(q.list_locked(threshold_seconds=0.1))) == 0


def test_retry_failed(q):
    q.put("foo")

    task = q.pop()

    q.mark_failed(task.message_id)

    assert q.get(task.message_id).status == MessageStatus.FAILED

    q.retry(task.message_id)

    assert q.get(task.message_id).status == MessageStatus.READY
    assert q.get(task.message_id).done_time is None
    assert q.qsize() == 1


def test_priority_queue(queue_with_priorities):
    t1 = queue_with_priorities.pop()
    assert t1.data == "secondIn"
    t2 = queue_with_priorities.pop()
    assert t2.data == "thirdIn"
    t3 = queue_with_priorities.pop()
    assert t3.data == "firstIn"
    t4 = queue_with_priorities.pop()
    assert t4.data == "fourthIn"
