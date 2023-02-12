import pytest
from litequeue import LiteQueue, MessageStatus, Message
import time
import sqlite3


print(sqlite3.sqlite_version)




@pytest.fixture(scope="function")
def q() -> LiteQueue:
    _q = LiteQueue(":memory:")
    return _q


@pytest.fixture(scope="function")
def qd(q) -> LiteQueue:
    q.put("hello")
    q.put("world")
    q.put("foo")
    q.put("bar")
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
            f"SELECT * FROM Queue WHERE status = {MessageStatus.DONE}"
        ).fetchall()
        == []
    )


def test_max_size():
    q = LiteQueue(":memory:", maxsize=50)
    for i in range(50):
        q.put(random_string(20))
    assert q.qsize() == 50

    with pytest.raises(sqlite3.IntegrityError):
        q.put(random_string(20))

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


