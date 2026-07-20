import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

import litequeue
from litequeue import LiteQueue
from litequeue import MessageStatus

print(sqlite3.sqlite_version)

# https://docs.pytest.org/en/7.1.x/how-to/fixtures.html#parametrizing-fixtures


def test_uuid7_matches_rfc_9562_test_vector(monkeypatch) -> None:
    timestamp_ns = 1_645_557_742_000_000_000
    counter = (0xCC3 << 30) | 0x18C4DC0C
    tail = 0x0C07398F
    monkeypatch.setattr(litequeue.time, "time_ns", lambda: timestamp_ns)
    monkeypatch.setattr(
        litequeue,
        "_uuid7_get_counter_and_tail",
        lambda: (counter, tail),
    )
    monkeypatch.setattr(litequeue, "_last_timestamp_v7", None)
    monkeypatch.setattr(litequeue, "_last_counter_v7", 0)

    message_id = litequeue.uuid7()

    assert str(message_id) == "017f22e2-79b0-7cc3-98c4-dc0c0c07398f"


def test_uuid7_uses_unix_milliseconds_and_rfc_bits(monkeypatch) -> None:
    timestamp_ms = 1_750_000_123_456
    monkeypatch.setattr(
        litequeue.time,
        "time_ns",
        lambda: timestamp_ms * 1_000_000,
    )
    monkeypatch.setattr(litequeue.os, "urandom", lambda size: bytes(size))
    monkeypatch.setattr(litequeue, "_last_timestamp_v7", None)
    monkeypatch.setattr(litequeue, "_last_counter_v7", 0)

    value = litequeue.uuid7()
    encoded_timestamp_ms = value.int >> 80

    assert encoded_timestamp_ms == timestamp_ms
    assert value.version == 7
    assert value.variant == "specified in RFC 4122"


def test_uuid7_is_monotonic_when_clock_repeats_or_regresses(monkeypatch) -> None:
    timestamps_ms = iter((10_000, 10_000, 9_999, 10_001))
    monkeypatch.setattr(
        litequeue.time,
        "time_ns",
        lambda: next(timestamps_ms) * 1_000_000,
    )
    monkeypatch.setattr(litequeue.os, "urandom", lambda size: bytes(size))
    monkeypatch.setattr(litequeue, "_last_timestamp_v7", None)
    monkeypatch.setattr(litequeue, "_last_counter_v7", 0)

    message_ids = [litequeue.uuid7() for _ in range(4)]

    assert message_ids == sorted(message_ids)
    assert len(set(message_ids)) == 4


def test_uuid7_is_unique_during_concurrent_generation(monkeypatch) -> None:
    timestamp_ms = 10_000
    monkeypatch.setattr(
        litequeue.time,
        "time_ns",
        lambda: timestamp_ms * 1_000_000,
    )
    monkeypatch.setattr(litequeue.os, "urandom", lambda size: bytes(size))
    monkeypatch.setattr(litequeue, "_last_timestamp_v7", None)
    monkeypatch.setattr(litequeue, "_last_counter_v7", 0)

    with ThreadPoolExecutor(max_workers=16) as executor:
        message_ids = list(executor.map(lambda _: litequeue.uuid7(), range(256)))

    uuid_values = sorted(message_id.int for message_id in message_ids)
    differences = [right - left for left, right in zip(uuid_values, uuid_values[1:])]

    assert len(set(message_ids)) == 256
    assert differences == [1 << 32] * 255


@pytest.mark.parametrize("pop_method", ("_pop_transaction", "_pop_returning"))
def test_mixed_uuid_formats_sort_by_message_id_after_reopen(
    tmp_path,
    monkeypatch,
    pop_method: str,
) -> None:
    database = tmp_path / "queue.sqlite3"
    old_message_id = "063e95f1-3d9e-7bbc-8000-a6a18a5f65d1"
    queue = LiteQueue(database)
    queue.conn.execute(
        f"""
        INSERT INTO {queue.table}
            (data, message_id, status, in_time, lock_time, done_time)
        VALUES (:data, :message_id, :status, :in_time, NULL, NULL)
        """,
        {
            "data": "old-format",
            "message_id": old_message_id,
            "status": MessageStatus.READY.value,
            "in_time": 1,
        },
    )
    queue.close()

    timestamp_ns = 1_645_557_742_000_000_000
    monkeypatch.setattr(litequeue.time, "time_ns", lambda: timestamp_ns)
    monkeypatch.setattr(litequeue.os, "urandom", lambda size: bytes(size))
    monkeypatch.setattr(litequeue, "_last_timestamp_v7", None)
    monkeypatch.setattr(litequeue, "_last_counter_v7", 0)

    reopened_queue = LiteQueue(database)
    reopened_queue.pop = getattr(reopened_queue, pop_method)
    new_message = reopened_queue.put("rfc-format")

    assert new_message.message_id < old_message_id
    assert reopened_queue.peek().message_id == new_message.message_id
    assert reopened_queue.pop().message_id == new_message.message_id

    reopened_queue.close()


@pytest.fixture(
    scope="function",
    params=[None, "CustomQueue"],
    ids=["table_name=<Default>", "table_name=CustomQueue"],
)
def queue_name(request) -> str:
    return request.param


@pytest.fixture(scope="function", params=["_pop_transaction", "_pop_returning"])
def single_queue(request, queue_name) -> LiteQueue:
    kwargs = {"filename_or_conn": ":memory:"}
    if queue_name is not None:
        kwargs["queue_name"] = queue_name
    _q = LiteQueue(**kwargs)

    if _q.get_sqlite_version() > 35:
        _q.pop = getattr(_q, request.param)

    return _q


@pytest.fixture(scope="function")
def queue_with_data(single_queue) -> LiteQueue:
    q = single_queue
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
    assert q.conn.isolation_level is None, (
        f"Isolation level not set properly for connection '{kwargs}'"
    )


@pytest.mark.parametrize(
    "queue_name",
    (
        "",
        "queue name",
        "queue;name",
        "queue--name",
        "queue/*name*/",
        "1queue",
        "café",
        "evil BEFORE INSERT ON victim BEGIN DELETE FROM victim; END; /*",
    ),
)
def test_invalid_queue_name_is_rejected_before_schema_changes(queue_name: str) -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute('CREATE TABLE "victim" ("value" TEXT NOT NULL)')
    conn.execute(
        'INSERT INTO "victim" ("value") VALUES (:value)',
        {"value": "safe"},
    )

    with pytest.raises(ValueError, match="Invalid table name"):
        LiteQueue(filename_or_conn=conn, maxsize=1, queue_name=queue_name)

    schema_objects = conn.execute(
        'SELECT "name" FROM "sqlite_master" WHERE "name" != :name',
        {"name": "victim"},
    ).fetchall()
    victim_rows = conn.execute('SELECT "value" FROM "victim"').fetchall()

    assert schema_objects == []
    assert victim_rows == [("safe",)]


@pytest.mark.parametrize(
    "queue_name", ("Queue", "custom_queue_2", "_private", "select")
)
def test_valid_queue_names_create_safely_quoted_schema(queue_name: str) -> None:
    q = LiteQueue(filename_or_conn=":memory:", maxsize=1, queue_name=queue_name)
    trigger_name = f"maxsize_control_{queue_name}"

    table = q.conn.execute(
        'SELECT "sql" FROM "sqlite_master" WHERE "type" = :type AND "name" = :name',
        {"type": "table", "name": queue_name},
    ).fetchone()
    indexes = q.conn.execute(
        'SELECT "sql" FROM "sqlite_master" WHERE "type" = :type ORDER BY "name"',
        {"type": "index"},
    ).fetchall()
    trigger = q.conn.execute(
        'SELECT "sql" FROM "sqlite_master" WHERE "type" = :type AND "name" = :name',
        {"type": "trigger", "name": trigger_name},
    ).fetchone()

    index_sql = [index[0] for index in indexes]

    assert table is not None
    assert f'CREATE TABLE "{queue_name}"' in table[0]
    assert index_sql == [
        f'CREATE INDEX "SIdx" ON "{queue_name}"(status)',
        f'CREATE INDEX "TIdx" ON "{queue_name}"(message_id)',
    ]
    assert trigger is not None
    assert f'CREATE TRIGGER "{trigger_name}"' in trigger[0]
    assert f'ON "{queue_name}"' in trigger[0]

    message = q.put("hello")
    assert q.get(message.message_id) is not None


def test_insert_pop(single_queue):
    q = single_queue
    first = q.put("hello")
    q.put("world")
    q.put("foo")
    q.put("bar")

    task = q.pop()
    assert task.data == "hello"
    assert str(task.message_id) == str(first.message_id)
    assert task.status == MessageStatus.LOCKED
    assert task.done_time is None


def test_get_unknow(single_queue):
    q = single_queue
    assert q.get("nothing") is None


def test_pop_all_locked(queue_with_data):
    q = queue_with_data
    # Lock every message
    for _ in range(4):
        q.pop()

    assert q.pop() is None


def test_basic_actions(queue_with_data):
    q = queue_with_data

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


def test_queue_size(queue_with_data):
    q = queue_with_data

    assert q.qsize() == 4
    task = q.pop()
    q.put("x")
    q.put("y")
    assert q.qsize() == 6
    q.done(task.message_id)
    assert q.qsize() == 5


def test_prune(queue_with_data):
    q = queue_with_data
    while not q.empty():
        t = q.pop()
        q.done(t.message_id)
    q.prune()

    assert (
        q.conn.execute(
            f"SELECT * FROM {q.table} WHERE status = {MessageStatus.DONE.value}"
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


def test_empty(queue_with_data):
    q = queue_with_data
    assert q.empty() is False

    q2 = LiteQueue(":memory:")
    assert q2.empty() is True


def test_list_locked(single_queue):
    q = single_queue
    q.put("foo")

    task = q.pop()

    time.sleep(0.2)

    assert len(list(q.list_locked(threshold_seconds=0.1))) == 1
    assert len(list(q.list_locked(threshold_seconds=20))) == 0

    q.done(task.message_id)

    assert len(list(q.list_locked(threshold_seconds=0.1))) == 0


def test_retry_failed(single_queue):
    q = single_queue
    q.put("foo")

    task = q.pop()

    q.mark_failed(task.message_id)

    assert q.get(task.message_id).status == MessageStatus.FAILED

    q.retry(task.message_id)

    assert q.get(task.message_id).status == MessageStatus.READY
    assert q.get(task.message_id).done_time is None
    assert q.qsize() == 1


def test_count_failed(single_queue):
    q = single_queue

    q.put("foot")
    task = q.pop()
    q.mark_failed(task.message_id)

    assert len(list(q.list_failed())) == 1
