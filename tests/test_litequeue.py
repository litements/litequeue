import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import litequeue
from litequeue import LiteQueue
from litequeue import Message
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


def test_mixed_uuid_formats_sort_by_message_id_after_reopen(
    tmp_path,
    monkeypatch,
) -> None:
    old_message_id = "063e95f1-3d9e-7bbc-8000-a6a18a5f65d1"
    queue = LiteQueue(name="queue", folder=tmp_path)
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

    reopened_queue = LiteQueue(name="queue", folder=tmp_path)
    new_message = reopened_queue.put("rfc-format")

    assert new_message.message_id < old_message_id
    assert reopened_queue.peek().message_id == new_message.message_id
    assert reopened_queue.pop().message_id == new_message.message_id

    reopened_queue.close()


@pytest.fixture(scope="function")
def single_queue() -> LiteQueue:
    connection = sqlite3.connect(":memory:")
    _q = LiteQueue(conn=connection)
    return _q


@pytest.fixture(scope="function")
def queue_with_data(single_queue) -> LiteQueue:
    q = single_queue
    q.put("hello")
    q.put("world")
    q.put("foo")
    q.put("bar")
    return q


def test_existing_connection_is_used_in_autocommit_mode() -> None:
    """A supplied connection is reused and switched to autocommit mode."""
    connection = sqlite3.connect(":memory:")

    queue = LiteQueue(conn=connection)

    assert queue.conn is connection
    assert queue.conn.isolation_level is None


@pytest.mark.parametrize(
    ("sqlite_version", "expected_pop_method"),
    (
        ((3, 34, 0), "_pop_transaction"),
        ((3, 35, 0), "_pop_returning"),
    ),
)
def test_selects_pop_method_for_sqlite_features(
    monkeypatch,
    sqlite_version: tuple[int, int, int],
    expected_pop_method: str,
) -> None:
    """Pop uses RETURNING when SQLite supports it and the fallback otherwise."""
    monkeypatch.setattr(litequeue.sqlite3, "sqlite_version_info", sqlite_version)
    connection = sqlite3.connect(":memory:")

    queue = LiteQueue(conn=connection)

    assert queue.pop == getattr(queue, expected_pop_method)


@pytest.mark.parametrize(
    "kwargs",
    (
        {},
        {"name": "queue", "conn": sqlite3.connect(":memory:")},
    ),
)
def test_name_and_connection_are_mutually_exclusive(kwargs) -> None:
    """Exactly one queue name or SQLite connection is required."""
    with pytest.raises(ValueError, match="Exactly one of name or conn"):
        LiteQueue(**kwargs)


def test_name_creates_queue_sqlite3_database(tmp_path: Path) -> None:
    """The queue name maps directly to a .queue.sqlite3 database filename."""
    database_path = tmp_path / "email.queue.sqlite3"

    queue = LiteQueue(name="email", folder=tmp_path)
    queue.close()

    assert database_path.is_file()


def test_name_uses_current_directory_by_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Named queues default to the current working directory."""
    monkeypatch.chdir(tmp_path)

    queue = LiteQueue(name="jobs")
    queue.close()

    assert (tmp_path / "jobs.queue.sqlite3").is_file()


def test_queues_in_different_folders_are_created_and_reused(tmp_path: Path) -> None:
    """A name and folder pair identifies one persistent queue database."""
    incoming_folder = tmp_path / "incoming"
    outgoing_folder = tmp_path / "outgoing"
    incoming_folder.mkdir()
    outgoing_folder.mkdir()

    incoming_queue = LiteQueue(name="incoming", folder=incoming_folder)
    outgoing_queue = LiteQueue(name="outgoing", folder=outgoing_folder)
    incoming_message = incoming_queue.put("receive order")
    outgoing_message = outgoing_queue.put("send receipt")
    incoming_queue.close()
    outgoing_queue.close()

    queue_locations = (
        ("incoming", incoming_folder, incoming_message.message_id),
        ("outgoing", outgoing_folder, outgoing_message.message_id),
    )
    for queue_name, queue_folder, message_id in queue_locations:
        database_path = queue_folder / f"{queue_name}.queue.sqlite3"
        assert database_path.is_file()

        reopened_queue = LiteQueue(name=queue_name, folder=queue_folder)
        table_rows = reopened_queue.conn.execute(
            """
            SELECT name
            FROM sqlite_schema
            WHERE type = 'table' AND name NOT GLOB 'sqlite_*'
            """
        ).fetchall()

        assert [row["name"] for row in table_rows] == ["Queue"]
        assert get_queue_indexes(reopened_queue, "Queue") == {
            "Queue_message_id_unique_idx": (True, ["message_id"]),
            "Queue_status_message_id_idx": (False, ["status", "message_id"]),
        }
        assert reopened_queue.get(message_id) is not None
        assert reopened_queue.qsize() == 1
        reopened_queue.close()


def test_folder_cannot_be_used_with_connection(tmp_path: Path) -> None:
    """A folder is meaningful only when LiteQueue creates the database."""
    connection = sqlite3.connect(":memory:")

    with pytest.raises(ValueError, match="folder cannot be used with conn"):
        LiteQueue(conn=connection, folder=tmp_path)


def test_name_cannot_include_a_directory(tmp_path: Path) -> None:
    """Queue location is controlled only through the folder argument."""
    name_with_path = str(tmp_path / "jobs")

    with pytest.raises(ValueError, match="name must not contain a directory path"):
        LiteQueue(name=name_with_path)


def test_queue_creates_fixed_schema() -> None:
    """A new database always receives the fixed Queue schema."""
    connection = sqlite3.connect(":memory:")
    q = LiteQueue(conn=connection, maxsize=1)

    table = q.conn.execute(
        'SELECT "sql" FROM "sqlite_master" WHERE "type" = :type AND "name" = :name',
        {"type": "table", "name": "Queue"},
    ).fetchone()
    indexes = q.conn.execute(
        'SELECT "sql" FROM "sqlite_master" WHERE "type" = :type ORDER BY "name"',
        {"type": "index"},
    ).fetchall()
    trigger = q.conn.execute(
        'SELECT "sql" FROM "sqlite_master" WHERE "type" = :type AND "name" = :name',
        {"type": "trigger", "name": "maxsize_control_Queue"},
    ).fetchone()

    index_sql = [index[0] for index in indexes]

    assert table is not None
    assert 'CREATE TABLE "Queue"' in table[0]
    assert index_sql == [
        'CREATE UNIQUE INDEX "Queue_message_id_unique_idx" ON "Queue"(message_id)',
        'CREATE INDEX "Queue_status_message_id_idx" ON "Queue"(status, message_id)',
    ]
    assert trigger is not None
    assert 'CREATE TRIGGER "maxsize_control_Queue"' in trigger[0]
    assert 'ON "Queue"' in trigger[0]

    message = q.put("hello")
    assert q.get(message.message_id) is not None


def test_database_with_custom_queue_table_is_rejected_without_changes() -> None:
    """A legacy custom queue table produces a clear, non-mutating error."""
    connection = sqlite3.connect(":memory:")
    connection.execute('CREATE TABLE "CustomQueue" ("value" TEXT NOT NULL)')
    connection.execute(
        'INSERT INTO "CustomQueue" ("value") VALUES (:value)',
        {"value": "preserved"},
    )

    expected_message = (
        "LiteQueue no longer supports multiple queues or other tables in one "
        "database. Found unsupported table: CustomQueue. Each queue must use "
        "its own database."
    )
    with pytest.raises(ValueError, match=expected_message):
        LiteQueue(conn=connection)

    table_names = connection.execute(
        "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name"
    ).fetchall()
    stored_rows = connection.execute('SELECT "value" FROM "CustomQueue"').fetchall()

    assert [tuple(row) for row in table_names] == [("CustomQueue",)]
    assert [tuple(row) for row in stored_rows] == [("preserved",)]


def test_database_with_queue_and_another_table_is_rejected(tmp_path: Path) -> None:
    """A database cannot mix the Queue table with another application table."""
    database_path = tmp_path / "shared.queue.sqlite3"
    queue = LiteQueue(name="shared", folder=tmp_path)
    message = queue.put("preserved queue message")
    queue.close()

    connection = sqlite3.connect(database_path)
    connection.execute('CREATE TABLE "ApplicationData" ("value" TEXT NOT NULL)')
    connection.commit()

    with pytest.raises(
        ValueError,
        match="Found unsupported table: ApplicationData",
    ):
        LiteQueue(conn=connection)

    queue_row = connection.execute(
        'SELECT "data" FROM "Queue" WHERE "message_id" = :message_id',
        {"message_id": message.message_id},
    ).fetchone()
    assert tuple(queue_row) == ("preserved queue message",)


def test_database_error_lists_multiple_unsupported_tables() -> None:
    """The error identifies every table that prevents single-queue use."""
    connection = sqlite3.connect(":memory:")
    connection.execute('CREATE TABLE "Incoming" ("value" TEXT)')
    connection.execute('CREATE TABLE "Outgoing" ("value" TEXT)')

    with pytest.raises(
        ValueError,
        match="Found unsupported tables: Incoming, Outgoing",
    ):
        LiteQueue(conn=connection)

    table_names = connection.execute(
        "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name"
    ).fetchall()
    assert [tuple(row) for row in table_names] == [("Incoming",), ("Outgoing",)]


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
    connection = sqlite3.connect(":memory:")
    q = LiteQueue(conn=connection, maxsize=50)
    for i in range(50):
        q.put(f"data_{i}")
    assert q.qsize() == 50

    with pytest.raises(sqlite3.IntegrityError):
        q.put("new")

    assert q.full()

    q.pop()

    assert not q.full()


def test_maxsize_persists_when_reopened_without_a_value(tmp_path):
    queue = LiteQueue(name="queue", folder=tmp_path, maxsize=1)
    queue.put("first")
    queue.close()

    reopened_queue = LiteQueue(name="queue", folder=tmp_path)

    assert reopened_queue.maxsize == 1
    assert reopened_queue.full()
    with pytest.raises(sqlite3.IntegrityError, match="Max queue length reached: 1"):
        reopened_queue.put("second")


def test_maxsize_reopens_with_the_same_value(tmp_path):
    LiteQueue(name="queue", folder=tmp_path, maxsize=2).close()

    reopened_queue = LiteQueue(name="queue", folder=tmp_path, maxsize=2)

    assert reopened_queue.maxsize == 2


@pytest.mark.parametrize(
    ("original_maxsize", "conflicting_maxsize"),
    ((1, 2), (None, 1)),
)
def test_conflicting_maxsize_is_rejected(
    tmp_path,
    original_maxsize,
    conflicting_maxsize,
):
    LiteQueue(
        name="queue",
        folder=tmp_path,
        maxsize=original_maxsize,
    ).close()

    with pytest.raises(ValueError, match="conflicts with stored maxsize"):
        LiteQueue(
            name="queue",
            folder=tmp_path,
            maxsize=conflicting_maxsize,
        )

    reopened_queue = LiteQueue(name="queue", folder=tmp_path)
    assert reopened_queue.maxsize == original_maxsize


def test_concurrent_producers_do_not_exceed_persistent_maxsize(tmp_path):
    LiteQueue(name="queue", folder=tmp_path, maxsize=5).close()
    barrier = threading.Barrier(10)
    results = []

    def put_message(index):
        queue = LiteQueue(name="queue", folder=tmp_path)
        barrier.wait()
        try:
            queue.put(f"message-{index}")
        except sqlite3.IntegrityError:
            results.append(False)
        else:
            results.append(True)
        finally:
            queue.close()

    threads = [
        threading.Thread(target=put_message, args=(index,)) for index in range(10)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    queue = LiteQueue(name="queue", folder=tmp_path)
    assert results.count(True) == 5
    assert results.count(False) == 5
    assert queue.qsize() == 5
    assert queue.full()


def test_zero_maxsize_is_persistent(tmp_path):
    LiteQueue(name="queue", folder=tmp_path, maxsize=0).close()

    reopened_queue = LiteQueue(name="queue", folder=tmp_path)

    assert reopened_queue.maxsize == 0
    assert reopened_queue.full()
    with pytest.raises(sqlite3.IntegrityError, match="Max queue length reached: 0"):
        reopened_queue.put("message")


@pytest.mark.parametrize("maxsize", (-1, -10))
def test_negative_maxsize_is_rejected_before_schema_changes(tmp_path, maxsize):
    database_path = tmp_path / "queue.queue.sqlite3"

    with pytest.raises(ValueError, match="maxsize must be zero or a positive integer"):
        LiteQueue(name="queue", folder=tmp_path, maxsize=maxsize)

    assert not database_path.exists()


@pytest.mark.parametrize("maxsize", (True, 1.5, "2"))
def test_invalid_maxsize_type_is_rejected_before_schema_changes(tmp_path, maxsize):
    database_path = tmp_path / "queue.queue.sqlite3"

    with pytest.raises(TypeError, match="maxsize must be an integer or None"):
        LiteQueue(name="queue", folder=tmp_path, maxsize=maxsize)

    assert not database_path.exists()


def test_empty(queue_with_data):
    q = queue_with_data
    assert q.empty() is False

    connection = sqlite3.connect(":memory:")
    q2 = LiteQueue(conn=connection)
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


def test_done_returns_whether_message_exists(single_queue: LiteQueue) -> None:
    q = single_queue
    task = q.put("foo")

    assert q.done(task.message_id) is True
    assert q.done("missing-message") is False


def test_mark_failed_returns_whether_message_exists(single_queue: LiteQueue) -> None:
    q = single_queue
    task = q.put("foo")

    assert q.mark_failed(task.message_id) is True
    assert q.mark_failed("missing-message") is False


def test_retry_returns_whether_message_exists(single_queue: LiteQueue) -> None:
    q = single_queue
    task = q.put("foo")
    q.mark_failed(task.message_id)

    assert q.retry(task.message_id) is True
    assert q.retry("missing-message") is False


def test_count_failed(single_queue):
    q = single_queue

    q.put("foot")
    task = q.pop()
    q.mark_failed(task.message_id)

    assert len(list(q.list_failed())) == 1


def get_queue_indexes(
    queue: LiteQueue,
    table_name: str,
) -> dict[str, tuple[bool, list[str]]]:
    """Return index uniqueness and columns for a queue table."""
    indexes: dict[str, tuple[bool, list[str]]] = {}
    index_rows = queue.conn.execute(f'PRAGMA index_list("{table_name}")').fetchall()

    for index_row in index_rows:
        index_name = index_row["name"]
        column_rows = queue.conn.execute(
            f'PRAGMA index_info("{index_name}")'
        ).fetchall()
        columns = [column_row["name"] for column_row in column_rows]
        indexes[index_name] = (bool(index_row["unique"]), columns)

    return indexes


def test_queue_gets_fixed_indexes() -> None:
    """The single Queue table receives the fixed unique and FIFO indexes."""
    connection = sqlite3.connect(":memory:")
    queue = LiteQueue(conn=connection)

    assert get_queue_indexes(queue, "Queue") == {
        "Queue_message_id_unique_idx": (True, ["message_id"]),
        "Queue_status_message_id_idx": (False, ["status", "message_id"]),
    }


def test_duplicate_message_ids_are_rejected_by_sqlite() -> None:
    """The database, rather than application code, enforces message ID uniqueness."""
    connection = sqlite3.connect(":memory:")
    queue = LiteQueue(conn=connection)
    message = queue.put("original")

    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed"):
        queue.conn.execute(
            f"""
            INSERT INTO {queue.table}
                (data, message_id, status, in_time, lock_time, done_time)
            VALUES
                (:data, :message_id, :status, :in_time, NULL, NULL)
            """,
            {
                "data": "duplicate",
                "message_id": message.message_id,
                "status": MessageStatus.READY.value,
                "in_time": time.time_ns(),
            },
        )


@pytest.mark.parametrize(
    "statement",
    (
        "SELECT * FROM [Queue] WHERE status = 0 ORDER BY message_id LIMIT 1",
        """
        UPDATE [Queue]
        SET status = 1, lock_time = :now
        WHERE rowid = (
            SELECT rowid
            FROM [Queue]
            WHERE status = 0
            ORDER BY message_id
            LIMIT 1
        )
        RETURNING *
        """,
    ),
    ids=("peek", "pop"),
)
def test_fifo_queries_use_composite_index_without_temporary_sort(statement: str) -> None:
    """FIFO peek and pop use the composite status/message ID index."""
    connection = sqlite3.connect(":memory:")
    queue = LiteQueue(conn=connection)
    plan_rows = queue.conn.execute(
        f"EXPLAIN QUERY PLAN {statement}",
        {"now": time.time_ns()},
    ).fetchall()
    plan = "\n".join(row["detail"] for row in plan_rows)

    assert "Queue_status_message_id_idx" in plan
    assert "USE TEMP B-TREE" not in plan


def test_all_message_read_paths_return_typed_status(single_queue) -> None:
    q = single_queue

    q.put("locked")
    failed_message = q.put("failed")
    ready_message = q.put("ready")
    locked_message = q.pop()
    message_to_fail = q.pop()
    q.mark_failed(message_to_fail.message_id)

    messages = [
        ready_message,
        q.peek(),
        q.get(ready_message.message_id),
        locked_message,
        *q.list_locked(threshold_seconds=0),
        *q.list_failed(),
    ]

    assert failed_message.message_id == message_to_fail.message_id
    assert all(isinstance(message, Message) for message in messages)
    assert all(isinstance(message.status, MessageStatus) for message in messages)
    assert {message.status for message in messages} == {
        MessageStatus.READY,
        MessageStatus.LOCKED,
        MessageStatus.FAILED,
    }


def test_unknown_stored_status_raises_useful_error(single_queue) -> None:
    q = single_queue
    message = q.put("invalid status")
    q.conn.execute(
        f"UPDATE {q.table} SET status = :status WHERE message_id = :message_id",
        {"status": 99, "message_id": message.message_id},
    )

    with pytest.raises(ValueError, match="Unknown message status: 99"):
        q.get(message.message_id)
