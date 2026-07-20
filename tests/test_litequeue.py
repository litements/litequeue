import sqlite3
import threading
import time
from pathlib import Path

import pytest

from litequeue import LiteQueue
from litequeue import MessageStatus

print(sqlite3.sqlite_version)

# https://docs.pytest.org/en/7.1.x/how-to/fixtures.html#parametrizing-fixtures


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
        f'CREATE UNIQUE INDEX "{queue_name}_message_id_unique_idx" '
        f'ON "{queue_name}"(message_id)',
        f'CREATE INDEX "{queue_name}_status_message_id_idx" '
        f'ON "{queue_name}"(status, message_id)',
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


def test_maxsize_persists_when_reopened_without_a_value(tmp_path):
    database_path = tmp_path / "queue.db"
    queue = LiteQueue(database_path, maxsize=1)
    queue.put("first")
    queue.close()

    reopened_queue = LiteQueue(database_path)

    assert reopened_queue.maxsize == 1
    assert reopened_queue.full()
    with pytest.raises(sqlite3.IntegrityError, match="Max queue length reached: 1"):
        reopened_queue.put("second")


def test_maxsize_reopens_with_the_same_value(tmp_path):
    database_path = tmp_path / "queue.db"
    LiteQueue(database_path, maxsize=2).close()

    reopened_queue = LiteQueue(database_path, maxsize=2)

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
    database_path = tmp_path / "queue.db"
    LiteQueue(database_path, maxsize=original_maxsize).close()

    with pytest.raises(ValueError, match="conflicts with stored maxsize"):
        LiteQueue(database_path, maxsize=conflicting_maxsize)

    reopened_queue = LiteQueue(database_path)
    assert reopened_queue.maxsize == original_maxsize


def test_concurrent_producers_do_not_exceed_persistent_maxsize(tmp_path):
    database_path = tmp_path / "queue.db"
    LiteQueue(database_path, maxsize=5).close()
    barrier = threading.Barrier(10)
    results = []

    def put_message(index):
        queue = LiteQueue(database_path)
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

    queue = LiteQueue(database_path)
    assert results.count(True) == 5
    assert results.count(False) == 5
    assert queue.qsize() == 5
    assert queue.full()


def test_zero_maxsize_is_persistent(tmp_path):
    database_path = tmp_path / "queue.db"
    LiteQueue(database_path, maxsize=0).close()

    reopened_queue = LiteQueue(database_path)

    assert reopened_queue.maxsize == 0
    assert reopened_queue.full()
    with pytest.raises(sqlite3.IntegrityError, match="Max queue length reached: 0"):
        reopened_queue.put("message")


@pytest.mark.parametrize("maxsize", (-1, -10))
def test_negative_maxsize_is_rejected_before_schema_changes(tmp_path, maxsize):
    database_path = tmp_path / "queue.db"

    with pytest.raises(ValueError, match="maxsize must be zero or a positive integer"):
        LiteQueue(database_path, maxsize=maxsize)

    assert not database_path.exists()


@pytest.mark.parametrize("maxsize", (True, 1.5, "2"))
def test_invalid_maxsize_type_is_rejected_before_schema_changes(tmp_path, maxsize):
    database_path = tmp_path / "queue.db"

    with pytest.raises(TypeError, match="maxsize must be an integer or None"):
        LiteQueue(database_path, maxsize=maxsize)

    assert not database_path.exists()


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


def test_each_queue_gets_queue_specific_indexes(tmp_path: Path) -> None:
    """Every queue in one database receives its own complete index set."""
    database_path = tmp_path / "multiple-queues.sqlite3"
    first_queue = LiteQueue(database_path, queue_name="incoming")
    second_queue = LiteQueue(database_path, queue_name="outgoing")

    first_indexes = get_queue_indexes(first_queue, "incoming")
    second_indexes = get_queue_indexes(second_queue, "outgoing")

    assert first_indexes == {
        "incoming_message_id_unique_idx": (True, ["message_id"]),
        "incoming_status_message_id_idx": (False, ["status", "message_id"]),
    }
    assert second_indexes == {
        "outgoing_message_id_unique_idx": (True, ["message_id"]),
        "outgoing_status_message_id_idx": (False, ["status", "message_id"]),
    }


def test_duplicate_message_ids_are_rejected_by_sqlite() -> None:
    """The database, rather than application code, enforces message ID uniqueness."""
    queue = LiteQueue(":memory:")
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
    queue = LiteQueue(":memory:")
    plan_rows = queue.conn.execute(
        f"EXPLAIN QUERY PLAN {statement}",
        {"now": time.time_ns()},
    ).fetchall()
    plan = "\n".join(row["detail"] for row in plan_rows)

    assert "Queue_status_message_id_idx" in plan
    assert "USE TEMP B-TREE" not in plan


def test_opening_legacy_queue_migrates_indexes(tmp_path: Path) -> None:
    """Opening an old queue replaces its global indexes without changing rows."""
    database_path = tmp_path / "legacy.sqlite3"
    connection = sqlite3.connect(database_path)
    connection.execute(
        """
        CREATE TABLE Queue (
            data TEXT NOT NULL,
            message_id TEXT NOT NULL,
            status INTEGER NOT NULL,
            in_time INTEGER NOT NULL,
            lock_time INTEGER,
            done_time INTEGER
        )
        """
    )
    connection.execute("CREATE INDEX TIdx ON Queue(message_id)")
    connection.execute("CREATE INDEX SIdx ON Queue(status)")
    connection.execute(
        "INSERT INTO Queue VALUES ('preserved', 'id-1', 0, 1, NULL, NULL)"
    )
    connection.commit()
    connection.close()

    queue = LiteQueue(database_path)

    assert queue.get("id-1").data == "preserved"
    assert get_queue_indexes(queue, "Queue") == {
        "Queue_message_id_unique_idx": (True, ["message_id"]),
        "Queue_status_message_id_idx": (False, ["status", "message_id"]),
    }


def test_opening_legacy_queue_with_duplicate_ids_fails(tmp_path: Path) -> None:
    """Legacy duplicate IDs fail migration instead of being silently removed."""
    database_path = tmp_path / "legacy-duplicates.sqlite3"
    connection = sqlite3.connect(database_path)
    connection.execute(
        """
        CREATE TABLE Queue (
            data TEXT NOT NULL,
            message_id TEXT NOT NULL,
            status INTEGER NOT NULL,
            in_time INTEGER NOT NULL,
            lock_time INTEGER,
            done_time INTEGER
        )
        """
    )
    connection.execute(
        "INSERT INTO Queue VALUES ('first', 'duplicate', 0, 1, NULL, NULL)"
    )
    connection.execute(
        "INSERT INTO Queue VALUES ('second', 'duplicate', 0, 2, NULL, NULL)"
    )
    connection.commit()

    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed"):
        LiteQueue(connection)

    row_count = connection.execute("SELECT COUNT(*) FROM Queue").fetchone()[0]
    assert row_count == 2
    connection.close()
