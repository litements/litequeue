import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path

import pytest

import litequeue
from litequeue import LiteQueue
from litequeue import Message
from litequeue import MessageStatus


def require_message(message: Message | None) -> Message:
    """Return a message after asserting that a queue operation succeeded."""
    assert message is not None
    return message


@pytest.fixture(
    params=(
        pytest.param(
            "_pop_returning",
            marks=pytest.mark.skipif(
                sqlite3.sqlite_version_info < (3, 35, 0),
                reason="SQLite RETURNING requires SQLite 3.35 or newer",
            ),
        ),
        "_pop_transaction",
    )
)
def shared_queue(request, tmp_path: Path) -> LiteQueue:
    """Return a shared queue using one of the supported pop paths."""
    database_path = tmp_path / f"{request.param}.sqlite3"
    queue = LiteQueue(filename=database_path)
    queue.pop = getattr(queue, request.param)
    return queue


def test_file_queue_uses_shared_connection_options(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """File queues enable cross-thread use and disable statement caching."""
    connect = sqlite3.connect
    received_options: list[dict[str, object]] = []

    def recording_connect(*args, **kwargs):
        received_options.append(kwargs)
        return connect(*args, **kwargs)

    monkeypatch.setattr(litequeue.sqlite3, "connect", recording_connect)

    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    queue.close()

    assert len(received_options) == 11
    assert all(options["check_same_thread"] is False for options in received_options)
    assert all(options["cached_statements"] == 0 for options in received_options)


def test_read_pool_contains_ten_distinct_query_only_connections(
    tmp_path: Path,
) -> None:
    """File queues provide ten protected read slots separate from writes."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")

    with ExitStack() as stack:
        connections = [
            stack.enter_context(queue._read_connection()) for _ in range(10)
        ]

        assert len({id(connection) for connection in connections}) == 10
        assert all(connection is not queue.conn for connection in connections)
        assert all(
            connection.execute("PRAGMA query_only").fetchone()[0] == 1
            for connection in connections
        )
        with pytest.raises(sqlite3.OperationalError, match="readonly database"):
            connections[0].execute(f"DELETE FROM {queue.table}")


def test_read_connection_is_returned_to_pool(tmp_path: Path) -> None:
    """The eleventh reader waits until one of ten checked-out slots returns."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    read_started = threading.Event()

    def read_size() -> int:
        read_started.set()
        return queue.qsize()

    with ThreadPoolExecutor(max_workers=1) as executor:
        with ExitStack() as stack:
            for _ in range(10):
                stack.enter_context(queue._read_connection())
            result = executor.submit(read_size)
            assert read_started.wait(timeout=5)
            assert not result.done()

        assert result.result(timeout=5) == 0


def test_shared_queue_enforces_maxsize_under_contention(tmp_path: Path) -> None:
    """Concurrent producers on one instance cannot exceed its capacity."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3", maxsize=8)

    def put_message(index: int) -> bool:
        result = True
        try:
            queue.put(str(index))
        except sqlite3.IntegrityError:
            result = False
        return result

    with ThreadPoolExecutor(max_workers=16) as executor:
        results = list(executor.map(put_message, range(32)))

    assert results.count(True) == 8
    assert results.count(False) == 24
    assert queue.qsize() == 8


@pytest.mark.parametrize("message_count", (16, 128))
def test_shared_queue_supports_concurrent_lifecycle_operations(
    shared_queue: LiteQueue,
    message_count: int,
) -> None:
    """One queue supports concurrent put, pop, and completion."""
    queue = shared_queue

    with ThreadPoolExecutor(max_workers=16) as executor:
        inserted = list(executor.map(queue.put, map(str, range(message_count))))
        pop_results = executor.map(lambda _: queue.pop(), range(message_count))
        popped = [require_message(message) for message in pop_results]
        completed = list(
            executor.map(queue.done, [message.message_id for message in popped])
        )

    inserted_ids = {message.message_id for message in inserted}
    popped_ids = {message.message_id for message in popped}
    assert popped_ids == inserted_ids
    assert completed == [True] * message_count
    assert queue.qsize() == 0


def test_concurrent_consumers_do_not_duplicate_claims(
    shared_queue: LiteQueue,
) -> None:
    """Repeated contention never returns one message to two consumers."""
    queue = shared_queue
    message_count = 256
    for index in range(message_count):
        queue.put(str(index))

    with ThreadPoolExecutor(max_workers=32) as executor:
        pop_results = executor.map(lambda _: queue.pop(), range(message_count))
        popped = [require_message(message) for message in pop_results]
        empty_results = list(executor.map(lambda _: queue.pop(), range(32)))

    message_ids = [message.message_id for message in popped]
    assert len(set(message_ids)) == message_count
    assert empty_results == [None] * 32


def test_transaction_rollback_excludes_concurrent_put(tmp_path: Path) -> None:
    """Another thread cannot join and be reverted by an active transaction."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    transaction_started = threading.Event()
    allow_rollback = threading.Event()

    def rollback_message() -> None:
        with pytest.raises(RuntimeError, match="roll back"):
            with queue.transaction(mode="IMMEDIATE"):
                queue.put("rolled back")
                transaction_started.set()
                allow_rollback.wait(timeout=5)
                raise RuntimeError("roll back")

    with ThreadPoolExecutor(max_workers=2) as executor:
        rollback_result = executor.submit(rollback_message)
        assert transaction_started.wait(timeout=5)
        committed_result = executor.submit(queue.put, "committed")
        assert not committed_result.done()
        allow_rollback.set()
        rollback_result.result()
        committed = committed_result.result()

    assert queue.get(committed.message_id) == committed
    assert queue.qsize() == 1


def test_reads_continue_without_observing_uncommitted_writes(tmp_path: Path) -> None:
    """Read connections see committed state while another thread may roll back."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    queue.put("committed")
    transaction_started = threading.Event()
    allow_rollback = threading.Event()
    uncommitted_message_ids: list[str] = []

    def rollback_message() -> None:
        with pytest.raises(RuntimeError, match="roll back"):
            with queue.transaction(mode="IMMEDIATE"):
                message = queue.put("uncommitted")
                uncommitted_message_ids.append(message.message_id)
                transaction_started.set()
                allow_rollback.wait(timeout=5)
                raise RuntimeError("roll back")

    with ThreadPoolExecutor(max_workers=2) as executor:
        rollback_result = executor.submit(rollback_message)
        assert transaction_started.wait(timeout=5)
        read_result = executor.submit(queue.qsize)

        assert read_result.result(timeout=5) == 1
        uncommitted_message_id = uncommitted_message_ids[0]
        assert queue.get(uncommitted_message_id) is None

        allow_rollback.set()
        rollback_result.result()


def test_transaction_owner_reads_its_uncommitted_writes(tmp_path: Path) -> None:
    """Reads in the transaction thread use the write connection."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    message_id = ""

    with pytest.raises(RuntimeError, match="roll back"):
        with queue.transaction(mode="IMMEDIATE"):
            message = queue.put("uncommitted")
            message_id = message.message_id
            assert queue.get(message.message_id) == message
            raise RuntimeError("roll back")

    assert queue.get(message_id) is None


def test_concurrent_failure_and_retry_transitions(tmp_path: Path) -> None:
    """Failure and retry operations are safe on one shared connection."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    for index in range(64):
        queue.put(str(index))
    messages = [require_message(queue.pop()) for _ in range(64)]
    message_ids = [message.message_id for message in messages]

    with ThreadPoolExecutor(max_workers=16) as executor:
        failed = list(executor.map(queue.mark_failed, message_ids))
        retried = list(executor.map(queue.retry, message_ids))

    assert failed == [True] * 64
    assert retried == [True] * 64
    assert queue.qsize() == 64
    stored_messages = [
        require_message(queue.get(message_id)) for message_id in message_ids
    ]
    statuses = [message.status for message in stored_messages]
    assert all(status is MessageStatus.READY for status in statuses)


def test_close_waits_for_active_transaction(tmp_path: Path) -> None:
    """Close cannot interrupt a transaction running in another thread."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    transaction_started = threading.Event()
    allow_commit = threading.Event()
    close_started = threading.Event()

    def hold_transaction() -> None:
        with queue.transaction(mode="IMMEDIATE"):
            queue.put("committed")
            transaction_started.set()
            allow_commit.wait(timeout=5)

    def close_queue() -> None:
        close_started.set()
        queue.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        transaction_result = executor.submit(hold_transaction)
        assert transaction_started.wait(timeout=5)
        close_result = executor.submit(close_queue)
        assert close_started.wait(timeout=5)
        assert not close_result.done()
        allow_commit.set()
        transaction_result.result()
        close_result.result()

    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        queue.qsize()


def test_close_waits_for_checked_out_read_connection(tmp_path: Path) -> None:
    """Pool shutdown waits until an active reader returns its connection."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    read_started = threading.Event()
    allow_read_to_finish = threading.Event()
    close_started = threading.Event()

    def hold_read_connection() -> None:
        with queue._read_connection():
            read_started.set()
            allow_read_to_finish.wait(timeout=5)

    def close_queue() -> None:
        close_started.set()
        queue.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        read_result = executor.submit(hold_read_connection)
        assert read_started.wait(timeout=5)
        close_result = executor.submit(close_queue)
        assert close_started.wait(timeout=5)
        assert not close_result.done()

        allow_read_to_finish.set()
        read_result.result()
        close_result.result()
