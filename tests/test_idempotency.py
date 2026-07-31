import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from litequeue import IdempotencyConflict
from litequeue import LiteQueue
from litequeue import Message
from litequeue import MessageStatus


def test_put_replay_returns_the_original_message(tmp_path: Path) -> None:
    """A repeated key and payload return one persistent queue message."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")

    original = queue.put("charge account", idempotency_key="charge-123")
    replay = queue.put("charge account", idempotency_key="charge-123")

    assert replay == original
    assert replay.idempotency_key == "charge-123"
    assert queue.stored_count() == 1


def test_put_replay_returns_the_current_message_state(tmp_path: Path) -> None:
    """A replay returns the current state instead of an old state snapshot."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    original = queue.put("charge account", idempotency_key="charge-123")
    claim = queue.pop()

    assert claim is not None

    locked_replay = queue.put(
        "charge account",
        idempotency_key="charge-123",
    )

    assert locked_replay.message_id == original.message_id
    assert locked_replay.status is MessageStatus.LOCKED
    assert locked_replay.claim_id == claim.claim_id
    assert queue.done(message_id=claim.message_id, claim_id=claim.claim_id)

    done_replay = queue.put(
        "charge account",
        idempotency_key="charge-123",
    )

    assert done_replay.message_id == original.message_id
    assert done_replay.status is MessageStatus.DONE
    assert queue.stored_count() == 1


def test_put_rejects_different_data_for_an_existing_key(tmp_path: Path) -> None:
    """One idempotency key cannot identify two different message payloads."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    original = queue.put("amount=10", idempotency_key="charge-123")

    with pytest.raises(IdempotencyConflict) as error:
        queue.put("amount=1000", idempotency_key="charge-123")

    assert error.value.idempotency_key == "charge-123"
    assert "already exists with different data" in str(error.value)
    assert queue.get(original.message_id) == original
    assert queue.stored_count() == 1


def test_put_without_a_key_always_creates_a_new_message(tmp_path: Path) -> None:
    """The default put behavior does not deduplicate equal payloads."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")

    first = queue.put("same data")
    second = queue.put("same data")

    assert first.message_id != second.message_id
    assert first.idempotency_key is None
    assert second.idempotency_key is None
    assert queue.stored_count() == 2


def test_unkeyed_messages_support_the_claim_aware_lifecycle(tmp_path: Path) -> None:
    """Idempotency keys remain optional when consumers use claim IDs."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    inserted = queue.put("work")

    assert inserted.idempotency_key is None
    assert inserted.claim_id == 0

    first_claim = queue.pop()

    assert first_claim is not None
    assert first_claim.idempotency_key is None
    assert first_claim.claim_id == 1
    assert queue.retry(
        message_id=first_claim.message_id,
        claim_id=first_claim.claim_id,
    )

    second_claim = queue.pop()

    assert second_claim is not None
    assert second_claim.message_id == inserted.message_id
    assert second_claim.idempotency_key is None
    assert second_claim.claim_id == first_claim.claim_id + 1
    assert queue.done(
        message_id=second_claim.message_id,
        claim_id=second_claim.claim_id,
    )

    completed = queue.get(inserted.message_id)

    assert completed is not None
    assert completed.status is MessageStatus.DONE
    assert completed.idempotency_key is None


@pytest.mark.parametrize("data", (None, True, 1, 1.5, b"work", object()))
def test_put_rejects_nonstring_message_data(
    tmp_path: Path,
    data: object,
) -> None:
    """Put rejects values that SQLite could convert into different text."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")

    with pytest.raises(TypeError, match="data must be a string"):
        queue.put(
            data,  # type: ignore[arg-type]
            idempotency_key="request-1",
        )

    assert queue.stored_count() == 0


def test_different_keys_can_use_the_same_data(tmp_path: Path) -> None:
    """Different logical operations can have identical message payloads."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")

    first = queue.put("same data", idempotency_key="operation-1")
    second = queue.put("same data", idempotency_key="operation-2")

    assert first.message_id != second.message_id
    assert queue.stored_count() == 2


@pytest.mark.parametrize("idempotency_key", (1, True, 1.5, b"key", object()))
def test_put_rejects_nonstring_idempotency_keys(
    tmp_path: Path,
    idempotency_key: object,
) -> None:
    """Put rejects key types that SQLite could coerce or cannot store."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")

    with pytest.raises(
        TypeError,
        match="idempotency_key must be a string or None",
    ):
        queue.put(
            "work",
            idempotency_key=idempotency_key,  # type: ignore[arg-type]
        )

    assert queue.stored_count() == 0


@pytest.mark.parametrize(
    "idempotency_key",
    (
        "",
        "' OR 1 = 1 --",
        '"; DROP TABLE Queue; --',
        "注文/ключ/🔒",
        "key\x00suffix",
        " ",
    ),
)
def test_put_treats_special_idempotency_keys_as_opaque_text(
    tmp_path: Path,
    idempotency_key: str,
) -> None:
    """Parameterized SQL stores special key text without interpretation."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")

    original = queue.put("work", idempotency_key=idempotency_key)
    replay = queue.put("work", idempotency_key=idempotency_key)

    assert replay == original
    assert replay.idempotency_key == idempotency_key
    assert queue.stored_count() == 1


def test_idempotency_keys_are_case_sensitive(tmp_path: Path) -> None:
    """Keys with different letter cases identify different operations."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")

    uppercase = queue.put("work", idempotency_key="REQUEST-1")
    lowercase = queue.put("work", idempotency_key="request-1")

    assert uppercase.message_id != lowercase.message_id
    assert queue.stored_count() == 2


def test_idempotency_key_does_not_control_fifo_order(tmp_path: Path) -> None:
    """Caller keys do not replace the sortable UUIDv7 message ID."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    first = queue.put("first", idempotency_key="zzz")
    queue.put("second", idempotency_key="aaa")

    claimed = queue.pop()

    assert claimed is not None
    assert claimed.message_id == first.message_id


def test_prune_releases_a_completed_idempotency_key(tmp_path: Path) -> None:
    """Prune deletes the key reservation with its completed message."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    original = queue.put("first payload", idempotency_key="request-1")
    claim = queue.pop()

    assert claim is not None
    assert queue.done(message_id=claim.message_id, claim_id=claim.claim_id)

    queue.prune()

    replacement = queue.put("new payload", idempotency_key="request-1")

    assert replacement.message_id != original.message_id
    assert replacement.data == "new payload"
    assert queue.stored_count() == 1


def test_prune_can_keep_a_failed_key_reserved(tmp_path: Path) -> None:
    """A retained failed message continues to reserve its idempotency key."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    queue.put("work", idempotency_key="request-1")
    claim = queue.pop()

    assert claim is not None
    assert queue.mark_failed(
        message_id=claim.message_id,
        claim_id=claim.claim_id,
    )

    queue.prune(include_failed=False)

    replay = queue.put("work", idempotency_key="request-1")

    assert replay.status is MessageStatus.FAILED
    assert replay.message_id == claim.message_id

    queue.prune(include_failed=True)

    replacement = queue.put("different work", idempotency_key="request-1")

    assert replacement.message_id != claim.message_id


def test_replay_succeeds_when_the_ready_queue_is_full(tmp_path: Path) -> None:
    """A replay reads an existing row and does not use more queue capacity."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3", maxsize=1)
    original = queue.put("work", idempotency_key="request-1")

    replay = queue.put("work", idempotency_key="request-1")

    assert replay == original
    assert queue.full()

    with pytest.raises(
        sqlite3.IntegrityError,
        match="Max queue length reached: 1",
    ):
        queue.put("other work", idempotency_key="request-2")


def test_idempotent_put_uses_the_callers_transaction(tmp_path: Path) -> None:
    """An idempotent put participates in an active explicit transaction."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")

    with pytest.raises(RuntimeError, match="roll back"):
        with queue.transaction(mode="IMMEDIATE"):
            inserted = queue.put("work", idempotency_key="request-1")
            replay = queue.put("work", idempotency_key="request-1")

            assert replay == inserted
            raise RuntimeError("roll back")

    assert queue.stored_count() == 0

    replacement = queue.put("other work", idempotency_key="request-1")

    assert replacement.data == "other work"


def test_immediate_transactions_serialize_concurrent_keyed_puts(
    tmp_path: Path,
) -> None:
    """Explicit immediate transactions preserve concurrent replay behavior."""
    database_path = tmp_path / "queue.sqlite3"
    first_queue = LiteQueue(filename=database_path, timeout=10)
    second_queue = LiteQueue(filename=database_path, timeout=10)
    barrier = threading.Barrier(2)

    def put_in_transaction(queue: LiteQueue) -> Message:
        """Submit one keyed message in a caller-owned immediate transaction."""
        barrier.wait(timeout=5)
        with queue.transaction(mode="IMMEDIATE"):
            return queue.put("work", idempotency_key="request-1")

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(put_in_transaction, first_queue),
            executor.submit(put_in_transaction, second_queue),
        ]
        messages = [future.result(timeout=15) for future in futures]

    message_ids = {message.message_id for message in messages}

    assert len(message_ids) == 1
    assert first_queue.stored_count() == 1


def test_replay_survives_close_and_reopen(tmp_path: Path) -> None:
    """A caller can retry an ambiguous submission through a new connection."""
    database_path = tmp_path / "queue.sqlite3"
    queue = LiteQueue(filename=database_path)
    original = queue.put("work", idempotency_key="request-1")
    queue.close()

    reopened_queue = LiteQueue(filename=database_path)
    replay = reopened_queue.put("work", idempotency_key="request-1")

    assert replay.message_id == original.message_id
    assert reopened_queue.stored_count() == 1


def test_same_key_is_independent_in_each_queue_file(tmp_path: Path) -> None:
    """The queue file defines the scope of an idempotency key."""
    first_queue = LiteQueue(filename=tmp_path / "first.sqlite3")
    second_queue = LiteQueue(filename=tmp_path / "second.sqlite3")

    first = first_queue.put("first", idempotency_key="request-1")
    second = second_queue.put("second", idempotency_key="request-1")

    assert first.message_id != second.message_id
    assert first.data == "first"
    assert second.data == "second"


def test_concurrent_replays_create_one_message(tmp_path: Path) -> None:
    """Concurrent producers with one key receive the same queue message."""
    database_path = tmp_path / "queue.sqlite3"
    LiteQueue(filename=database_path).close()
    producer_count = 32
    barrier = threading.Barrier(producer_count)

    def put_replay() -> Message:
        """Submit one concurrent replay through an independent connection."""
        queue = LiteQueue(filename=database_path, timeout=10)
        try:
            barrier.wait(timeout=10)
            return queue.put("work", idempotency_key="request-1")
        finally:
            queue.close()

    with ThreadPoolExecutor(max_workers=producer_count) as executor:
        futures = [executor.submit(put_replay) for _ in range(producer_count)]
        messages = [future.result(timeout=20) for future in futures]

    message_ids = {message.message_id for message in messages}
    queue = LiteQueue(filename=database_path)

    assert len(message_ids) == 1
    assert queue.stored_count() == 1


def test_concurrent_conflicting_replays_choose_one_payload(tmp_path: Path) -> None:
    """Concurrent conflicting producers store one payload and reject the other."""
    database_path = tmp_path / "queue.sqlite3"
    LiteQueue(filename=database_path).close()
    payloads = ["amount=10", "amount=1000"] * 8
    barrier = threading.Barrier(len(payloads))

    def put_payload(payload: str) -> Message | IdempotencyConflict:
        """Submit one payload and return either its message or its conflict."""
        queue = LiteQueue(filename=database_path, timeout=10)
        try:
            barrier.wait(timeout=10)
            return queue.put(payload, idempotency_key="charge-123")
        except IdempotencyConflict as error:
            return error
        finally:
            queue.close()

    with ThreadPoolExecutor(max_workers=len(payloads)) as executor:
        futures = [executor.submit(put_payload, payload) for payload in payloads]
        results = [future.result(timeout=20) for future in futures]

    messages = [result for result in results if isinstance(result, Message)]
    conflicts = [
        result for result in results if isinstance(result, IdempotencyConflict)
    ]
    stored_payloads = {message.data for message in messages}
    message_ids = {message.message_id for message in messages}
    queue = LiteQueue(filename=database_path)

    assert len(messages) == 8
    assert len(conflicts) == 8
    assert len(stored_payloads) == 1
    assert len(message_ids) == 1
    assert queue.stored_count() == 1


def test_shared_queue_concurrent_replays_create_one_message(tmp_path: Path) -> None:
    """One shared queue serializes same-key producer retries across threads."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    producer_count = 16
    barrier = threading.Barrier(producer_count)

    def put_replay() -> Message:
        """Submit one retry through the shared queue instance."""
        barrier.wait(timeout=10)
        return queue.put("work", idempotency_key="request-1")

    with ThreadPoolExecutor(max_workers=producer_count) as executor:
        futures = [executor.submit(put_replay) for _ in range(producer_count)]
        messages = [future.result(timeout=20) for future in futures]

    message_ids = {message.message_id for message in messages}

    assert len(message_ids) == 1
    assert queue.stored_count() == 1
    assert queue.get(messages[0].message_id) == messages[0]


def test_shared_queue_concurrent_conflicts_choose_one_payload(
    tmp_path: Path,
) -> None:
    """One shared queue stores one winner for conflicting producer retries."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    payloads = ["amount=10", "amount=1000"] * 8
    barrier = threading.Barrier(len(payloads))

    def put_payload(payload: str) -> Message | IdempotencyConflict:
        """Submit one conflicting payload through the shared queue instance."""
        try:
            barrier.wait(timeout=10)
            return queue.put(payload, idempotency_key="charge-123")
        except IdempotencyConflict as error:
            return error

    with ThreadPoolExecutor(max_workers=len(payloads)) as executor:
        futures = [executor.submit(put_payload, payload) for payload in payloads]
        results = [future.result(timeout=20) for future in futures]

    messages = [result for result in results if isinstance(result, Message)]
    conflicts = [
        result for result in results if isinstance(result, IdempotencyConflict)
    ]
    stored_payloads = {message.data for message in messages}
    message_ids = {message.message_id for message in messages}
    stored_message = queue.get(messages[0].message_id)

    assert len(messages) == 8
    assert len(conflicts) == 8
    assert len(stored_payloads) == 1
    assert len(message_ids) == 1
    assert stored_message is not None
    assert stored_message.data in stored_payloads
    assert queue.stored_count() == 1


def test_legacy_schema_migration_adds_safe_message_fields(tmp_path: Path) -> None:
    """Opening an old fixed schema adds new fields and recovers old locks."""
    database_path = tmp_path / "legacy.sqlite3"
    connection = sqlite3.connect(database_path)
    connection.execute(
        """
        CREATE TABLE "Queue" (
            "data" TEXT NOT NULL,
            "message_id" TEXT NOT NULL,
            "status" INTEGER NOT NULL,
            "in_time" INTEGER NOT NULL,
            "lock_time" INTEGER,
            "done_time" INTEGER
        )
        """
    )
    connection.executemany(
        """
        INSERT INTO "Queue"
            ("data", "message_id", "status", "in_time", "lock_time", "done_time")
        VALUES
            (:data, :message_id, :status, :in_time, :lock_time, :done_time)
        """,
        (
            {
                "data": "ready",
                "message_id": "ready-id",
                "status": MessageStatus.READY.value,
                "in_time": 1_000_000_000,
                "lock_time": None,
                "done_time": None,
            },
            {
                "data": "locked",
                "message_id": "locked-id",
                "status": MessageStatus.LOCKED.value,
                "in_time": 1_000_000_000,
                "lock_time": 2_000_000_000,
                "done_time": None,
            },
            {
                "data": "done",
                "message_id": "done-id",
                "status": MessageStatus.DONE.value,
                "in_time": 1_000_000_000,
                "lock_time": 2_000_000_000,
                "done_time": 3_000_000_000,
            },
            {
                "data": "failed",
                "message_id": "failed-id",
                "status": MessageStatus.FAILED.value,
                "in_time": 1_000_000_000,
                "lock_time": 2_000_000_000,
                "done_time": 4_000_000_000,
            },
        ),
    )
    connection.commit()
    connection.close()

    queue = LiteQueue(filename=database_path)
    ready = queue.get("ready-id")
    recovered = queue.get("locked-id")
    completed = queue.get("done-id")
    failed = queue.get("failed-id")
    column_rows = queue.conn.execute('PRAGMA table_info("Queue")').fetchall()
    column_names = {row["name"] for row in column_rows}

    assert ready is not None
    assert ready.idempotency_key is None
    assert ready.claim_id == 0
    assert ready.in_time == 1_000_000_000
    assert recovered is not None
    assert recovered.status is MessageStatus.READY
    assert recovered.lock_time is None
    assert recovered.done_time is None
    assert recovered.claim_id == 0
    assert completed is not None
    assert completed.status is MessageStatus.DONE
    assert completed.in_time == 1_000_000_000
    assert completed.lock_time == 2_000_000_000
    assert completed.done_time == 3_000_000_000
    assert completed.claim_id == 0
    assert failed is not None
    assert failed.status is MessageStatus.FAILED
    assert failed.in_time == 1_000_000_000
    assert failed.lock_time == 2_000_000_000
    assert failed.done_time == 4_000_000_000
    assert failed.claim_id == 1
    assert {"idempotency_key", "claim_id"} <= column_names
    assert queue.conn.execute("PRAGMA user_version").fetchone()[0] == 3
    assert queue.qsize() == 2
    assert queue.done_count() == 1
    assert queue.failed_count() == 1

    assert queue.retry(message_id=failed.message_id, claim_id=failed.claim_id)
    retried_failure = queue.get(failed.message_id)

    assert retried_failure is not None
    assert retried_failure.status is MessageStatus.READY
    assert retried_failure.lock_time is None
    assert retried_failure.done_time is None
    assert retried_failure.claim_id == 1

    idempotent = queue.put("new work", idempotency_key="request-1")
    replay = queue.put("new work", idempotency_key="request-1")

    assert replay.message_id == idempotent.message_id
