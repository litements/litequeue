import math
import sqlite3
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import litequeue
from litequeue import LiteQueue
from litequeue import MessageStatus


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
def claim_queue(request: pytest.FixtureRequest, tmp_path: Path) -> LiteQueue:
    """Return a queue that uses one supported claim implementation."""
    queue = LiteQueue(filename=tmp_path / f"{request.param}.sqlite3")
    queue.pop = getattr(queue, request.param)
    return queue


def test_pop_assigns_a_new_claim_id_for_each_delivery(tmp_path: Path) -> None:
    """Each delivery receives a larger claim ID for the same message."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    inserted = queue.put("work")

    first_claim = queue.pop()

    assert first_claim is not None
    assert inserted.claim_id == 0
    assert first_claim.claim_id == 1
    assert queue.retry(
        message_id=first_claim.message_id,
        claim_id=first_claim.claim_id,
    )

    second_claim = queue.pop()

    assert second_claim is not None
    assert second_claim.message_id == first_claim.message_id
    assert second_claim.claim_id == 2
    assert second_claim.status is MessageStatus.LOCKED


def test_stale_worker_cannot_complete_a_new_claim(claim_queue: LiteQueue) -> None:
    """An old worker cannot complete a later delivery of the same message."""
    queue = claim_queue
    queue.put("work")
    stale_claim = queue.pop()

    assert stale_claim is not None
    assert queue.retry(
        message_id=stale_claim.message_id,
        claim_id=stale_claim.claim_id,
    )

    current_claim = queue.pop()

    assert current_claim is not None
    assert current_claim.claim_id > stale_claim.claim_id
    assert not queue.done(
        message_id=stale_claim.message_id,
        claim_id=stale_claim.claim_id,
    )

    stored_message = queue.get(current_claim.message_id)

    assert stored_message is not None
    assert stored_message.status is MessageStatus.LOCKED
    assert stored_message.claim_id == current_claim.claim_id
    assert queue.done(
        message_id=current_claim.message_id,
        claim_id=current_claim.claim_id,
    )


def test_expired_claim_recovery_fences_the_old_worker(
    claim_queue: LiteQueue,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expired-claim recovery prevents a late acknowledgement from an old worker."""
    queue = claim_queue
    current_time = [1.0]
    monkeypatch.setattr(litequeue, "time_seconds", lambda: current_time[0])
    queue.put("work")
    stale_claim = queue.pop()

    assert stale_claim is not None

    current_time[0] += 31

    assert queue.retry_expired(threshold_seconds=30) == 1

    current_claim = queue.pop()

    assert current_claim is not None
    assert current_claim.claim_id == stale_claim.claim_id + 1
    assert not queue.done(
        message_id=stale_claim.message_id,
        claim_id=stale_claim.claim_id,
    )
    assert queue.done(
        message_id=current_claim.message_id,
        claim_id=current_claim.claim_id,
    )


@pytest.mark.parametrize("operation_name", ("done", "mark_failed", "retry"))
def test_stale_claim_cannot_apply_a_state_transition(
    claim_queue: LiteQueue,
    operation_name: str,
) -> None:
    """Each claim-owned transition rejects a claim ID from an old delivery."""
    queue = claim_queue
    queue.put("work")
    stale_claim = queue.pop()

    assert stale_claim is not None
    assert queue.retry(
        message_id=stale_claim.message_id,
        claim_id=stale_claim.claim_id,
    )

    current_claim = queue.pop()
    operation = getattr(queue, operation_name)

    assert current_claim is not None
    assert not operation(
        message_id=stale_claim.message_id,
        claim_id=stale_claim.claim_id,
    )

    stored_message = queue.get(current_claim.message_id)

    assert stored_message is not None
    assert stored_message.status is MessageStatus.LOCKED
    assert stored_message.claim_id == current_claim.claim_id


def test_claim_owned_transitions_reject_invalid_source_states(tmp_path: Path) -> None:
    """A matching message ID cannot bypass the required source state."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    ready = queue.put("ready")

    assert not queue.done(message_id=ready.message_id, claim_id=1)
    assert not queue.mark_failed(message_id=ready.message_id, claim_id=1)
    assert not queue.retry(message_id=ready.message_id, claim_id=1)

    locked = queue.pop()

    assert locked is not None
    assert queue.done(message_id=locked.message_id, claim_id=locked.claim_id)
    assert not queue.done(message_id=locked.message_id, claim_id=locked.claim_id)
    assert not queue.mark_failed(
        message_id=locked.message_id,
        claim_id=locked.claim_id,
    )
    assert not queue.retry(message_id=locked.message_id, claim_id=locked.claim_id)


def test_failed_claim_can_return_to_ready_once(tmp_path: Path) -> None:
    """A failed claim can return to ready only with its current claim ID."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    queue.put("work")
    claim = queue.pop()

    assert claim is not None
    assert queue.mark_failed(message_id=claim.message_id, claim_id=claim.claim_id)
    assert queue.retry(message_id=claim.message_id, claim_id=claim.claim_id)
    assert not queue.retry(message_id=claim.message_id, claim_id=claim.claim_id)

    stored_message = queue.get(claim.message_id)

    assert stored_message is not None
    assert stored_message.status is MessageStatus.READY
    assert stored_message.lock_time is None
    assert stored_message.done_time is None
    assert stored_message.claim_id == claim.claim_id


@pytest.mark.parametrize("operation_name", ("done", "mark_failed", "retry"))
@pytest.mark.parametrize("claim_id", (None, True, 1.0, "1", b"1", object()))
def test_claim_owned_transitions_reject_invalid_claim_id_types(
    tmp_path: Path,
    operation_name: str,
    claim_id: object,
) -> None:
    """Claim-owned transitions reject values that are not integers."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    message = queue.put("work")
    operation = getattr(queue, operation_name)

    with pytest.raises(TypeError, match="claim_id must be an integer"):
        operation(message_id=message.message_id, claim_id=claim_id)


@pytest.mark.parametrize("operation_name", ("done", "mark_failed", "retry"))
@pytest.mark.parametrize("claim_id", (-1, 0, 2**63))
def test_claim_owned_transitions_reject_invalid_claim_id_values(
    tmp_path: Path,
    operation_name: str,
    claim_id: int,
) -> None:
    """Claim-owned transitions reject integers outside the SQLite range."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    message = queue.put("work")
    operation = getattr(queue, operation_name)

    with pytest.raises(
        ValueError,
        match="claim_id must be between 1 and 9223372036854775807",
    ):
        operation(message_id=message.message_id, claim_id=claim_id)


def test_retry_expired_changes_only_old_locked_messages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expired-claim recovery leaves fresh claims and other states unchanged."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    current_time = [1.0]
    monkeypatch.setattr(litequeue, "time_seconds", lambda: current_time[0])

    queue.put("old claim")
    old_claim = queue.pop()

    current_time[0] += 5
    queue.put("completed")
    completed_claim = queue.pop()
    queue.put("failed")
    failed_claim = queue.pop()

    assert completed_claim is not None
    assert failed_claim is not None
    assert queue.done(
        message_id=completed_claim.message_id,
        claim_id=completed_claim.claim_id,
    )
    assert queue.mark_failed(
        message_id=failed_claim.message_id,
        claim_id=failed_claim.claim_id,
    )

    completed_before = queue.get(completed_claim.message_id)
    failed_before = queue.get(failed_claim.message_id)

    current_time[0] += 15
    queue.put("fresh claim")
    fresh_claim = queue.pop()
    queue.put("ready")

    current_time[0] += 15

    assert old_claim is not None
    assert fresh_claim is not None
    assert queue.retry_expired(threshold_seconds=30) == 1

    recovered = queue.get(old_claim.message_id)
    unchanged = queue.get(fresh_claim.message_id)

    assert recovered is not None
    assert recovered.status is MessageStatus.READY
    assert recovered.lock_time is None
    assert recovered.done_time is None
    assert unchanged is not None
    assert unchanged.status is MessageStatus.LOCKED
    assert unchanged.lock_time == fresh_claim.lock_time
    assert queue.get(completed_claim.message_id) == completed_before
    assert queue.get(failed_claim.message_id) == failed_before


@pytest.mark.parametrize("threshold_seconds", (None, True, "1", object()))
def test_retry_expired_rejects_invalid_threshold_types(
    tmp_path: Path,
    threshold_seconds: object,
) -> None:
    """Expired-claim recovery rejects nonnumeric threshold values."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")

    with pytest.raises(TypeError, match="threshold_seconds must be a number"):
        queue.retry_expired(
            threshold_seconds=threshold_seconds,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "threshold_seconds",
    (-1, -0.1, math.nan, math.inf, -math.inf),
)
def test_retry_expired_rejects_invalid_threshold_values(
    tmp_path: Path,
    threshold_seconds: float,
) -> None:
    """Expired-claim recovery rejects negative and nonfinite thresholds."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")

    with pytest.raises(
        ValueError,
        match="threshold_seconds must be finite and zero or positive",
    ):
        queue.retry_expired(threshold_seconds=threshold_seconds)


def test_concurrent_expired_recovery_retries_each_claim_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent recovery calls cannot retry one claim more than once."""
    database_path = tmp_path / "queue.sqlite3"
    first_queue = LiteQueue(filename=database_path)
    second_queue = LiteQueue(filename=database_path)
    current_time = [1.0]
    monkeypatch.setattr(litequeue, "time_seconds", lambda: current_time[0])
    first_queue.put("work")
    claim = first_queue.pop()
    current_time[0] += 2
    barrier = threading.Barrier(2)

    def recover(queue: LiteQueue) -> int:
        """Start recovery with the other worker and return its update count."""
        barrier.wait(timeout=5)
        return queue.retry_expired(threshold_seconds=1)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(recover, first_queue),
            executor.submit(recover, second_queue),
        ]
        results = [future.result(timeout=10) for future in futures]

    assert claim is not None
    assert sorted(results) == [0, 1]

    recovered = first_queue.get(claim.message_id)

    assert recovered is not None
    assert recovered.status is MessageStatus.READY
    assert recovered.claim_id == claim.claim_id


def test_completion_and_expired_recovery_have_one_winner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent completion and recovery cannot both change one claim."""
    database_path = tmp_path / "queue.sqlite3"
    worker_queue = LiteQueue(filename=database_path)
    recovery_queue = LiteQueue(filename=database_path)
    current_time = [1.0]
    monkeypatch.setattr(litequeue, "time_seconds", lambda: current_time[0])
    worker_queue.put("work")
    claim = worker_queue.pop()
    current_time[0] += 2
    barrier = threading.Barrier(2)

    assert claim is not None

    def complete() -> bool:
        """Try to complete the current claim."""
        barrier.wait(timeout=5)
        return worker_queue.done(
            message_id=claim.message_id,
            claim_id=claim.claim_id,
        )

    def recover() -> int:
        """Try to recover the same expired claim."""
        barrier.wait(timeout=5)
        return recovery_queue.retry_expired(threshold_seconds=1)

    with ThreadPoolExecutor(max_workers=2) as executor:
        completion_future = executor.submit(complete)
        recovery_future = executor.submit(recover)
        completion_result = completion_future.result(timeout=10)
        recovery_result = recovery_future.result(timeout=10)

    stored_message = worker_queue.get(claim.message_id)

    assert stored_message is not None
    valid_outcomes = {
        (True, 0, MessageStatus.DONE),
        (False, 1, MessageStatus.READY),
    }
    actual_outcome = (
        completion_result,
        recovery_result,
        stored_message.status,
    )
    assert actual_outcome in valid_outcomes


def test_retry_expired_uses_a_strict_age_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A claim expires only after its age is greater than the threshold."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    current_time = [1.0]
    monkeypatch.setattr(litequeue, "time_seconds", lambda: current_time[0])
    queue.put("work")
    claim = queue.pop()
    current_time[0] += 30

    assert claim is not None
    assert queue.retry_expired(threshold_seconds=30) == 0

    current_time[0] += 0.001

    assert queue.retry_expired(threshold_seconds=30) == 1


def test_retry_expired_rejects_a_seconds_threshold_too_large_for_float(
    tmp_path: Path,
) -> None:
    """A duration must fit the floating-point seconds representation."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")

    with pytest.raises(ValueError, match="threshold_seconds is too large"):
        queue.retry_expired(threshold_seconds=10**400)


def test_retry_expired_accepts_a_large_finite_seconds_threshold(
    tmp_path: Path,
) -> None:
    """A large finite seconds value remains valid."""
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")

    assert queue.retry_expired(threshold_seconds=sys.float_info.max) == 0


def test_claim_id_sequence_survives_close_and_reopen(tmp_path: Path) -> None:
    """A reopened queue continues the per-message claim ID sequence."""
    database_path = tmp_path / "queue.sqlite3"
    queue = LiteQueue(filename=database_path)
    queue.put("work")
    first_claim = queue.pop()

    assert first_claim is not None
    assert queue.retry(
        message_id=first_claim.message_id,
        claim_id=first_claim.claim_id,
    )

    queue.close()

    reopened_queue = LiteQueue(filename=database_path)
    second_claim = reopened_queue.pop()

    assert second_claim is not None
    assert second_claim.message_id == first_claim.message_id
    assert second_claim.claim_id == first_claim.claim_id + 1
