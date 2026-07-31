import json
import math
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

from litequeue import LiteQueue
from litequeue import Message
from litequeue import MessageStatus
from litequeue.migrations import get_user_version

VERSION_012_SETUP_SCRIPT = '''\
#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "litequeue==0.12",
# ]
# ///

import argparse
import importlib.metadata
import json
import sys
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path

from litequeue import LiteQueue


@dataclass(frozen=True, slots=True)
class Version012Fixture:
    """Describe messages created by the published LiteQueue 0.12 package."""

    source_version: str
    ready_message_id: str
    ready_in_time_ns: int
    locked_message_id: str
    locked_in_time_ns: int
    completed_message_id: str
    completed_in_time_ns: int
    completed_lock_time_ns: int
    completed_done_time_ns: int
    failed_message_id: str
    failed_in_time_ns: int
    failed_lock_time_ns: int
    failed_done_time_ns: int


def parse_args() -> argparse.Namespace:
    """Parse the database and manifest output paths."""
    parser = argparse.ArgumentParser()
    parser.add_argument("database_path", type=Path)
    parser.add_argument("manifest_path", type=Path)
    return parser.parse_args()


def main(*, database_path: Path, manifest_path: Path) -> int:
    """Create a version 0.12 queue with messages in each useful state."""
    source_version = importlib.metadata.version("litequeue")
    if source_version != "0.12":
        raise RuntimeError(f"Expected litequeue 0.12, found {source_version}")

    queue = LiteQueue(filename=database_path, maxsize=10)
    try:
        completed_insert = queue.put("completed by version 0.12")
        first_completed_claim = queue.pop()
        assert first_completed_claim is not None
        assert first_completed_claim.message_id == completed_insert.message_id
        assert queue.retry(completed_insert.message_id)

        second_completed_claim = queue.pop()
        assert second_completed_claim is not None
        assert second_completed_claim.message_id == completed_insert.message_id
        assert queue.done(completed_insert.message_id)
        completed = queue.get(completed_insert.message_id)
        assert completed is not None
        assert completed.lock_time is not None
        assert completed.done_time is not None

        failed_insert = queue.put("failed by version 0.12")
        failed_claim = queue.pop()
        assert failed_claim is not None
        assert failed_claim.message_id == failed_insert.message_id
        assert queue.mark_failed(failed_insert.message_id)
        failed = queue.get(failed_insert.message_id)
        assert failed is not None
        assert failed.lock_time is not None
        assert failed.done_time is not None

        locked_insert = queue.put("locked by version 0.12")
        locked = queue.pop()
        assert locked is not None
        assert locked.message_id == locked_insert.message_id
        assert locked.lock_time is not None

        ready = queue.put("ready from version 0.12")

        fixture = Version012Fixture(
            source_version=source_version,
            ready_message_id=ready.message_id,
            ready_in_time_ns=ready.in_time,
            locked_message_id=locked.message_id,
            locked_in_time_ns=locked.in_time,
            completed_message_id=completed.message_id,
            completed_in_time_ns=completed.in_time,
            completed_lock_time_ns=completed.lock_time,
            completed_done_time_ns=completed.done_time,
            failed_message_id=failed.message_id,
            failed_in_time_ns=failed.in_time,
            failed_lock_time_ns=failed.lock_time,
            failed_done_time_ns=failed.done_time,
        )
    finally:
        queue.close()

    fixture_json = json.dumps(asdict(fixture), sort_keys=True)
    manifest_path.write_text(fixture_json, encoding="utf-8")
    return 0


if __name__ == "__main__":
    arguments = parse_args()
    sys.exit(
        main(
            database_path=arguments.database_path,
            manifest_path=arguments.manifest_path,
        )
    )
'''

VERSION_013_SETUP_SCRIPT = '''\
#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "litequeue==0.13",
# ]
# ///

import argparse
import importlib.metadata
import json
import sys
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path

from litequeue import LiteQueue


@dataclass(frozen=True, slots=True)
class Version013Fixture:
    """Describe messages created by the published LiteQueue 0.13 package."""

    source_version: str
    ready_message_id: str
    ready_in_time_seconds: float
    locked_message_id: str
    locked_in_time_seconds: float
    locked_lock_time_seconds: float
    locked_claim_id: int
    completed_message_id: str
    completed_in_time_seconds: float
    completed_lock_time_seconds: float
    completed_done_time_seconds: float
    completed_claim_id: int


def parse_args() -> argparse.Namespace:
    """Parse the database and manifest output paths."""
    parser = argparse.ArgumentParser()
    parser.add_argument("database_path", type=Path)
    parser.add_argument("manifest_path", type=Path)
    return parser.parse_args()


def main(*, database_path: Path, manifest_path: Path) -> int:
    """Create a version 0.13 queue with seconds-based timestamps."""
    source_version = importlib.metadata.version("litequeue")
    if source_version != "0.13":
        raise RuntimeError(f"Expected litequeue 0.13, found {source_version}")

    queue = LiteQueue(filename=database_path, maxsize=10)
    try:
        schema_version = queue.conn.execute("PRAGMA user_version").fetchone()[0]
        if schema_version != 2:
            raise RuntimeError(f"Expected schema version 2, found {schema_version}")

        completed_insert = queue.put(
            "completed by version 0.13",
            idempotency_key="completed-013",
        )
        completed_claim = queue.pop()
        assert completed_claim is not None
        assert completed_claim.message_id == completed_insert.message_id
        assert queue.done(
            message_id=completed_claim.message_id,
            claim_id=completed_claim.claim_id,
        )
        completed = queue.get(completed_claim.message_id)
        assert completed is not None
        assert completed.lock_time is not None
        assert completed.done_time is not None

        completed_replay = queue.put(
            "completed by version 0.13",
            idempotency_key="completed-013",
        )
        assert completed_replay == completed

        locked_insert = queue.put("locked by version 0.13")
        locked = queue.pop()
        assert locked is not None
        assert locked.message_id == locked_insert.message_id
        assert locked.lock_time is not None

        ready = queue.put(
            "ready from version 0.13",
            idempotency_key="ready-013",
        )

        fixture = Version013Fixture(
            source_version=source_version,
            ready_message_id=ready.message_id,
            ready_in_time_seconds=ready.in_time,
            locked_message_id=locked.message_id,
            locked_in_time_seconds=locked.in_time,
            locked_lock_time_seconds=locked.lock_time,
            locked_claim_id=locked.claim_id,
            completed_message_id=completed.message_id,
            completed_in_time_seconds=completed.in_time,
            completed_lock_time_seconds=completed.lock_time,
            completed_done_time_seconds=completed.done_time,
            completed_claim_id=completed.claim_id,
        )
    finally:
        queue.close()

    fixture_json = json.dumps(asdict(fixture), sort_keys=True)
    manifest_path.write_text(fixture_json, encoding="utf-8")
    return 0


if __name__ == "__main__":
    arguments = parse_args()
    sys.exit(
        main(
            database_path=arguments.database_path,
            manifest_path=arguments.manifest_path,
        )
    )
'''


@dataclass(frozen=True, slots=True)
class Version012Fixture:
    """Describe the version 0.12 database contents used by the migration test."""

    source_version: str
    ready_message_id: str
    ready_in_time_ns: int
    locked_message_id: str
    locked_in_time_ns: int
    completed_message_id: str
    completed_in_time_ns: int
    completed_lock_time_ns: int
    completed_done_time_ns: int
    failed_message_id: str
    failed_in_time_ns: int
    failed_lock_time_ns: int
    failed_done_time_ns: int


@dataclass(frozen=True, slots=True)
class Version013Fixture:
    """Describe the version 0.13 database contents used by the migration test."""

    source_version: str
    ready_message_id: str
    ready_in_time_seconds: float
    locked_message_id: str
    locked_in_time_seconds: float
    locked_lock_time_seconds: float
    locked_claim_id: int
    completed_message_id: str
    completed_in_time_seconds: float
    completed_lock_time_seconds: float
    completed_done_time_seconds: float
    completed_claim_id: int


@dataclass(frozen=True, slots=True)
class ReleaseDatabase:
    """Contain paths produced by one isolated release setup script."""

    database_path: Path
    manifest_path: Path


def _read_version_012_fixture(manifest_path: Path) -> Version012Fixture:
    """Load metadata written by the isolated version 0.12 process."""
    manifest_json = manifest_path.read_text(encoding="utf-8")
    manifest = json.loads(manifest_json)
    return Version012Fixture(**manifest)


def _read_version_013_fixture(manifest_path: Path) -> Version013Fixture:
    """Load metadata written by the isolated version 0.13 process."""
    manifest_json = manifest_path.read_text(encoding="utf-8")
    manifest = json.loads(manifest_json)
    return Version013Fixture(**manifest)


def _create_release_database(
    *,
    tmp_path: Path,
    source_version: str,
    setup_script: str,
) -> ReleaseDatabase:
    """Run one published release in isolation and return its output paths."""
    version_slug = source_version.replace(".", "_")
    script_path = tmp_path / f"create_version_{version_slug}_queue.py"
    database_path = tmp_path / f"version_{version_slug}_queue.sqlite3"
    manifest_path = tmp_path / f"version_{version_slug}_fixture.json"
    script_path.write_text(setup_script, encoding="utf-8")

    command = [
        "uv",
        "run",
        "--isolated",
        "--no-project",
        "--script",
        str(script_path),
        str(database_path),
        str(manifest_path),
    ]
    setup_environment = os.environ.copy()
    setup_environment.pop("PYTHONPATH", None)
    setup_result = subprocess.run(
        args=command,
        cwd=tmp_path,
        capture_output=True,
        check=False,
        env=setup_environment,
        text=True,
    )

    assert setup_result.returncode == 0, setup_result.stderr
    return ReleaseDatabase(
        database_path=database_path,
        manifest_path=manifest_path,
    )


def _assert_converted_nanoseconds(timestamp: int | None, seconds: float) -> None:
    """Assert that a seconds timestamp became integer nanoseconds."""
    assert timestamp is not None
    assert isinstance(timestamp, int)
    expected_nanoseconds = int(round(seconds * 1_000_000_000))
    assert timestamp == expected_nanoseconds


def _assert_restored_nanoseconds(timestamp: int | None, nanoseconds: int) -> None:
    """Assert that migrations 2 and 3 restore an old nanosecond timestamp."""
    assert timestamp is not None
    assert isinstance(timestamp, int)
    max_round_trip_loss = int(math.ulp(float(nanoseconds)))
    round_trip_loss = abs(timestamp - nanoseconds)
    assert round_trip_loss <= max_round_trip_loss


def _assert_no_idempotency_key(*messages: Message) -> None:
    """Assert that historical messages did not acquire producer keys."""
    for message in messages:
        assert message.idempotency_key is None


def test_checkout_migrates_a_queue_created_by_version_012(tmp_path: Path) -> None:
    """The checkout upgrades real version 0.12 data and preserves its states."""
    release_database = _create_release_database(
        tmp_path=tmp_path,
        source_version="0.12",
        setup_script=VERSION_012_SETUP_SCRIPT,
    )
    fixture = _read_version_012_fixture(release_database.manifest_path)
    assert fixture.source_version == "0.12"

    queue = LiteQueue(filename=release_database.database_path)
    try:
        ready = queue.get(fixture.ready_message_id)
        recovered_locked = queue.get(fixture.locked_message_id)
        completed = queue.get(fixture.completed_message_id)
        failed = queue.get(fixture.failed_message_id)

        assert get_user_version(queue.conn) == 3
        assert queue.maxsize == 10
        assert ready is not None
        assert recovered_locked is not None
        assert completed is not None
        assert failed is not None
        _assert_no_idempotency_key(ready, recovered_locked, completed, failed)

        assert ready.status is MessageStatus.READY
        assert ready.claim_id == 0
        _assert_restored_nanoseconds(ready.in_time, fixture.ready_in_time_ns)

        assert recovered_locked.status is MessageStatus.READY
        assert recovered_locked.claim_id == 0
        assert recovered_locked.lock_time is None
        assert recovered_locked.done_time is None
        _assert_restored_nanoseconds(
            recovered_locked.in_time,
            fixture.locked_in_time_ns,
        )

        assert completed.status is MessageStatus.DONE
        assert completed.claim_id == 0
        _assert_restored_nanoseconds(
            completed.in_time,
            fixture.completed_in_time_ns,
        )
        _assert_restored_nanoseconds(
            completed.lock_time,
            fixture.completed_lock_time_ns,
        )
        _assert_restored_nanoseconds(
            completed.done_time,
            fixture.completed_done_time_ns,
        )

        assert failed.status is MessageStatus.FAILED
        assert failed.claim_id == 1
        _assert_restored_nanoseconds(failed.in_time, fixture.failed_in_time_ns)
        _assert_restored_nanoseconds(
            failed.lock_time,
            fixture.failed_lock_time_ns,
        )
        _assert_restored_nanoseconds(
            failed.done_time,
            fixture.failed_done_time_ns,
        )

        keyed_message = queue.put(
            "created after migration",
            idempotency_key="post-migration-message",
        )
        keyed_replay = queue.put(
            "created after migration",
            idempotency_key="post-migration-message",
        )

        assert keyed_replay == keyed_message
        assert keyed_message.idempotency_key == "post-migration-message"
        assert keyed_message.claim_id == 0

        locked_claim = queue.pop()
        assert locked_claim is not None
        assert locked_claim.message_id == recovered_locked.message_id
        assert locked_claim.claim_id == 1
        assert queue.done(
            message_id=locked_claim.message_id,
            claim_id=locked_claim.claim_id,
        )

        ready_claim = queue.pop()
        assert ready_claim is not None
        assert ready_claim.message_id == ready.message_id
        assert ready_claim.claim_id == 1
        assert queue.done(
            message_id=ready_claim.message_id,
            claim_id=ready_claim.claim_id,
        )

        assert queue.retry(
            message_id=failed.message_id,
            claim_id=failed.claim_id,
        )
        failed_claim = queue.pop()
        assert failed_claim is not None
        assert failed_claim.message_id == failed.message_id
        assert failed_claim.claim_id == 2
        assert queue.done(
            message_id=failed_claim.message_id,
            claim_id=failed_claim.claim_id,
        )

        keyed_claim = queue.pop()
        assert keyed_claim is not None
        assert keyed_claim.message_id == keyed_message.message_id
        assert keyed_claim.idempotency_key == "post-migration-message"
        assert keyed_claim.claim_id == 1
        assert queue.done(
            message_id=keyed_claim.message_id,
            claim_id=keyed_claim.claim_id,
        )
    finally:
        queue.close()


def test_checkout_migrates_a_queue_created_by_version_013(tmp_path: Path) -> None:
    """The checkout converts real version 0.13 timestamps to nanoseconds."""
    release_database = _create_release_database(
        tmp_path=tmp_path,
        source_version="0.13",
        setup_script=VERSION_013_SETUP_SCRIPT,
    )
    fixture = _read_version_013_fixture(release_database.manifest_path)
    assert fixture.source_version == "0.13"

    queue = LiteQueue(filename=release_database.database_path)
    try:
        ready = queue.get(fixture.ready_message_id)
        locked = queue.get(fixture.locked_message_id)
        completed = queue.get(fixture.completed_message_id)

        assert get_user_version(queue.conn) == 3
        assert queue.maxsize == 10
        assert ready is not None
        assert locked is not None
        assert completed is not None

        assert ready.status is MessageStatus.READY
        assert ready.idempotency_key == "ready-013"
        assert ready.claim_id == 0
        _assert_converted_nanoseconds(
            ready.in_time,
            fixture.ready_in_time_seconds,
        )

        assert locked.status is MessageStatus.LOCKED
        assert locked.idempotency_key is None
        assert locked.claim_id == fixture.locked_claim_id
        _assert_converted_nanoseconds(
            locked.in_time,
            fixture.locked_in_time_seconds,
        )
        _assert_converted_nanoseconds(
            locked.lock_time,
            fixture.locked_lock_time_seconds,
        )

        assert completed.status is MessageStatus.DONE
        assert completed.idempotency_key == "completed-013"
        assert completed.claim_id == fixture.completed_claim_id
        _assert_converted_nanoseconds(
            completed.in_time,
            fixture.completed_in_time_seconds,
        )
        _assert_converted_nanoseconds(
            completed.lock_time,
            fixture.completed_lock_time_seconds,
        )
        _assert_converted_nanoseconds(
            completed.done_time,
            fixture.completed_done_time_seconds,
        )

        completed_replay = queue.put(
            "completed by version 0.13",
            idempotency_key="completed-013",
        )
        ready_replay = queue.put(
            "ready from version 0.13",
            idempotency_key="ready-013",
        )

        assert completed_replay == completed
        assert ready_replay == ready

        new_message = queue.put(
            "created after migration",
            idempotency_key="post-migration-013",
        )

        assert isinstance(new_message.in_time, int)
        assert new_message.idempotency_key == "post-migration-013"
        assert new_message.claim_id == 0

        assert queue.retry(
            message_id=locked.message_id,
            claim_id=locked.claim_id,
        )
        retried_claim = queue.pop()
        assert retried_claim is not None
        assert retried_claim.message_id == locked.message_id
        assert retried_claim.claim_id == locked.claim_id + 1
        assert queue.done(
            message_id=retried_claim.message_id,
            claim_id=retried_claim.claim_id,
        )

        ready_claim = queue.pop()
        assert ready_claim is not None
        assert ready_claim.message_id == ready.message_id
        assert ready_claim.idempotency_key == "ready-013"
        assert ready_claim.claim_id == 1
        assert queue.done(
            message_id=ready_claim.message_id,
            claim_id=ready_claim.claim_id,
        )

        new_claim = queue.pop()
        assert new_claim is not None
        assert new_claim.message_id == new_message.message_id
        assert new_claim.idempotency_key == "post-migration-013"
        assert new_claim.claim_id == 1
        assert queue.done(
            message_id=new_claim.message_id,
            claim_id=new_claim.claim_id,
        )
    finally:
        queue.close()
