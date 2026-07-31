import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from litequeue import LiteQueue
from litequeue.migrations import MIGRATIONS
from litequeue.migrations import apply_migrations
from litequeue.migrations import get_user_version


def _connect(database_path: Path) -> sqlite3.Connection:
    """Open an autocommit connection for migration tests."""
    connection = sqlite3.connect(
        database=database_path,
        isolation_level=None,
        timeout=10,
    )
    connection.row_factory = sqlite3.Row
    return connection


def _create_version_012_schema(connection: sqlite3.Connection) -> None:
    """Create the fixed schema shipped by LiteQueue 0.12."""
    connection.execute(
        """
        CREATE TABLE "Queue"
        (
          data       TEXT NOT NULL
          , message_id TEXT NOT NULL
          , status     INTEGER NOT NULL
          , in_time    INTEGER NOT NULL
          , lock_time  INTEGER
          , done_time  INTEGER
        )
        """
    )
    connection.execute(
        'CREATE UNIQUE INDEX "Queue_message_id_unique_idx" ON "Queue"(message_id)'
    )
    connection.execute(
        'CREATE INDEX "Queue_status_message_id_idx" ON "Queue"(status, message_id)'
    )


def _queue_columns(connection: sqlite3.Connection) -> list[str]:
    """Return Queue column names in storage order."""
    rows = connection.execute('PRAGMA table_info("Queue")').fetchall()
    return [str(row["name"]) for row in rows]


def _schema_sql(connection: sqlite3.Connection) -> list[str]:
    """Return the stored Queue schema objects in name order."""
    rows = connection.execute(
        """
        SELECT "sql"
        FROM "sqlite_schema"
        WHERE "tbl_name" = :table_name
          AND "sql" IS NOT NULL
        ORDER BY "name"
        """,
        {"table_name": "Queue"},
    ).fetchall()
    return [str(row["sql"]) for row in rows]


def test_migration_1_reproduces_the_version_012_schema(tmp_path: Path) -> None:
    """Migration 1 creates only the table and indexes from version 0.12."""
    connection = _connect(tmp_path / "queue.sqlite3")

    apply_migrations(
        connection,
        {1: MIGRATIONS[1]},
    )

    index_rows = connection.execute(
        """
        SELECT "name"
        FROM "sqlite_schema"
        WHERE "type" = :object_type
        ORDER BY "name"
        """,
        {"object_type": "index"},
    ).fetchall()
    index_names = [str(row["name"]) for row in index_rows]
    column_rows = connection.execute('PRAGMA table_info("Queue")').fetchall()
    column_schema = [
        (
            row["name"],
            row["type"],
            bool(row["notnull"]),
            row["dflt_value"],
            bool(row["pk"]),
        )
        for row in column_rows
    ]

    assert get_user_version(connection) == 1
    assert column_schema == [
        ("data", "TEXT", True, None, False),
        ("message_id", "TEXT", True, None, False),
        ("status", "INTEGER", True, None, False),
        ("in_time", "INTEGER", True, None, False),
        ("lock_time", "INTEGER", False, None, False),
        ("done_time", "INTEGER", False, None, False),
    ]
    assert index_names == [
        "Queue_message_id_unique_idx",
        "Queue_status_message_id_idx",
    ]


def test_migration_1_adopts_an_existing_version_012_database(
    tmp_path: Path,
) -> None:
    """Migration 1 leaves an existing version 0.12 schema and data unchanged."""
    connection = _connect(tmp_path / "queue.sqlite3")
    _create_version_012_schema(connection)
    connection.execute(
        """
        INSERT INTO "Queue"
          ("data", "message_id", "status", "in_time", "lock_time", "done_time")
        VALUES
          (:data, :message_id, :status, :in_time, :lock_time, :done_time)
        """,
        {
            "data": "preserved",
            "message_id": "message-1",
            "status": 2,
            "in_time": 10,
            "lock_time": 20,
            "done_time": 30,
        },
    )
    schema_before = _schema_sql(connection)

    apply_migrations(
        connection,
        {1: MIGRATIONS[1]},
    )
    apply_migrations(
        connection,
        {1: MIGRATIONS[1]},
    )

    stored_row = connection.execute(
        """
        SELECT
          "data", "message_id", "status", "in_time", "lock_time", "done_time"
        FROM "Queue"
        WHERE "message_id" = :message_id
        """,
        {"message_id": "message-1"},
    ).fetchone()

    assert get_user_version(connection) == 1
    assert _schema_sql(connection) == schema_before
    assert stored_row is not None
    assert tuple(stored_row) == ("preserved", "message-1", 2, 10, 20, 30)


def test_migrations_are_applied_in_version_order(tmp_path: Path) -> None:
    """A reverse-insertion dictionary still applies migrations 1 and 2."""
    connection = _connect(tmp_path / "queue.sqlite3")
    reverse_order = {
        2: MIGRATIONS[2],
        1: MIGRATIONS[1],
    }

    apply_migrations(connection, reverse_order)

    assert get_user_version(connection) == 2
    assert _queue_columns(connection) == [
        "data",
        "message_id",
        "status",
        "in_time",
        "lock_time",
        "done_time",
        "idempotency_key",
        "claim_id",
    ]


def test_migration_2_upgrades_a_database_at_version_1(tmp_path: Path) -> None:
    """A database marked as version 1 receives only the feature migration."""
    database_path = tmp_path / "queue.sqlite3"
    connection = _connect(database_path)
    _create_version_012_schema(connection)
    connection.execute(
        """
        INSERT INTO "Queue"
          ("data", "message_id", "status", "in_time", "lock_time", "done_time")
        VALUES
          (:data, :message_id, :status, :in_time, :lock_time, :done_time)
        """,
        {
            "data": "completed",
            "message_id": "message-1",
            "status": 2,
            "in_time": 1_500_000_000,
            "lock_time": 2_000_000_000,
            "done_time": 3_250_000_000,
        },
    )
    connection.execute("PRAGMA user_version = 1")
    connection.close()

    queue = LiteQueue(filename=database_path)
    migrated_message = queue.get("message-1")

    assert get_user_version(queue.conn) == 2
    assert _queue_columns(queue.conn)[-2:] == ["idempotency_key", "claim_id"]
    assert migrated_message is not None
    assert migrated_message.in_time == 1.5
    assert migrated_message.lock_time == 2.0
    assert migrated_message.done_time == 3.25


def test_existing_version_012_capacity_survives_all_migrations(
    tmp_path: Path,
) -> None:
    """Schema adoption preserves the existing maxsize trigger and setting."""
    database_path = tmp_path / "queue.sqlite3"
    connection = _connect(database_path)
    _create_version_012_schema(connection)
    connection.execute(
        """
        CREATE TRIGGER "maxsize_control_Queue"
        BEFORE INSERT ON "Queue"
        WHEN (
            SELECT COUNT(*)
            FROM "Queue"
            WHERE "status" = 0
        ) >= 2
        BEGIN
            SELECT RAISE(ABORT, 'Max queue length reached: 2');
        END
        """
    )
    connection.close()

    queue = LiteQueue(filename=database_path)

    assert get_user_version(queue.conn) == 2
    assert queue.maxsize == 2
    queue.put("first")
    queue.put("second")

    with pytest.raises(
        sqlite3.IntegrityError,
        match="Max queue length reached: 2",
    ):
        queue.put("third")


def test_failed_migration_does_not_advance_user_version(tmp_path: Path) -> None:
    """A migration error rolls back its statements and version update."""
    connection = _connect(tmp_path / "queue.sqlite3")
    _create_version_012_schema(connection)
    connection.execute(
        'ALTER TABLE "Queue" ADD COLUMN "claim_id" INTEGER NOT NULL DEFAULT 0'
    )
    connection.execute("PRAGMA user_version = 1")

    with pytest.raises(
        sqlite3.OperationalError,
        match="duplicate column name: claim_id",
    ):
        apply_migrations(connection, MIGRATIONS)

    assert get_user_version(connection) == 1
    assert "idempotency_key" not in _queue_columns(connection)
    assert "claim_id" in _queue_columns(connection)


def test_concurrent_migration_runners_apply_each_version_once(
    tmp_path: Path,
) -> None:
    """Concurrent startup serializes each pending schema migration."""
    database_path = tmp_path / "queue.sqlite3"
    connection = _connect(database_path)
    _create_version_012_schema(connection)
    connection.close()
    barrier = threading.Barrier(2)

    def migrate() -> None:
        """Run all migrations through one independent connection."""
        worker_connection = _connect(database_path)
        try:
            barrier.wait(timeout=5)
            apply_migrations(worker_connection, MIGRATIONS)
        finally:
            worker_connection.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(migrate) for _ in range(2)]
        for future in futures:
            future.result(timeout=15)

    migrated_connection = _connect(database_path)

    assert get_user_version(migrated_connection) == 2
    assert _queue_columns(migrated_connection)[-2:] == [
        "idempotency_key",
        "claim_id",
    ]


def test_newer_database_version_is_rejected_without_schema_changes(
    tmp_path: Path,
) -> None:
    """An older library does not attempt to downgrade a newer database."""
    connection = _connect(tmp_path / "queue.sqlite3")
    connection.execute("PRAGMA user_version = 3")

    with pytest.raises(
        RuntimeError,
        match="Database schema version 3 is newer than supported version 2",
    ):
        apply_migrations(connection, MIGRATIONS)

    table_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM "sqlite_schema"
        WHERE "type" = :object_type
        """,
        {"object_type": "table"},
    ).fetchone()

    assert get_user_version(connection) == 3
    assert table_count is not None
    assert int(table_count[0]) == 0
