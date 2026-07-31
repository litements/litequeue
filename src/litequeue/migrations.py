"""Ordered schema migrations for the LiteQueue SQLite database."""

import sqlite3

type Migration = tuple[str, ...]


# Migration 1 intentionally reproduces the LiteQueue 0.12 fixed schema.
# The optional maxsize trigger remains a per-queue setting managed at startup.
# Existing databases start with user_version 0, so every statement is idempotent.
MIGRATIONS: dict[int, Migration] = {
    1: (
        """
        CREATE TABLE IF NOT EXISTS "Queue"
        (
          "data"       TEXT NOT NULL
          , "message_id" TEXT NOT NULL
          , "status"     INTEGER NOT NULL
          , "in_time"    INTEGER NOT NULL
          , "lock_time"  INTEGER
          , "done_time"  INTEGER
        )
        """,
        (
            'CREATE UNIQUE INDEX IF NOT EXISTS "Queue_message_id_unique_idx" '
            'ON "Queue"(message_id)'
        ),
        (
            'CREATE INDEX IF NOT EXISTS "Queue_status_message_id_idx" '
            'ON "Queue"(status, message_id)'
        ),
    ),
    # Migration 2 converts the historical timestamp unit before new workers run.
    2: (
        """
        ALTER TABLE "Queue"
        ADD COLUMN "idempotency_key" TEXT
        """,
        """
        ALTER TABLE "Queue"
        ADD COLUMN "claim_id" INTEGER NOT NULL DEFAULT 0
        """,
        """
        UPDATE "Queue"
        SET
          "in_time" = "in_time" / 1000000000.0
          , "lock_time" = "lock_time" / 1000000000.0
          , "done_time" = "done_time" / 1000000000.0
        """,
        """
        UPDATE "Queue"
        SET "claim_id" = 1
        WHERE "status" = 3
        """,
        """
        UPDATE "Queue"
        SET
          "status" = 0
          , "lock_time" = NULL
          , "done_time" = NULL
        WHERE "status" = 1
        """,
        (
            'CREATE UNIQUE INDEX "Queue_idempotency_key_unique_idx" '
            'ON "Queue"(idempotency_key) '
            "WHERE idempotency_key IS NOT NULL"
        ),
    ),
    # Restore integer nanosecond timestamps after the seconds storage format.
    3: (
        """
        UPDATE "Queue"
        SET
          "in_time" = CAST(ROUND("in_time" * 1000000000.0) AS INTEGER)
          , "lock_time" = CAST(ROUND("lock_time" * 1000000000.0) AS INTEGER)
          , "done_time" = CAST(ROUND("done_time" * 1000000000.0) AS INTEGER)
        """,
    ),
}


def get_user_version(connection: sqlite3.Connection) -> int:
    """Return the database schema version stored by SQLite."""
    row = connection.execute("PRAGMA user_version").fetchone()
    if row is None:
        raise RuntimeError("SQLite did not return PRAGMA user_version")

    return int(row[0])


def _apply_migration(
    connection: sqlite3.Connection,
    *,
    version: int,
    migration: Migration,
) -> None:
    """Apply one migration atomically when its version is still pending."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        current_version = get_user_version(connection)
        if version <= current_version:
            connection.commit()
            return

        for statement in migration:
            connection.execute(statement)

        connection.execute(f"PRAGMA user_version = {version}")
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def apply_migrations(
    connection: sqlite3.Connection,
    migrations: dict[int, Migration],
) -> None:
    """Apply pending migrations in ascending version order."""
    if not migrations:
        return

    latest_version = max(migrations)
    current_version = get_user_version(connection)
    if current_version > latest_version:
        raise RuntimeError(
            f"Database schema version {current_version} is newer than "
            f"supported version {latest_version}"
        )

    for version in sorted(migrations):
        migration = migrations[version]
        if not migration:
            raise ValueError(f"Migration {version} has no SQL statements")

        _apply_migration(
            connection,
            version=version,
            migration=migration,
        )

    applied_version = get_user_version(connection)
    if applied_version != latest_version:
        raise RuntimeError(
            f"Database schema migration stopped at version {applied_version}; "
            f"expected version {latest_version}"
        )
