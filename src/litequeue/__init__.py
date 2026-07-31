"""Persistent SQLite queue with retry-safe submission and delivery fencing.

LiteQueue owns one fixed ``Queue`` table per database file. Durable writes are
serialized by SQLite. Idempotency keys are unique within that file, and each
delivery transition must match the message's current integer claim ID.
"""

import math
import os
import pprint
import re
import sqlite3
import threading
import time
from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import replace
from enum import Enum
from pathlib import Path
from queue import Queue
from typing import Any
from uuid import UUID

from litequeue.migrations import MIGRATIONS
from litequeue.migrations import apply_migrations

# Expose the clock used for persisted message timestamps.
time_seconds = time.time

# Copied from CPython's Lib/uuid.py.
_RFC_4122_VERSION_7_FLAGS = (7 << 76) | (0x8000 << 48)
_last_timestamp_v7: int | None = None
_last_counter_v7 = 0


def _uuid7_get_counter_and_tail() -> tuple[int, int]:
    """Generate the UUIDv7 counter and random tail."""
    random_value = int.from_bytes(os.urandom(10))
    counter = (random_value >> 32) & 0x1FF_FFFF_FFFF
    tail = random_value & 0xFFFF_FFFF
    return counter, tail


def uuid7() -> UUID:
    """Generate a UUID from a Unix timestamp in milliseconds and random bits.

    UUIDv7 objects feature monotonicity within a millisecond.
    """
    # --- 48 ---   -- 4 --   --- 12 ---   -- 2 --   --- 30 ---   - 32 -
    # unix_ts_ms | version | counter_hi | variant | counter_lo | random
    #
    # 'counter = counter_hi | counter_lo' is a 42-bit counter constructed
    # with Method 1 of RFC 9562, §6.2, and its MSB is set to 0.
    #
    # 'random' is a 32-bit random value regenerated for every new UUID.
    #
    # If multiple UUIDs are generated within the same millisecond, the LSB
    # of 'counter' is incremented by 1. When overflowing, the timestamp is
    # advanced and the counter is reset to a random 42-bit integer with MSB
    # set to 0.

    global _last_timestamp_v7
    global _last_counter_v7

    # UUIDv7 needs millisecond precision. Queue metrics use time_seconds().
    nanoseconds = time.time_ns()
    timestamp_ms = nanoseconds // 1_000_000

    if _last_timestamp_v7 is None or timestamp_ms > _last_timestamp_v7:
        counter, tail = _uuid7_get_counter_and_tail()
    else:
        if timestamp_ms < _last_timestamp_v7:
            timestamp_ms = _last_timestamp_v7 + 1
        counter = _last_counter_v7 + 1
        if counter > 0x3FF_FFFF_FFFF:
            timestamp_ms += 1
            counter, tail = _uuid7_get_counter_and_tail()
        else:
            tail = int.from_bytes(os.urandom(4))

    unix_ts_ms = timestamp_ms & 0xFFFF_FFFF_FFFF
    counter_msbs = counter >> 30
    counter_hi = counter_msbs & 0x0FFF
    counter_lo = counter & 0x3FFF_FFFF
    tail &= 0xFFFF_FFFF

    int_uuid_7 = unix_ts_ms << 80
    int_uuid_7 |= counter_hi << 64
    int_uuid_7 |= counter_lo << 32
    int_uuid_7 |= tail
    int_uuid_7 |= _RFC_4122_VERSION_7_FLAGS
    result = UUID(int=int_uuid_7)

    _last_timestamp_v7 = timestamp_ms
    _last_counter_v7 = counter
    return result


class MessageStatus(int, Enum):
    READY = 0
    LOCKED = 1
    DONE = 2
    FAILED = 3


class IdempotencyConflict(ValueError):
    """An idempotency key refers to a stored message with different data."""

    def __init__(self, idempotency_key: str) -> None:
        """Create an error for one conflicting idempotency key."""
        self.idempotency_key = idempotency_key
        message = (
            f"Idempotency key {idempotency_key!r} already exists with different data"
        )
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class Message:
    """A persistent queue message and its current delivery state.

    A ready message has claim ID 0 until its first delivery. Each later
    delivery increments the claim ID for that message.
    """

    data: str
    message_id: str  # UUID v7
    status: MessageStatus
    in_time: float
    lock_time: float | None
    done_time: float | None
    idempotency_key: str | None = None
    claim_id: int = 0


def _message_from_row(row: sqlite3.Row) -> Message:
    """Convert a SQLite row into a typed message."""

    stored_status = row["status"]
    try:
        status = MessageStatus(stored_status)
    except ValueError as error:
        raise ValueError(f"Unknown message status: {stored_status!r}") from error

    return Message(
        data=row["data"],
        message_id=row["message_id"],
        status=status,
        in_time=row["in_time"],
        lock_time=row["lock_time"],
        done_time=row["done_time"],
        idempotency_key=row["idempotency_key"],
        claim_id=row["claim_id"],
    )


type PopFunction = Callable[[], Message | None]

_QUEUE_TABLE_NAME = "Queue"
_READ_CONNECTION_POOL_SIZE = 10
_SQLITE_MAX_INTEGER = 2**63 - 1
_MANAGED_CONNECTION_OPTIONS = {
    "autocommit",
    "cached_statements",
    "check_same_thread",
    "database",
    "isolation_level",
}


def _validate_claim_id(claim_id: int) -> int:
    """Validate a claim ID before SQLite uses it in a transition."""
    claim_id_is_integer = isinstance(claim_id, int)
    claim_id_is_boolean = isinstance(claim_id, bool)
    if not claim_id_is_integer or claim_id_is_boolean:
        raise TypeError("claim_id must be an integer")

    claim_id_is_in_range = 1 <= claim_id <= _SQLITE_MAX_INTEGER
    if not claim_id_is_in_range:
        raise ValueError("claim_id must be between 1 and 9223372036854775807")

    return claim_id


def _validate_idempotency_key(idempotency_key: str | None) -> str | None:
    """Validate an optional producer idempotency key."""
    if idempotency_key is None:
        return None

    if not isinstance(idempotency_key, str):
        raise TypeError("idempotency_key must be a string or None")

    return idempotency_key


def _validate_message_data(data: str) -> str:
    """Validate message data before SQLite stores it."""
    if not isinstance(data, str):
        raise TypeError("data must be a string")

    return data


def _validate_threshold_seconds(threshold_seconds: int | float) -> float:
    """Validate a duration and return its value in seconds."""
    threshold_is_number = isinstance(threshold_seconds, (int, float))
    threshold_is_boolean = isinstance(threshold_seconds, bool)
    if not threshold_is_number or threshold_is_boolean:
        raise TypeError("threshold_seconds must be a number")

    try:
        validated_threshold_seconds = float(threshold_seconds)
    except OverflowError as error:
        raise ValueError("threshold_seconds is too large") from error

    threshold_is_finite = math.isfinite(validated_threshold_seconds)
    threshold_is_zero_or_positive = validated_threshold_seconds >= 0
    if not threshold_is_finite or not threshold_is_zero_or_positive:
        raise ValueError("threshold_seconds must be finite and zero or positive")

    return validated_threshold_seconds


def validate_maxsize(maxsize: int | None) -> int | None:
    """Validate and return a queue capacity."""

    maxsize_is_integer = isinstance(maxsize, int)
    maxsize_is_boolean = isinstance(maxsize, bool)
    if maxsize is not None and (not maxsize_is_integer or maxsize_is_boolean):
        raise TypeError("maxsize must be an integer or None")

    if maxsize is not None and maxsize < 0:
        raise ValueError("maxsize must be zero or a positive integer")

    return maxsize


class LiteQueue:
    def __init__(
        self,
        filename: str | Path,
        maxsize: int | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Create a new queue.

        Args:
        - filename: Location of the SQLite database file. The file is the queue.
        - maxsize: Maximum number of ready messages allowed in the queue. The
          value is stored as an immutable queue setting. When reopening a
          queue, omit it to use the stored setting or pass the same value.
          Conflicting values raise ValueError. Zero creates a queue that
          cannot accept messages (default: None, unlimited on first creation).
        - kwargs: Additional options forwarded to every `sqlite3.connect()`
          call, including `timeout`, `detect_types`, `factory`, and `uri`.
          LiteQueue manages `database`, `isolation_level`, `check_same_thread`,
          `cached_statements`, and `autocommit`; passing one raises ValueError.

        Each database can contain only one LiteQueue queue, stored in the fixed
        "Queue" table. Other tables are not supported.

        One LiteQueue instance can be shared between threads when SQLite was
        compiled in serialized mode, as in standard CPython builds. LiteQueue
        serializes writes and explicit transactions while a read-only pool
        serves committed data to other threads.
        """
        filename_is_supported = isinstance(filename, (str, Path))
        if not filename_is_supported:
            raise TypeError("filename must be a string or pathlib.Path")

        if filename == "":
            raise ValueError("filename must not be empty")

        managed_options = _MANAGED_CONNECTION_OPTIONS.intersection(kwargs)
        if managed_options:
            option_list = ", ".join(sorted(managed_options))
            raise ValueError(
                f"LiteQueue manages SQLite connection options: {option_list}"
            )

        validated_maxsize = validate_maxsize(maxsize)

        self._write_connection_lock = threading.RLock()
        self._transaction_owner: int | None = None
        self._close_state_lock = threading.Lock()
        self._is_closed = False

        self.pop: PopFunction = self._select_pop_func()

        database_filename = str(filename)
        self.conn = sqlite3.connect(
            database=database_filename,
            isolation_level=None,
            check_same_thread=False,
            # Disable cached statements due to a bug in CPython >= 3.12.
            # https://github.com/python/cpython/issues/118172
            cached_statements=0,
            **kwargs,
        )
        self.conn.row_factory = sqlite3.Row
        self.table = f'"{_QUEUE_TABLE_NAME}"'

        with self.transaction(mode="IMMEDIATE"):
            table_rows = self.conn.execute(
                """
                SELECT name
                FROM sqlite_schema
                WHERE type = 'table' AND name NOT GLOB 'sqlite_*'
                ORDER BY name
                """
            ).fetchall()
            table_names = [row["name"] for row in table_rows]
            unsupported_tables = [
                name for name in table_names if name != _QUEUE_TABLE_NAME
            ]
            if unsupported_tables:
                table_label = "table" if len(unsupported_tables) == 1 else "tables"
                table_list = ", ".join(unsupported_tables)
                raise ValueError(
                    "LiteQueue versions later than 0.9 require a database with a "
                    "single `Queue` table. These versions do not allow multiple "
                    "queues or database sharing with other applications. "
                    f"Found unsupported {table_label}: {table_list}. "
                    "See the migration guide: "
                    "https://github.com/litements/litequeue/blob/main/docs/"
                    "migrate_single_queue.md"
                )

            table_exists = bool(table_names)

        apply_migrations(self.conn, MIGRATIONS)

        with self.transaction(mode="IMMEDIATE"):
            stored_maxsize = self._get_stored_maxsize()
            if table_exists:
                maxsize_conflicts = validated_maxsize is not None and (
                    validated_maxsize != stored_maxsize
                )
                if maxsize_conflicts:
                    raise ValueError(
                        f"maxsize {validated_maxsize} conflicts with stored maxsize "
                        f"{stored_maxsize} for queue '{_QUEUE_TABLE_NAME}'"
                    )
                effective_maxsize = stored_maxsize
            else:
                effective_maxsize = validated_maxsize

            if effective_maxsize is not None:
                self.conn.execute(
                    f"""
CREATE TRIGGER IF NOT EXISTS "maxsize_control_Queue"
   BEFORE INSERT
   ON {self.table}
   WHEN (SELECT COUNT(*) FROM {self.table} WHERE status = {MessageStatus.READY.value}) >= {effective_maxsize}
BEGIN
    SELECT RAISE (ABORT,'Max queue length reached: {effective_maxsize}');
END;"""
                )

        self.maxsize = effective_maxsize

        journal_mode_row = self.conn.execute("PRAGMA journal_mode;").fetchone()
        current_journal_mode = journal_mode_row[0].lower()
        if current_journal_mode != "wal":
            # Changing journal mode takes a database lock. Avoid that lock when
            # reopening the queue after WAL has already been configured.
            self.conn.execute("PRAGMA journal_mode = WAL;")
        self.conn.execute("PRAGMA temp_store = MEMORY;")
        self.conn.execute("PRAGMA synchronous = NORMAL;")

        # Separate read connections prevent reads in one thread from joining
        # another thread's write transaction and observing data that may still
        # be rolled back. A small fixed pool also allows unrelated reads to run
        # concurrently without creating an unbounded number of file handles.
        read_connections: Queue[sqlite3.Connection] = Queue(
            maxsize=_READ_CONNECTION_POOL_SIZE
        )
        for _ in range(_READ_CONNECTION_POOL_SIZE):
            read_connection = sqlite3.connect(
                database=database_filename,
                isolation_level=None,
                check_same_thread=False,
                # Disable cached statements due to a bug in CPython >= 3.12.
                # https://github.com/python/cpython/issues/118172
                cached_statements=0,
                **kwargs,
            )
            read_connection.row_factory = sqlite3.Row
            # This is a second line of defense against accidentally routing
            # a mutation through the pool in a future code change.
            read_connection.execute("PRAGMA query_only = ON;")
            read_connections.put(read_connection)
        self._read_connections = read_connections

    def _get_stored_maxsize(self) -> int | None:
        """Read the immutable capacity from the queue's trigger."""

        trigger = self.conn.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'trigger' AND name = :trigger_name COLLATE NOCASE
            """,
            {"trigger_name": "maxsize_control_Queue"},
        ).fetchone()
        if trigger is None:
            return None

        trigger_sql = trigger["sql"]
        match = re.search(r"Max queue length reached: (-?\d+)", trigger_sql)
        if match is None:
            raise ValueError("Stored maxsize trigger for queue 'Queue' is invalid")

        stored_maxsize = int(match.group(1))
        return validate_maxsize(stored_maxsize)

    def get_sqlite_version(self) -> tuple[int, int, int]:
        """Return the SQLite version or reject unsupported major versions."""
        sqlite_version_info = sqlite3.sqlite_version_info
        major_version = int(sqlite_version_info[0])
        minor_version = int(sqlite_version_info[1])
        patch_version = int(sqlite_version_info[2])
        sqlite_version = (major_version, minor_version, patch_version)

        if major_version != 3:
            formatted_version = ".".join(str(part) for part in sqlite_version)
            raise RuntimeError(
                f"LiteQueue requires SQLite 3; found SQLite {formatted_version}"
            )

        return sqlite_version

    def _select_pop_func(self) -> PopFunction:
        """Select the fastest pop implementation supported by SQLite."""
        sqlite_version = self.get_sqlite_version()

        if sqlite_version >= (3, 35, 0):
            return self._pop_returning

        return self._pop_transaction

    def put(
        self,
        data: str,
        *,
        idempotency_key: str | None = None,
    ) -> Message:
        """Insert a message or return the current message for a replay.

        Args:
        - data: Message data as a string.
        - idempotency_key: Optional caller identity for one logical submission.

        A replay must use the same key and data. A replay returns the stored
        message with its current status, timestamps, and claim ID.

        Raises:
        - IdempotencyConflict: The key exists with different data.
        - TypeError: The data or key has an unsupported type.
        """
        validated_data = _validate_message_data(data)
        validated_key = _validate_idempotency_key(idempotency_key)
        if validated_key is None:
            return self._insert_message(
                data=validated_data,
                idempotency_key=None,
            )

        with self._immediate_transaction_if_needed():
            existing_row = self.conn.execute(
                f"""
                SELECT *
                FROM {self.table}
                WHERE idempotency_key = :idempotency_key
                """.strip(),
                {"idempotency_key": validated_key},
            ).fetchone()

            if existing_row is None:
                return self._insert_message(
                    data=validated_data,
                    idempotency_key=validated_key,
                )

            existing_message = _message_from_row(existing_row)
            if existing_message.data != validated_data:
                raise IdempotencyConflict(idempotency_key=validated_key)

            return existing_message

    def _insert_message(
        self,
        *,
        data: str,
        idempotency_key: str | None,
    ) -> Message:
        """Insert one message in the active transaction or in autocommit mode."""
        message_id = str(uuid7())
        now = time_seconds()

        with self._write_connection_lock:
            self.conn.execute(
                f"""
                INSERT INTO
                  {self.table}
                       (  data,  message_id,  idempotency_key, status,                      in_time, lock_time, done_time )
                VALUES ( :data, :message_id, :idempotency_key, {MessageStatus.READY.value}, :now   , NULL     , NULL      )
                """.strip(),
                {
                    "data": data,
                    "message_id": message_id,
                    "idempotency_key": idempotency_key,
                    "now": now,
                },
            )

        return Message(
            data=data,
            message_id=message_id,
            status=MessageStatus.READY,
            in_time=now,
            lock_time=None,
            done_time=None,
            idempotency_key=idempotency_key,
            claim_id=0,
        )

    def _pop_returning(self) -> Message | None:
        """Atomically claim the oldest ready message and increment its claim ID."""
        with self.transaction(mode="IMMEDIATE"):
            message = self.conn.execute(
                f"""
                 UPDATE {self.table}
                 SET status = {MessageStatus.LOCKED.value},
                     lock_time = :now,
                     claim_id = claim_id + 1
                 WHERE rowid = (SELECT rowid
                                FROM {self.table}
                                WHERE status = {MessageStatus.READY.value}
                                ORDER BY message_id
                                LIMIT 1)
                 RETURNING *
                 """,
                {"now": time_seconds()},
            ).fetchone()

            if not message:
                return None

            return _message_from_row(message)

    def _pop_transaction(self) -> Message | None:
        """Claim one message on SQLite versions without RETURNING support."""
        with self.transaction(mode="IMMEDIATE"):
            message = self.conn.execute(
                f"""
                SELECT * FROM {self.table}
                WHERE status = {MessageStatus.READY.value}
                ORDER BY message_id
                LIMIT 1
                """.strip()
            ).fetchone()

            if message is None:
                return None

            lock_time = time_seconds()
            self.conn.execute(
                f"""
                UPDATE {self.table} SET
                  status = {MessageStatus.LOCKED.value}
                  , lock_time = :lock_time
                  , claim_id = claim_id + 1
                WHERE message_id = :message_id
                  AND status = {MessageStatus.READY.value}
                """.strip(),
                {
                    "lock_time": lock_time,
                    "message_id": message["message_id"],
                },
            )

            selected_message = _message_from_row(message)
            return replace(
                selected_message,
                status=MessageStatus.LOCKED,
                lock_time=lock_time,
                claim_id=selected_message.claim_id + 1,
            )

    @contextmanager
    def _read_connection(self) -> Generator[sqlite3.Connection]:
        """Check out a read connection with correct transaction visibility."""
        current_thread = threading.get_ident()
        transaction_is_owned = self._transaction_owner == current_thread
        if transaction_is_owned:
            # The transaction owner must read through the write connection to
            # see its own uncommitted changes. RLock makes this reacquisition
            # safe while transaction() already holds the write lock.
            with self._write_connection_lock:
                yield self.conn
            return

        read_connections = self._read_connections
        with self._close_state_lock:
            if self._is_closed:
                raise sqlite3.ProgrammingError("Cannot operate on a closed database.")
            # Checkout happens while holding the close-state lock so close()
            # cannot drain the pool between the closed check and Queue.get().
            # Returning a connection never needs this lock, so waiting here
            # cannot prevent an active reader from releasing a pool slot.
            read_connection = read_connections.get()

        try:
            yield read_connection
        finally:
            # Always return the connection, including when row conversion or a
            # SQLite call fails, so one error cannot slowly exhaust the pool.
            read_connections.put(read_connection)

    def peek(self) -> Message | None:
        """Show next message to be popped, if any."""

        with self._read_connection() as connection:
            value = connection.execute(
                f"SELECT * FROM {self.table} WHERE status = {MessageStatus.READY.value} ORDER BY message_id LIMIT 1",
            ).fetchone()

        return _message_from_row(value) if value is not None else None

    def get(self, message_id: str) -> Message | None:
        """Get a message by its `message_id`."""

        with self._read_connection() as connection:
            value = connection.execute(
                f"SELECT * FROM {self.table} WHERE message_id = :message_id",
                {"message_id": message_id},
            ).fetchone()

        return _message_from_row(value) if value is not None else None

    def done(self, message_id: str, *, claim_id: int) -> bool:
        """Mark the current message claim as done.

        Return `True` when the message is locked with the specified claim ID.
        Return `False` for a missing message, stale claim, or invalid state.
        """

        validated_claim_id = _validate_claim_id(claim_id)
        now = time_seconds()

        with self._write_connection_lock:
            cursor = self.conn.execute(
                f"""
                UPDATE {self.table} SET
                  status = {MessageStatus.DONE.value}
                  , done_time = :now
                WHERE message_id = :message_id
                  AND status = {MessageStatus.LOCKED.value}
                  AND claim_id = :claim_id
                """.strip(),
                {
                    "now": now,
                    "message_id": message_id,
                    "claim_id": validated_claim_id,
                },
            )

        return cursor.rowcount > 0

    def mark_failed(self, message_id: str, *, claim_id: int) -> bool:
        """Mark the current message claim as failed.

        Return `True` when the message is locked with the specified claim ID.
        Return `False` for a missing message, stale claim, or invalid state.
        """

        validated_claim_id = _validate_claim_id(claim_id)

        with self._write_connection_lock:
            cursor = self.conn.execute(
                f"""
                UPDATE {self.table} SET
                  status = {MessageStatus.FAILED.value}
                  , done_time = :now
                WHERE message_id = :message_id
                  AND status = {MessageStatus.LOCKED.value}
                  AND claim_id = :claim_id
                """.strip(),
                {
                    "now": time_seconds(),
                    "message_id": message_id,
                    "claim_id": validated_claim_id,
                },
            )

        return cursor.rowcount > 0

    def list_locked(self, threshold_seconds: int | float) -> Iterator[Message]:
        """Return claims that are older than the specified age.

        This method returns a snapshot. Use `retry_expired()` when a recovery
        process must retry expired claims atomically.
        """

        validated_threshold_seconds = _validate_threshold_seconds(threshold_seconds)

        with self._read_connection() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM {self.table}
                WHERE
                  status = {MessageStatus.LOCKED.value}
                  AND lock_time < :time_value
                """.strip(),
                {"time_value": time_seconds() - validated_threshold_seconds},
            ).fetchall()

        for result in rows:
            yield _message_from_row(result)

    def list_failed(self) -> Iterator[Message]:
        """Return all the tasks in `FAILED` state."""

        with self._read_connection() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM {self.table}
                WHERE status = {MessageStatus.FAILED.value}
                """.strip()
            ).fetchall()

        for result in rows:
            yield _message_from_row(result)

    def retry(self, message_id: str, *, claim_id: int) -> bool:
        """Return a locked or failed message to the ready state.

        Return `True` when the status and claim ID match. Return `False` for a
        missing message, stale claim, or invalid state.
        """

        validated_claim_id = _validate_claim_id(claim_id)

        with self._write_connection_lock:
            cursor = self.conn.execute(
                f"""
                UPDATE {self.table} SET
                  status = {MessageStatus.READY.value}
                  , lock_time = NULL
                  , done_time = NULL
                WHERE message_id = :message_id
                  AND status IN (
                    {MessageStatus.LOCKED.value},
                    {MessageStatus.FAILED.value}
                  )
                  AND claim_id = :claim_id
                """.strip(),
                {
                    "message_id": message_id,
                    "claim_id": validated_claim_id,
                },
            )

        return cursor.rowcount > 0

    def retry_expired(self, threshold_seconds: int | float) -> int:
        """Return expired claims to ready in one atomic SQL update.

        The threshold must be finite and zero or positive. The return value is
        the number of claims that changed state.
        """
        validated_threshold_seconds = _validate_threshold_seconds(threshold_seconds)

        with self._write_connection_lock:
            cutoff_time = time_seconds() - validated_threshold_seconds
            cursor = self.conn.execute(
                f"""
                UPDATE {self.table} SET
                  status = {MessageStatus.READY.value}
                  , lock_time = NULL
                  , done_time = NULL
                WHERE status = {MessageStatus.LOCKED.value}
                  AND lock_time < :cutoff_time
                """.strip(),
                {"cutoff_time": cutoff_time},
            )

        return cursor.rowcount

    def _count_statuses(self, *statuses: MessageStatus) -> int:
        """Return the number of stored messages in the requested states."""
        if not statuses:
            return self.stored_count()

        placeholders = ", ".join("?" for _ in statuses)
        with self._read_connection() as connection:
            value = connection.execute(
                f"SELECT COUNT(*) FROM {self.table} WHERE status IN ({placeholders})",
                tuple(status.value for status in statuses),
            ).fetchone()
        return int(value[0])

    def ready_count(self) -> int:
        """Return the number of messages waiting to be claimed."""
        return self._count_statuses(MessageStatus.READY)

    def locked_count(self) -> int:
        """Return the number of messages currently claimed by workers."""
        return self._count_statuses(MessageStatus.LOCKED)

    def done_count(self) -> int:
        """Return the number of completed messages still stored."""
        return self._count_statuses(MessageStatus.DONE)

    def failed_count(self) -> int:
        """Return the number of failed messages still stored."""
        return self._count_statuses(MessageStatus.FAILED)

    def active_count(self) -> int:
        """Return the number of ready and locked messages."""
        return self._count_statuses(MessageStatus.READY, MessageStatus.LOCKED)

    def stored_count(self) -> int:
        """Return the total number of rows stored for every message state."""
        with self._read_connection() as connection:
            value = connection.execute(f"SELECT COUNT(*) FROM {self.table}").fetchone()
        return int(value[0])

    def qsize(self) -> int:
        """Return the ready backlog size."""
        return self.ready_count()

    def empty(self) -> bool:
        """Return True when the ready backlog is empty."""
        return self.qsize() == 0

    def full(self) -> bool:
        """Return True when the ready backlog has reached `maxsize`."""
        if self.maxsize is None:
            return False
        return self.qsize() >= self.maxsize

    def prune(self, include_failed: bool = True) -> None:
        """
        Delete `DONE` messages.

        If `include_failed` is True, this method also deletes `FAILED` messages.
        Deletion releases each message's idempotency key for later reuse.
        """
        with self._write_connection_lock:
            if include_failed:
                self.conn.execute(
                    f"DELETE FROM {self.table} WHERE status IN ({MessageStatus.DONE.value}, {MessageStatus.FAILED.value})"
                )
            else:
                self.conn.execute(
                    f"DELETE FROM {self.table} WHERE status IN ({MessageStatus.DONE.value})"
                )

    def vacuum(self) -> None:
        """
        Vacuum the database.

        IMPORTANT: The `VACUUM` step can take some time to finish depending on
        the size of the queue and how many messages have been deleted.
        """
        with self._write_connection_lock:
            self.conn.execute("VACUUM;")

    @contextmanager
    def _immediate_transaction_if_needed(self) -> Generator[None]:
        """Serialize idempotent insertion unless the caller owns a transaction.

        A new immediate transaction makes the key lookup and insert atomic.
        An explicit caller transaction remains authoritative when one exists.
        """
        current_thread = threading.get_ident()
        current_thread_owns_transaction = self._transaction_owner == current_thread
        if current_thread_owns_transaction:
            yield
            return

        with self.transaction(mode="IMMEDIATE"):
            yield

    # SQLite works better in autocommit mode when using short DML (INSERT /
    # UPDATE / DELETE) statements
    @contextmanager
    def transaction(self, mode: str = "DEFERRED") -> Generator[None]:
        """Run a transaction while excluding other threads from the connection."""
        if mode not in {"DEFERRED", "IMMEDIATE", "EXCLUSIVE"}:
            raise ValueError(f"Transaction mode '{mode}' is not valid")
        with self._write_connection_lock:
            # We must issue a "BEGIN" explicitly when running in auto-commit mode.
            self.conn.execute(f"BEGIN {mode}")
            self._transaction_owner = threading.get_ident()
            try:
                # Yield control back to the caller.
                yield
            except BaseException:
                self.conn.rollback()  # Roll back all changes if an exception occurs.
                raise
            else:
                self.conn.commit()
            finally:
                self._transaction_owner = None

    def __repr__(self) -> str:
        with self._read_connection() as connection:
            rows = connection.execute(f"SELECT * FROM {self.table} LIMIT 3").fetchall()
            display_items = [_message_from_row(row) for row in rows]
            connection_repr = repr(self.conn)

        items = pprint.pformat(display_items)
        return f"{type(self).__name__}(Connection={connection_repr}, items={items})"

    def close(self) -> None:
        with self._write_connection_lock:
            with self._close_state_lock:
                if self._is_closed:
                    return
                self._is_closed = True

            read_connections = self._read_connections
            # Draining all ten slots waits for checked-out readers to finish.
            # Once _is_closed is set, new readers fail before checkout, so they
            # cannot race shutdown or use a closed connection.
            connections_to_close = [
                read_connections.get() for _ in range(_READ_CONNECTION_POOL_SIZE)
            ]
            for read_connection in connections_to_close:
                read_connection.close()

            self.conn.close()


# Kept for backwards compatibility
SQLQueue = LiteQueue
