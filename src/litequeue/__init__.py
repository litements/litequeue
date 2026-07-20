import os
import pprint
import re
import sqlite3
import threading
import time
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import replace
from enum import Enum
from pathlib import Path
from queue import Queue
from typing import Any
from uuid import UUID

# Expose function used by uuid7() to get current time in nanoseconds
# since the Unix epoch.
time_ns = time.time_ns

# Copied from CPython's Lib/uuid.py.
_RFC_4122_VERSION_7_FLAGS = (7 << 76) | (0x8000 << 48)
_last_timestamp_v7 = None
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


@dataclass(frozen=True, slots=True)
class Message:
    data: str
    message_id: str  # UUID v7
    status: MessageStatus
    in_time: int
    lock_time: int | None
    done_time: int | None


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
    )


type PopFunction = Callable[[], Message | None]

_QUEUE_TABLE_NAME = "Queue"
_READ_CONNECTION_POOL_SIZE = 10
_MANAGED_CONNECTION_OPTIONS = {
    "autocommit",
    "cached_statements",
    "check_same_thread",
    "database",
    "isolation_level",
}


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
        name: str,
        folder: Path | None = None,
        maxsize: int | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Create a new queue.

        Args:
        - name: Queue name. LiteQueue stores it in `<name>.queue.sqlite3`.
        - folder: Directory for a named queue. Defaults to the current directory.
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
        if not isinstance(name, str):
            raise TypeError("name must be a string")

        if name == "":
            raise ValueError("name must not be empty")

        if Path(name).name != name:
            raise ValueError("name must not contain a directory path; use folder")

        if folder is not None and not isinstance(folder, Path):
            raise TypeError("folder must be a pathlib.Path or None")

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

        queue_folder = folder if folder is not None else Path.cwd()
        if not queue_folder.is_dir():
            raise ValueError("folder must be an existing directory")

        database_filename = queue_folder / f"{name}.queue.sqlite3"
        self.conn = sqlite3.connect(
            database=str(database_filename),
            isolation_level=None,
            check_same_thread=False,
            # Disable cached statements due to a bug in CPython >= 3.12.
            # https://github.com/python/cpython/issues/118172
            cached_statements=0,
            **kwargs,
        )

        self.conn.row_factory = sqlite3.Row

        self.pop: PopFunction = self._select_pop_func()

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
                    "LiteQueue no longer supports multiple queues or other tables "
                    f"in one database. Found unsupported {table_label}: {table_list}. "
                    "Each queue must use its own database."
                )

            table_exists = bool(table_names)

            # int == bool in SQLite
            # will have rowid as primary key by default
            self.conn.execute(
                f"""CREATE TABLE IF NOT EXISTS {self.table}
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

            self.conn.execute(
                f'CREATE UNIQUE INDEX IF NOT EXISTS "Queue_message_id_unique_idx" '
                f"ON {self.table}(message_id)"
            )

            self.conn.execute(
                f'CREATE INDEX IF NOT EXISTS "Queue_status_message_id_idx" '
                f"ON {self.table}(status, message_id)"
            )

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
                database=str(database_filename),
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

    def get_sqlite_version(self) -> int:
        sqlite_ver = sqlite3.sqlite_version_info

        v_major = int(sqlite_ver[0])
        v_min = int(sqlite_ver[1])
        # _v_bug = int(sqlite_ver[2])

        assert v_major == 3

        return v_min

    def _select_pop_func(self) -> PopFunction:
        """Select the fastest pop implementation supported by SQLite."""
        sqlite_minor_version = self.get_sqlite_version()

        if sqlite_minor_version >= 35:
            return self._pop_returning

        return self._pop_transaction

    def put(self, data: str) -> Message:
        """
        Insert a new message
        """
        # timeout: int = None
        message_id = str(uuid7())
        now = time_ns()

        with self._write_connection_lock:
            self.conn.execute(
                f"""
                INSERT INTO
                  {self.table}
                       (  data,  message_id, status,                      in_time, lock_time, done_time )
                VALUES ( :data, :message_id, {MessageStatus.READY.value}, :now   , NULL     , NULL      )
                """.strip(),
                {"data": data, "message_id": message_id, "now": now},
            )

        return Message(
            data=data,
            message_id=message_id,
            status=MessageStatus.READY,
            in_time=now,
            lock_time=None,
            done_time=None,
        )

    def _pop_returning(self) -> Message | None:
        with self.transaction(mode="IMMEDIATE"):
            message = self.conn.execute(
                f"""
                 UPDATE {self.table}
                 SET status = {MessageStatus.LOCKED.value}, lock_time = :now
                 WHERE rowid = (SELECT rowid
                                FROM {self.table}
                                WHERE status = {MessageStatus.READY.value}
                                ORDER BY message_id
                                LIMIT 1)
                 RETURNING *
                 """,
                {"now": time_ns()},
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

            lock_time = time_ns()
            self.conn.execute(
                f"""
                UPDATE {self.table} SET
                  status = {MessageStatus.LOCKED.value}
                  , lock_time = :lock_time
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
            )

    @contextmanager
    def _read_connection(self) -> Iterator[sqlite3.Connection]:
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
        "Show next message to be popped, if any."

        with self._read_connection() as connection:
            value = connection.execute(
                f"SELECT * FROM {self.table} WHERE status = {MessageStatus.READY.value} ORDER BY message_id LIMIT 1",
            ).fetchone()

        return _message_from_row(value) if value is not None else None

    def get(self, message_id: str) -> Message | None:
        "Get a message by its `message_id`"

        with self._read_connection() as connection:
            value = connection.execute(
                f"SELECT * FROM {self.table} WHERE message_id = :message_id",
                {"message_id": message_id},
            ).fetchone()

        return _message_from_row(value) if value is not None else None

    def done(self, message_id: str) -> bool:
        """
        Mark message as done.
        If executed multiple times, `done_time` will be
        the last time this function is called.

        Return `True` when the message exists, otherwise `False`.
        """

        now = time_ns()

        with self._write_connection_lock:
            cursor = self.conn.execute(
                f"""
                UPDATE {self.table} SET
                  status = {MessageStatus.DONE.value}
                  , done_time = :now
                WHERE message_id = :message_id
                """.strip(),
                {"now": now, "message_id": message_id},
            )

        return cursor.rowcount > 0

    def mark_failed(self, message_id: str) -> bool:
        """
        Mark a message as failed.

        Return `True` when the message exists, otherwise `False`.
        """

        with self._write_connection_lock:
            cursor = self.conn.execute(
                f"""
                UPDATE {self.table} SET
                  status = {MessageStatus.FAILED.value}
                  , done_time = :now
                WHERE message_id = :message_id
                """.strip(),
                {"now": time_ns(), "message_id": message_id},
            )

        return cursor.rowcount > 0

    def list_locked(self, threshold_seconds: int) -> Iterator[Message]:
        """
        Return all the tasks that have been in the `LOCKED` state for more than
        `threshold_seconds` seconds.
        """

        threshold_nanoseconds = threshold_seconds * 1e9

        with self._read_connection() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM {self.table}
                WHERE
                  status = {MessageStatus.LOCKED.value}
                  AND  lock_time < :time_value
                """.strip(),
                {"time_value": time_ns() - threshold_nanoseconds},
            ).fetchall()

        for result in rows:
            yield _message_from_row(result)

    def list_failed(self) -> Iterator[Message]:
        """
        Return all the tasks in `FAILED` state.
        """

        with self._read_connection() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM {self.table}
                WHERE
                  status = {MessageStatus.FAILED.value}
                """.strip()
            ).fetchall()

        for result in rows:
            yield _message_from_row(result)

    def retry(self, message_id: str) -> bool:
        """
        Mark a locked message as free again.

        Return `True` when the message exists, otherwise `False`.
        """

        with self._write_connection_lock:
            cursor = self.conn.execute(
                f"""
                UPDATE {self.table} SET
                  status = {MessageStatus.READY.value}
                  , done_time = NULL
                WHERE message_id = :message_id
                """.strip(),
                {"message_id": message_id},
            )

        return cursor.rowcount > 0

    def qsize(self) -> int:
        """
        Get current size of the queue.
        """

        with self._read_connection() as connection:
            cursor = connection.execute(
                f"""
            SELECT COUNT(*) FROM {self.table}
            WHERE status NOT IN ({MessageStatus.DONE.value}, {MessageStatus.FAILED.value})
            """.strip()
            )
            size = next(cursor)[0]

        return size

    def empty(self) -> bool:
        """
        Return True if the queue is empty.
        """

        with self._read_connection() as connection:
            value = connection.execute(
                f"SELECT COUNT(*) as cnt FROM {self.table} WHERE status = {MessageStatus.READY.value}"
            ).fetchone()
        return not bool(value["cnt"])

    def full(self) -> bool:
        """
        Return True if the queue is full.
        """

        # Here I need to check compared to the maxsize value
        # If maxsize is not set, the queue can grow forever
        if self.maxsize is None:
            return False

        with self._read_connection() as connection:
            value = connection.execute(
                f"SELECT COUNT(*) as cnt FROM {self.table} WHERE status = {MessageStatus.READY.value}"
            ).fetchone()

        if value["cnt"] >= self.maxsize:
            return True
        else:
            return False

    def prune(self, include_failed: bool = True) -> None:
        """
        Delete `DONE` messages.

        If `include_failed` is True, the messages in `FAILED` state will be deleted too.
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

    # SQLite works better in autocommit mode when using short DML (INSERT /
    # UPDATE / DELETE) statements
    @contextmanager
    def transaction(self, mode: str = "DEFERRED") -> Iterator[None]:
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
