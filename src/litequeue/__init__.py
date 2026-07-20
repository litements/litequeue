import os
import pprint
import re
import sqlite3
import time
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
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


type PopFunction = Callable[[], Message | None]

_QUEUE_TABLE_NAME = "Queue"


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
        name: str | None = None,
        conn: sqlite3.Connection | None = None,
        folder: Path | None = None,
        maxsize: int | None = None,
        sqlite_cache_size_bytes: int = 256_000,
        **kwargs,
    ) -> None:
        """
        Create a new queue.

        Args:
        - name: Queue name. LiteQueue stores it in `<name>.queue.sqlite3`.
        - conn: Existing SQLite connection to use instead of creating a database.
        - folder: Directory for a named queue. Defaults to the current directory.
        - maxsize: Maximum number of ready messages allowed in the queue. The
          value is stored as an immutable queue setting. When reopening a
          queue, omit it to use the stored setting or pass the same value.
          Conflicting values raise ValueError. Zero creates a queue that
          cannot accept messages (default: None, unlimited on first creation).
        - sqlite_cache_size_bytes: Size for the SQLite cache_size in bytes (default: 256_000 [256MB])

        Each database can contain only one LiteQueue queue, stored in the fixed
        "Queue" table. Other tables are not supported.

        """
        name_was_provided = name is not None
        connection_was_provided = conn is not None
        if name_was_provided == connection_was_provided:
            raise ValueError("Exactly one of name or conn must be provided")

        if name is not None and not isinstance(name, str):
            raise TypeError("name must be a string")

        if name == "":
            raise ValueError("name must not be empty")

        if name is not None and Path(name).name != name:
            raise ValueError("name must not contain a directory path; use folder")

        if conn is not None and not isinstance(conn, sqlite3.Connection):
            raise TypeError("conn must be a sqlite3.Connection")

        if folder is not None and not isinstance(folder, Path):
            raise TypeError("folder must be a pathlib.Path or None")

        if conn is not None and folder is not None:
            raise ValueError("folder cannot be used with conn")

        if conn is not None and kwargs:
            raise ValueError("SQLite connection options cannot be used with conn")

        validated_maxsize = validate_maxsize(maxsize)

        assert sqlite_cache_size_bytes > 0
        cache_n = -1 * sqlite_cache_size_bytes

        if conn is not None:
            self.conn = conn
            self.conn.isolation_level = None
        else:
            queue_folder = folder if folder is not None else Path.cwd()
            if not queue_folder.is_dir():
                raise ValueError("folder must be an existing directory")

            database_filename = queue_folder / f"{name}.queue.sqlite3"
            self.conn = sqlite3.connect(
                str(database_filename),
                isolation_level=None,
                check_same_thread=False,
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

        # if fast:
        self.conn.execute("PRAGMA journal_mode = WAL;")
        self.conn.execute("PRAGMA temp_store = MEMORY;")
        self.conn.execute("PRAGMA synchronous = NORMAL;")
        self.conn.execute(f"PRAGMA cache_size = {cache_n};")

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
        """
        Decide which message pop() logic to use
        depending on the sqlite version.
        """

        v = self.get_sqlite_version()

        if v >= 35:
            # RETURNING clause available
            return self._pop_returning

        else:
            # RETURNING clause unavailable
            # use custom locking logic
            return self._pop_transaction

    def put(self, data: str) -> Message:
        """
        Insert a new message
        """
        # timeout: int = None
        message_id = str(uuid7())
        now = time_ns()

        _cursor = self.conn.execute(  # noqa
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
        # this should happen all inside a single transaction
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

            return Message(**message)

    def _pop_transaction(self) -> Message | None:
        """
        Pop from the queue using a transaction and custom locking logic.
        This function should be used with SQLite versions < 3.35.0
        since that's when the UPDATE ... RETURNING clause was introduced.
        """

        # lastrowid not working as I expected when executing
        # updates inside a transaction

        # this should happen all inside a single transaction
        with self.transaction(mode="IMMEDIATE"):
            # the `pop` action happens in 3 steps that happen inside a transaction
            # 1: select the first undone message
            # 2: lock the message to avoid another process from getting it too
            # 3: return the selected message
            # I think there's a chance that 2 processes lock the same row, there are 2
            # mechanisms to deal with it:
            # * Using the "IMMEDIATE" mode for the transaction, which locks the database immediately.
            # * When doing the UPDATE statement, the condition checks the status again.
            message = self.conn.execute(
                f"""
            SELECT * FROM {self.table}
            WHERE rowid = (SELECT rowid
                           FROM {self.table}
                           WHERE status = {MessageStatus.READY.value}
                           ORDER BY message_id
                           LIMIT 1)
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
                WHERE message_id = :message_id AND status = {MessageStatus.READY.value}
                """.strip(),
                {
                    "lock_time": lock_time,
                    "message_id": message["message_id"],
                },
            )

            # We have updated the status in the databases, we will manually set
            # it in the returned object before returning it to the user
            return Message(
                data=message["data"],
                message_id=message["message_id"],
                status=MessageStatus.LOCKED,
                in_time=message["in_time"],
                lock_time=lock_time,
                done_time=message["done_time"],
            )

    def peek(self) -> Message | None:
        "Show next message to be popped, if any."

        value = self.conn.execute(
            f"SELECT * FROM {self.table} WHERE status = {MessageStatus.READY.value} ORDER BY message_id LIMIT 1",
        ).fetchone()

        return Message(**value) if value is not None else None

    def get(self, message_id: str) -> Message | None:
        "Get a message by its `message_id`"

        value = self.conn.execute(
            f"SELECT * FROM {self.table} WHERE message_id = :message_id",
            {"message_id": message_id},
        ).fetchone()

        return Message(**value) if value is not None else None

    def done(self, message_id: str) -> int | None:
        """
        Mark message as done.
        If executed multiple times, `done_time` will be
        the last time this function is called.
        """

        now = time_ns()

        x = self.conn.execute(
            f"""
            UPDATE {self.table} SET
              status = {MessageStatus.DONE.value}
              , done_time = :now
            WHERE message_id = :message_id
            """.strip(),
            {"now": now, "message_id": message_id},
        ).lastrowid

        return x

    def mark_failed(self, message_id: str) -> int | None:
        """
        Mark a message as failed.
        """

        x = self.conn.execute(
            f"""
            UPDATE {self.table} SET
              status = {MessageStatus.FAILED.value}
              , done_time = :now
            WHERE message_id = :message_id
            """.strip(),
            {"now": time_ns(), "message_id": message_id},
        ).lastrowid

        return x

    def list_locked(self, threshold_seconds: int) -> Iterator[Message]:
        """
        Return all the tasks that have been in the `LOCKED` state for more than
        `threshold_seconds` seconds.
        """

        threshold_nanoseconds = threshold_seconds * 1e9

        cursor = self.conn.execute(
            f"""
            SELECT * FROM {self.table}
            WHERE
              status = {MessageStatus.LOCKED.value}
              AND  lock_time < :time_value
            """.strip(),
            {"time_value": time_ns() - threshold_nanoseconds},
        )

        for result in cursor:
            yield Message(**result)

    def list_failed(self) -> Iterator[Message]:
        """
        Return all the tasks in `FAILED` state.
        """

        cursor = self.conn.execute(
            f"""
            SELECT * FROM {self.table}
            WHERE
              status = {MessageStatus.FAILED.value}
            """.strip()
        )

        for result in cursor:
            yield Message(**result)

    def retry(self, message_id: str) -> int | None:
        """
        Mark a locked message as free again.
        """

        x = self.conn.execute(
            f"""
            UPDATE {self.table} SET
              status = {MessageStatus.READY.value}
              , done_time = NULL
            WHERE message_id = :message_id
            """.strip(),
            {"message_id": message_id},
        ).lastrowid

        return x

    def qsize(self) -> int:
        """
        Get current size of the queue.
        """

        cursor = self.conn.execute(
            f"""
        SELECT COUNT(*) FROM {self.table}
        WHERE status NOT IN ({MessageStatus.DONE.value}, {MessageStatus.FAILED.value})
        """.strip()
        )

        return next(cursor)[0]

    def empty(self) -> bool:
        """
        Return True if the queue is empty.
        """

        value = self.conn.execute(
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

        value = self.conn.execute(
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
        self.conn.execute("VACUUM;")

    # SQLite works better in autocommit mode when using short DML (INSERT /
    # UPDATE / DELETE) statements
    @contextmanager
    def transaction(self, mode: str = "DEFERRED") -> Iterator[None]:
        if mode not in {"DEFERRED", "IMMEDIATE", "EXCLUSIVE"}:
            raise ValueError(f"Transaction mode '{mode}' is not valid")
        # We must issue a "BEGIN" explicitly when running in auto-commit mode.
        self.conn.execute(f"BEGIN {mode}")
        try:
            # Yield control back to the caller.
            yield
        except BaseException as e:
            self.conn.rollback()  # Roll back all changes if an exception occurs.
            raise e
        else:
            self.conn.commit()

    def __repr__(self) -> str:
        display_items = [
            Message(**x)
            for x in self.conn.execute(f"SELECT * FROM {self.table} LIMIT 3").fetchall()
        ]
        return f"{type(self).__name__}(Connection={self.conn!r}, items={pprint.pformat(display_items)})"

    def close(self) -> None:
        self.conn.close()


# Kept for backwards compatibility
SQLQueue = LiteQueue
