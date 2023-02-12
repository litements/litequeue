import pprint
import pathlib
from typing import Callable, Dict, Iterable, Optional, Union, Any, cast
import sqlite3
from contextlib import contextmanager
from enum import Enum
from dataclasses import dataclass
import sys
from uuid import UUID
import time
import os

_DKW: Dict[str, Any] = {}
if sys.version_info >= (3, 10):
    _DKW["slots"] = True

__version__ = "0.6"

# Extracted from https://github.com/stevesimmons/uuid7 under MIT license

# Expose function used by uuid7() to get current time in nanoseconds
# since the Unix epoch.
time_ns = time.time_ns


def _now() -> int:
    return int(time.time())


def uuid7(
    as_type: Optional[str] = None,
    time_func: Callable[[], int] = time_ns,
    _last=[0, 0, 0, 0],  # noqa
    _last_as_of=[0, 0, 0, 0],  # noqa
) -> Union[UUID, str, int, bytes]:
    """
    UUID v7, following the proposed extension to RFC4122 described in
    https://www.ietf.org/id/draft-peabody-dispatch-new-uuid-format-02.html.
    All representations (string, byte array, int) sort chronologically,
    with a potential time resolution of 50ns (if the system clock
    supports this).
    Parameters
    ----------
    as_type - Optional string to return the UUID in a different format.
                A uuid.UUID (version 7, variant 0x10) is returned unless
                this is one of 'str', 'int', 'hex' or 'bytes'.
    time_func - Set the time function, which must return integer
                nanoseconds since the Unix epoch, midnight on 1-Jan-1970.
                Defaults to time.time_ns(). This is exposed because
                time.time_ns() may have a low resolution on Windows.
    _last and _last_as_of - Used internally to trigger incrementing a
                sequence counter when consecutive calls have the same time
                values. The values [t1, t2, t3, seq] are described below.
    Returns
    -------
    A UUID object, or if as_type is specified, a string, int or
    bytes of length 16.
    Implementation notes
    --------------------
    The 128 bits in the UUID are allocated as follows:
    - 36 bits of whole seconds
    - 24 bits of fractional seconds, giving approx 50ns resolution
    - 14 bits of sequential counter, if called repeatedly in same time tick
    - 48 bits of randomness
    plus, at locations defined by RFC4122, 4 bits for the
    uuid version (0b111) and 2 bits for the uuid variant (0b10).
             0                   1                   2                   3
             0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    t1      |                 unixts (secs since epoch)                     |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    t2/t3   |unixts |  frac secs (12 bits)  |  ver  |  frac secs (12 bits)  |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    t4/rand |var|       seq (14 bits)       |          rand (16 bits)       |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    rand    |                          rand (32 bits)                       |
            +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    Indicative timings:
    - uuid.uuid4()            2.4us
    - uuid7()                 3.7us
    - uuid7(as_type='int')    1.6us
    - uuid7(as_type='str')    2.5us
    Examples
    --------
    >>> uuid7()
    UUID('061cb26a-54b8-7a52-8000-2124e7041024')
    >>> uuid7(0)
    UUID('00000000-0000-0000-0000-00000000000')
    >>> for fmt in ('bytes', 'hex', 'int', 'str', 'uuid', None):
    ...     print(fmt, repr(uuid7(as_type=fmt)))
    bytes b'\x06\x1c\xb8\xfe\x0f\x0b|9\x80\x00\tjt\x85\xb3\xbb'
    hex '061cb8fe0f0b7c3980011863b956b758'
    int 8124504378724980906989670469352026642
    str '061cb8fe-0f0b-7c39-8003-d44a7ee0bdf6'
    uuid UUID('061cb8fe-0f0b-7c39-8004-0489578299f6')
    None UUID('061cb8fe-0f0f-7df2-8000-afd57c2bf446')
    """
    ns = time_func()
    last = _last

    if ns == 0:
        # Special cose for all-zero uuid. Strictly speaking not a UUIDv7.
        t1 = t2 = t3 = t4 = 0
        rand = b"\0" * 6
    else:
        # Treat the first 8 bytes of the uuid as a long (t1) and two ints
        # (t2 and t3) holding 36 bits of whole seconds and 24 bits of
        # fractional seconds.
        # This gives a nominal 60ns resolution, comparable to the
        # timestamp precision in Linux (~200ns) and Windows (100ns ticks).
        sixteen_secs = 16_000_000_000
        t1, rest1 = divmod(ns, sixteen_secs)
        t2, rest2 = divmod(rest1 << 16, sixteen_secs)
        t3, _ = divmod(rest2 << 12, sixteen_secs)
        t3 |= 7 << 12  # Put uuid version in top 4 bits, which are 0 in t3

        # The next two bytes are an int (t4) with two bits for
        # the variant 2 and a 14 bit sequence counter which increments
        # if the time is unchanged.
        if t1 == last[0] and t2 == last[1] and t3 == last[2]:
            # Stop the seq counter wrapping past 0x3FFF.
            # This won't happen in practice, but if it does,
            # uuids after the 16383rd with that same timestamp
            # will not longer be correctly ordered but
            # are still unique due to the 6 random bytes.
            if last[3] < 0x3FFF:
                last[3] += 1
        else:
            last[:] = (t1, t2, t3, 0)
        t4 = (2 << 14) | last[3]  # Put variant 0b10 in top two bits

        # Six random bytes for the lower part of the uuid
        rand = os.urandom(6)

    # Build output
    if as_type == "str":
        return f"{t1:>08x}-{t2:>04x}-{t3:>04x}-{t4:>04x}-{rand.hex()}"

    r = int.from_bytes(rand, "big")
    uuid_int = (t1 << 96) + (t2 << 80) + (t3 << 64) + (t4 << 48) + r
    if as_type == "int":
        return uuid_int
    elif as_type == "hex":
        return f"{uuid_int:>032x}"
    elif as_type == "bytes":
        return uuid_int.to_bytes(16, "big")
    else:
        return UUID(int=uuid_int)


class MessageStatus(int, Enum):
    READY = 0
    LOCKED = 1
    DONE = 2
    FAILED = 3


@dataclass(frozen=True, **_DKW)
class Message:
    data: str
    message_id: UUID
    status: MessageStatus
    in_time: int
    lock_time: Optional[int]
    done_time: Optional[int]


class LiteQueue:
    def __init__(
        self,
        filename_or_conn: Optional[Union[sqlite3.Connection, str, pathlib.Path]] = None,
        memory: bool = False,
        maxsize: Optional[int] = None,
        **kwargs,
    ):
        assert (filename_or_conn is not None and not memory) or (
            filename_or_conn is None and memory
        ), "Either specify a filename_or_conn or pass memory=True"

        if memory or filename_or_conn == ":memory:":
            self.conn = sqlite3.connect(":memory:", isolation_level=None, **kwargs)

        elif isinstance(filename_or_conn, (str, pathlib.Path)):
            self.conn = sqlite3.connect(
                str(filename_or_conn),
                isolation_level=None,
                check_same_thread=False,
                **kwargs,
            )

        else:
            assert filename_or_conn is not None
            self.conn = filename_or_conn
            self.conn.isolation_level = None

        self.maxsize = int(maxsize) if maxsize is not None else maxsize
        self.conn.row_factory = sqlite3.Row

        self.pop: Callable = self._select_pop_func()

        with self.transaction():
            # int == bool in SQLite
            # will have rowid as primary key by default
            self.conn.execute(
                """CREATE TABLE IF NOT EXISTS Queue
                (
                  data       TEXT NOT NULL
                  , message_id TEXT NOT NULL
                  , status     INTEGER NOT NULL
                  , in_time    INTEGER NOT NULL DEFAULT (strftime('%s','now'))
                  , lock_time  INTEGER
                  , done_time  INTEGER
                )
                """
            )

            self.conn.execute("CREATE INDEX IF NOT EXISTS TIdx ON Queue(message_id)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS SIdx ON Queue(status)")

        # if fast:
        self.conn.execute("PRAGMA journal_mode = 'WAL';")
        self.conn.execute("PRAGMA temp_store = 2;")
        self.conn.execute("PRAGMA synchronous = 1;")
        self.conn.execute(f"PRAGMA cache_size = {-1 * 64_000};")

        if self.maxsize is not None:
            self.conn.execute(
                f"""
CREATE TRIGGER IF NOT EXISTS maxsize_control
   BEFORE INSERT
   ON Queue
   WHEN (SELECT COUNT(*) FROM Queue WHERE status = {MessageStatus.READY}) >= {self.maxsize}
BEGIN
    SELECT RAISE (ABORT,'Max queue length reached: {self.maxsize}');
END;"""
            )

    def get_sqlite_version(self) -> int:
        sqlite_ver = sqlite3.sqlite_version.split(".")

        v_major = int(sqlite_ver[0])
        v_min = int(sqlite_ver[1])
        # _v_bug = int(sqlite_ver[2])

        assert v_major == 3

        return v_min

    def _select_pop_func(self) -> Callable:
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
        message_id: str = cast(str, uuid7(as_type="str"))
        now = _now()

        _cursor = self.conn.execute(
            f"""
            INSERT INTO
              Queue(  data,  message_id, status,                 in_time, lock_time, done_time )
            VALUES ( :data, :message_id, {MessageStatus.READY}, :now    , NULL     , NULL      )
            """.strip(),
            {"data": data, "message_id": message_id, "now": now},
        )

        return Message(
            data=data,
            message_id=UUID(message_id),
            status=MessageStatus.READY,
            in_time=now,
            lock_time=None,
            done_time=None,
        )

    def _pop_returning(self) -> Optional[Message]:
        # this should happen all inside a single transaction
        with self.transaction(mode="IMMEDIATE"):
            message = self.conn.execute(
                f"""
UPDATE Queue
SET status = {MessageStatus.LOCKED}, lock_time = :now
WHERE rowid = (SELECT min(rowid) FROM Queue
                WHERE status = :status)
RETURNING *;
""",
                {"status": MessageStatus.READY, "now": _now()},
            ).fetchone()

            if not message:
                return None

            return Message(**message)

    def _pop_transaction(self) -> Optional[Message]:
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
                """
            SELECT message, message_id FROM Queue
            WHERE rowid = (SELECT min(rowid) FROM Queue
                           WHERE status = :status)
            """.strip(),
                {"status": MessageStatus.READY},
            ).fetchone()

            if message is None:
                return None

            self.conn.execute(
                f"""
                UPDATE Queue SET
                  status = {MessageStatus.LOCKED}
                  , lock_time = :now
                WHERE message_id = :message_id AND status = :status
                """.strip(),
                {
                    "now": _now(),
                    "status": MessageStatus.READY,
                    "message_id": message["message_id"],
                },
            )

            return Message(**message)

    def peek(self) -> Optional[Message]:
        "Show next message to be popped, if any."

        value = self.conn.execute(
            "SELECT * FROM Queue WHERE status = :status ORDER BY message_id LIMIT 1",
            {"status": MessageStatus.READY},
        ).fetchone()

        return Message(**value) if value is not None else None

    def get(self, message_id: str) -> Optional[Message]:
        "Get a message by its `message_id`"

        value = self.conn.execute(
            "SELECT * FROM Queue WHERE message_id = :message_id",
            {"message_id": message_id},
        ).fetchone()

        return Message(**value) if value is not None else None

    def done(self, message_id) -> int:
        """
        Mark message as done.
        If executed multiple times, `done_time` will be
        the last time this function is called.
        """

        now = _now()

        x = self.conn.execute(
            """
            UPDATE Queue SET
              status = :status
              , done_time = :now
            WHERE message_id = :message_id
            """.strip(),
            {"status": MessageStatus.DONE, "now": now, "message_id": message_id},
        ).lastrowid

        assert x
        return x

    def mark_failed(self, message_id) -> int:
        """
        Mark a message as failed.
        """

        x = self.conn.execute(
            """
            UPDATE Queue SET
              status = :status
              , done_time = :now
            WHERE message_id = :message_id
            """.strip(),
            {"status": MessageStatus.FAILED, "now": _now(), "message_id": message_id},
        ).lastrowid

        assert x
        return x

    def retry(self, message_id) -> int:
        """
        Mark a locked message as free again.
        """

        x = self.conn.execute(
            """
            UPDATE Queue SET
              status = :status
              , done_time = NULL
            WHERE message_id = :message_id
            """.strip(),
            {"status": MessageStatus.READY, "message_id": message_id},
        ).lastrowid

        assert x
        return x

    def qsize(self) -> int:
        """
        Get current size of the queue.
        """

        cursor = self.conn.execute(
            """
        SELECT COUNT(*) FROM Queue
        WHERE status NOT IN (:status_done, :status_failed)
        """.strip(),
            {"status_done": MessageStatus.DONE, "status_failed": MessageStatus.FAILED},
        )

        return next(cursor)[0]

    def empty(self) -> bool:
        """
        Return True if the queue is empty.
        """

        value = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM Queue WHERE status = :status",
            {"status": MessageStatus.READY},
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
            "SELECT COUNT(*) as cnt FROM Queue WHERE status = :status",
            {"status": MessageStatus.READY},
        ).fetchone()

        if value["cnt"] >= self.maxsize:
            return True
        else:
            return False

    def prune(self):
        """
        Delete `done` messages.
        """

        self.conn.execute(
            "DELETE FROM Queue WHERE status IN (:status_done, :status_failed)",
            {"status_done": MessageStatus.DONE, "status_failed": MessageStatus.FAILED},
        )

        return

    def vacuum(self):
        """
        Vacuum the database.

        IMPORTANT: The `VACUUM` step can take some time to finish depending on
        the size of the queue and how many messages have been deleted.
        """
        self.conn.execute("VACUUM;")

    # SQLite works better in autocommit mode when using short DML (INSERT /
    # UPDATE / DELETE) statements
    @contextmanager
    def transaction(self, mode="DEFERRED"):
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

    def __repr__(self):
        display_items = [
            Message(**x)
            for x in self.conn.execute("SELECT * FROM Queue LIMIT 3").fetchall()
        ]
        return f"{type(self).__name__}(Connection={self.conn!r}, items={pprint.pformat(display_items)})"

    def close(self):
        self.conn.close()


# Kept for backwards compatibility
SQLQueue = LiteQueue
