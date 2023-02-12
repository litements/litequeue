import pprint
import pathlib
from typing import Callable, Dict, Optional, Union
import sqlite3
from contextlib import contextmanager
from enum import Enum

__version__ = "0.6"


class MessageStatus(int, Enum):
    READY = 0
    LOCKED = 1
    DONE = 2
    FAILED = 3


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
                ( message TEXT NOT NULL,
                  message_id TEXT,
                  status INTEGER,
                  in_time INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                  lock_time INTEGER,
                  done_time INTEGER )
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

    def put(self, message: str) -> int:
        # timeout: int = None
        """
        Insert a new message
        """

        x = self.conn.execute(
            "INSERT INTO Queue VALUES (:message, lower(hex(randomblob(16))), 0, strftime('%s','now'), NULL, NULL)",
            {"message": message},
        ).lastrowid

        assert x
        return x

    def _pop_returning(self) -> Optional[Dict[str, Union[int, str]]]:
        # this should happen all inside a single transaction
        with self.transaction(mode="IMMEDIATE"):
            message = self.conn.execute(
                """
UPDATE Queue SET status = 1, lock_time = strftime('%s','now')
WHERE rowid = (SELECT min(rowid) FROM Queue
                WHERE status = :status)
RETURNING *;
""",
                {"status": MessageStatus.READY},
            ).fetchone()

            if not message:
                return None

            return dict(message)

    def _pop_transaction(self) -> Optional[Dict[str, Union[int, str]]]:
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
                """
                UPDATE Queue SET
                  status = 1
                  , lock_time = strftime('%s','now')
                WHERE message_id = :message_id AND status = :status
                """.strip(),
                {"status": MessageStatus.READY, "message_id": message["message_id"]},
            )

            return dict(message)

    def peek(self) -> Dict:
        "Show next message to be popped."

        value = self.conn.execute(
            "SELECT * FROM Queue WHERE status = :status ORDER BY in_time LIMIT 1",
            {"status": MessageStatus.READY},
        ).fetchone()
        return dict(value)

    def get(self, message_id: str) -> Optional[Dict[str, Union[int, str]]]:
        "Get a message by its `message_id`"

        value = self.conn.execute(
            "SELECT * FROM Queue WHERE message_id = :message_id",
            {"message_id": message_id},
        ).fetchone()

        return dict(value) if value is not None else value

    def done(self, message_id) -> int:
        """
        Mark message as done.
        If executed multiple times, `done_time` will be
        the last time this function is called.
        """

        x = self.conn.execute(
            """
            UPDATE Queue SET
              status = :status
              , done_time = strftime('%s','now')
            WHERE message_id = :message_id
            """.strip(),
            {"status": MessageStatus.DONE, "message_id": message_id},
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
              , done_time = strftime('%s','now')
            WHERE message_id = :message_id
            """.strip(),
            {"status": MessageStatus.FAILED, "message_id": message_id},
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
            dict(x) for x in self.conn.execute("SELECT * FROM Queue LIMIT 3").fetchall()
        ]
        return f"{type(self).__name__}(Connection={self.conn!r}, items={pprint.pformat(display_items)})"

    def close(self):
        self.conn.close()


# Kept for backwards compatibility
SQLQueue = LiteQueue
