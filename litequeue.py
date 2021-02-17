import pprint
from typing import Callable, Tuple, Dict, Optional, Union
import sqlite3
from contextlib import contextmanager

__version__ = "0.2.1"

# SQLite works better in autocommit mode when using short DML (INSERT / UPDATE / DELETE) statements
# source: https://charlesleifer.com/blog/going-fast-with-sqlite-and-python/
@contextmanager
def transaction(conn: sqlite3.Connection, mode: str = "DEFERRED"):
    # We must issue a "BEGIN" explicitly when running in auto-commit mode.
    # NOTE: f-strings are not recommended for SQL, but in this case it's something
    # internal to the queue that is never exposed to the end-user of the object
    conn.execute(f"BEGIN {mode}")
    try:
        # Yield control back to the caller.
        yield conn
    except:
        conn.rollback()  # Roll back all changes if an exception occurs.
        raise
    else:
        conn.commit()


class SQLQueue:
    def __init__(
        self,
        dbname=":memory:",
        maxsize: Optional[int] = None,
        check_same_thread=False,
        fast=True,
        **kwargs,
    ):
        self.dbname = dbname
        self.conn = sqlite3.connect(
            self.dbname,
            check_same_thread=check_same_thread,
            isolation_level=None,
            **kwargs,
        )
        self.maxsize = maxsize

        self.conn.row_factory = sqlite3.Row

        # status 0: free, 1: locked, 2: done

        with transaction(self.conn) as c:
            # int == bool in SQLite
            # will have rowid as primary key by default
            c.execute(
                """CREATE TABLE IF NOT EXISTS Queue 
                ( message TEXT NOT NULL,
                  task_id TEXT,
                  status INTEGER,
                  in_time INTEGER NOT NULL,
                  lock_time INTEGER,
                  done_time INTEGER )
                """
            )

            c.execute("CREATE INDEX IF NOT EXISTS TIdx ON Queue(task_id)")
            c.execute("CREATE INDEX IF NOT EXISTS SIdx ON Queue(status)")

        if fast:
            self.conn.execute("PRAGMA journal_mode = 'WAL';")
            self.conn.execute("PRAGMA temp_store = 2;")
            self.conn.execute("PRAGMA synchronous = 1;")
            self.conn.execute(f"PRAGMA cache_size = {-1 * 64_000};")

        if maxsize is not None:
            self.conn.execute(
                f"""
CREATE TRIGGER IF NOT EXISTS maxsize_control 
   BEFORE INSERT
   ON Queue
   WHEN (SELECT COUNT(*) FROM Queue WHERE status = 0) >= {self.maxsize}
BEGIN
    SELECT RAISE (ABORT,'Max queue length reached');
END;"""
            )

    def put(self, message: str, timeout: int = None) -> int:
        "Insert a new task"

        rid = self.conn.execute(
            "INSERT INTO Queue VALUES (:message, lower(hex(randomblob(16))), 0, strftime('%s','now'), NULL, NULL)",
            {"message": message},
        ).lastrowid

        return rid

    def pop(self) -> Optional[Dict[str, Union[int, str]]]:

        # lastrowid not working as I expected when executing
        # updates inside a transaction

        # this should happen all inside a single transaction
        with transaction(self.conn, mode="IMMEDIATE") as c:
            # the `pop` action happens in 3 steps that happen inside a transaction
            # 1: select the first undone task
            # 2: lock the task to avoid another process from getting it too
            # 3: return the selected task
            # I think there's a chance that 2 processes lock the same row, there are 2
            # mechanisms to deal with it:
            # * Using the "IMMEDIATE" mode for the transaction, which locks the database immediately.
            # * When doing the UPDATE statement, the condition checks the status again.
            task = c.execute(
                """
            SELECT message, task_id FROM Queue
            WHERE rowid = (SELECT min(rowid) FROM Queue
                           WHERE status = 0)
            """
            ).fetchone()

            if task is None:
                return None

            c.execute(
                """
UPDATE Queue SET status = 1, lock_time = strftime('%s','now') WHERE task_id = :task_id AND status = 0
""",
                {"task_id": task["task_id"]},
            )

            return dict(task)

    def peek(self) -> Dict:
        "Show next task to be popped."
        # order by should not be really needed
        value = self.conn.execute(
            "SELECT * FROM Queue WHERE status = 0 ORDER BY rowid LIMIT 1"
        ).fetchone()
        return dict(value)

    def get(self, task_id: str) -> Optional[Dict[str, Union[int, str]]]:
        "Get a task by its `task_id`"

        value = self.conn.execute(
            "SELECT * FROM Queue WHERE task_id = :task_id", {"task_id": task_id}
        ).fetchone()

        return dict(value) if value is not None else value

    def done(self, task_id) -> int:
        """
        Mark task as done.
        If executed multiple times, `done_time` will be
        the last time this function is called.
        """

        x = self.conn.execute(
            "UPDATE Queue SET status = 2,  done_time = strftime('%s','now') WHERE task_id = :task_id",
            {"task_id": task_id},
        ).lastrowid
        return x

    def qsize(self) -> int:
        return next(self.conn.execute("SELECT COUNT(*) FROM Queue WHERE status != 2"))[
            0
        ]

    def empty(self) -> bool:

        value = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM Queue WHERE status = 0"
        ).fetchone()
        return not bool(value["cnt"])

    def full(self) -> bool:
        raise NotImplementedError

    def prune(self):

        self.conn.execute("DELETE FROM Queue WHERE status = 2")
        self.conn.execute("VACUUM;")

        return

    def __repr__(self):
        return f"{type(self).__name__}(dbname={self.dbname!r}, items={pprint.pformat([dict(x) for x in self.conn.execute('SELECT * FROM Queue').fetchall()])})"

    def close(self):
        self.conn.close()
