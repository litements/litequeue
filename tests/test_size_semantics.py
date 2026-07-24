from pathlib import Path

from litequeue import LiteQueue


def test_locked_work_is_not_ready_backlog(tmp_path: Path) -> None:
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3", maxsize=1)
    queue.put("work")

    locked = queue.pop()

    assert locked is not None
    assert queue.qsize() == 0
    assert queue.empty()
    assert not queue.full()
    assert queue.ready_count() == 0
    assert queue.locked_count() == 1
    assert queue.active_count() == 1
    assert queue.stored_count() == 1


def test_counts_cover_every_message_status(tmp_path: Path) -> None:
    queue = LiteQueue(filename=tmp_path / "queue.sqlite3")
    queue.put("locked")
    queue.put("done")
    queue.put("failed")
    queue.put("ready")

    locked = queue.pop()
    done = queue.pop()
    failed = queue.pop()

    assert locked is not None
    assert done is not None
    assert failed is not None
    queue.done(done.message_id)
    queue.mark_failed(failed.message_id)

    assert queue.ready_count() == 1
    assert queue.locked_count() == 1
    assert queue.done_count() == 1
    assert queue.failed_count() == 1
    assert queue.qsize() == 1
    assert queue.active_count() == 2
    assert queue.stored_count() == 4
    assert not queue.empty()
