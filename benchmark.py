import argparse
import gc
import sqlite3
import statistics
import sys
import time
import timeit
from collections.abc import Callable
from pathlib import Path
from queue import Queue
from random import choice
from string import ascii_lowercase
from string import printable

from litequeue import LiteQueue


def random_string(
    string_length: int = 10,
    fuzz: bool = False,
    space: bool = False,
) -> str:
    """Generate a random string of fixed length."""
    letters = ascii_lowercase
    if space:
        letters += " "
    if fuzz:
        letters = printable
    return "".join(choice(letters) for _ in range(string_length))


def display_timings(label: str, timings: list[float], number: int) -> None:
    seconds_per_loop = [duration / number for duration in timings]
    mean = statistics.mean(seconds_per_loop)
    deviation = 0.0
    if len(seconds_per_loop) > 1:
        deviation = statistics.stdev(seconds_per_loop)
    print(f"{label}: {mean * 1_000_000:.2f} µs ± {deviation * 1_000_000:.2f} µs per loop")


def benchmark(label: str, operation: Callable[[], object], number: int, repeat: int) -> None:
    gc.collect()
    timings = timeit.repeat(operation, number=number, repeat=repeat)
    display_timings(label, timings, number)


def cleanup_database(database_path: Path) -> None:
    for suffix in ("", "-shm", "-wal"):
        candidate = Path(f"{database_path}{suffix}")
        if candidate.exists():
            candidate.unlink()


def benchmark_puts(number: int, repeat: int) -> None:
    standard_queue: Queue[str] = Queue()
    benchmark(
        "queue.Queue put",
        lambda: standard_queue.put(random_string(20)),
        number,
        repeat,
    )

    database_path = Path("test.sqlite3")
    cleanup_database(database_path)
    persistent_queue = LiteQueue(filename=database_path, maxsize=None)
    benchmark(
        "LiteQueue put (disk)",
        lambda: persistent_queue.put(random_string(20)),
        number,
        repeat,
    )
    persistent_queue.close()
    cleanup_database(database_path)


def benchmark_completion(number: int, repeat: int) -> None:
    standard_queue: Queue[str] = Queue()

    def complete_standard_task() -> None:
        task_id = random_string(20)
        standard_queue.put(task_id)
        standard_queue.get()
        standard_queue.task_done()

    benchmark("queue.Queue complete task", complete_standard_task, number, repeat)

    database_path = Path("completion.sqlite3")
    cleanup_database(database_path)
    lite_queue = LiteQueue(filename=database_path, maxsize=None)

    def complete_litequeue_task() -> None:
        task_id = random_string(20)
        lite_queue.put(task_id)
        task = lite_queue.pop()
        assert task is not None
        lite_queue.done(task.message_id)

    benchmark("LiteQueue complete task", complete_litequeue_task, number, repeat)
    lite_queue.close()
    cleanup_database(database_path)


def benchmark_pop_method(label: str, method_name: str, item_count: int) -> None:
    database_path = Path("pop_bench.sqlite3")
    cleanup_database(database_path)
    queue = LiteQueue(filename=database_path, maxsize=None)
    queue.pop = getattr(queue, method_name)

    prefill_count = max(10_000, item_count)
    for _ in range(prefill_count):
        queue.put(random_string(60))

    gc.collect()
    start = time.perf_counter()
    for _ in range(item_count):
        queue.pop()
    duration = time.perf_counter() - start
    operations_per_second = item_count / duration
    microseconds_per_operation = duration / item_count * 1_000_000

    print(
        f"{label}: {duration:.3f} seconds for {item_count} messages, "
        f"{operations_per_second:,.0f} operations/second, "
        f"{microseconds_per_operation:.2f} µs/operation"
    )
    queue.close()
    cleanup_database(database_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark litequeue against queue.Queue")
    parser.add_argument(
        "--number",
        type=int,
        default=10_000,
        help="Operations per repeat. Default: %(default)s",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=7,
        help="Number of repeats. Default: %(default)s",
    )
    parser.add_argument(
        "--pop-items",
        type=int,
        default=8_000,
        help="Messages used for each pop implementation. Default: %(default)s",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print(f"SQLite {sqlite3.sqlite_version}")
    benchmark_puts(args.number, args.repeat)
    benchmark_completion(args.number, args.repeat)
    benchmark_pop_method("LiteQueue pop with RETURNING", "_pop_returning", args.pop_items)
    benchmark_pop_method(
        "LiteQueue pop with transaction",
        "_pop_transaction",
        args.pop_items,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
