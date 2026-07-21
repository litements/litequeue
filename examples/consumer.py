#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "litequeue>=0.12",
# ]
# ///

import json
import random
import time
from pathlib import Path

from litequeue import LiteQueue

QUEUE_FILE = Path.cwd() / "tasks.sqlite3"
IDLE_TIMEOUT_SECONDS = 3
MAX_ATTEMPTS = 3
FAILURE_RATE = 0.2


def process_task(data: str) -> int:
    payload = json.loads(data)
    task_number = payload["task_number"]
    processing_time = random.uniform(0.1, 0.9)
    time.sleep(processing_time)

    if random.random() < FAILURE_RATE:
        raise RuntimeError("simulated processing failure")

    return task_number


def main() -> None:
    queue = LiteQueue(filename=QUEUE_FILE)
    attempts: dict[str, int] = {}
    idle_since = time.monotonic()

    try:
        while time.monotonic() - idle_since < IDLE_TIMEOUT_SECONDS:
            message = queue.pop()
            if message is None:
                time.sleep(0.1)
                continue

            idle_since = time.monotonic()
            message_id = message.message_id
            attempt = attempts.get(message_id, 0) + 1
            attempts[message_id] = attempt

            try:
                task_number = process_task(message.data)
            except Exception as error:
                if attempt < MAX_ATTEMPTS:
                    queue.retry(message_id)
                    print(f"Retrying {message_id} after attempt {attempt}: {error}")
                    continue

                queue.mark_failed(message_id)
                print(f"Failed {message_id} after {attempt} attempts: {error}")
                continue

            queue.done(message_id)
            attempts.pop(message_id, None)
            print(f"Completed task {task_number} on attempt {attempt}")
    finally:
        queue.close()

    print("Queue stayed empty for 3 seconds; consumer stopped.")


if __name__ == "__main__":
    main()
