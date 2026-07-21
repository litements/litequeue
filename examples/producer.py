#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "litequeue>=0.11",
# ]
# ///

import json
import time
from pathlib import Path

from litequeue import LiteQueue

QUEUE_FILE = Path(__file__).with_name("tasks.sqlite3")
TASK_COUNT = 20


def main() -> None:
    queue = LiteQueue(filename=QUEUE_FILE)

    try:
        for task_number in range(1, TASK_COUNT + 1):
            payload = json.dumps({"task_number": task_number})
            message = queue.put(payload)
            print(f"Produced task {task_number}: {message.message_id}")
            time.sleep(1)
    finally:
        queue.close()


if __name__ == "__main__":
    main()
