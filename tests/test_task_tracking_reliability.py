"""Small unit tests for task dispatch recovery behavior."""

import sys
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common import task_tracking


class _Result:
    def __init__(self, rows=(), rowcount=1, scalar=None):
        self._rows = rows
        self.rowcount = rowcount
        self._scalar = scalar

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def scalar(self):
        return self._scalar

    def scalars(self):
        return self


class _OutboxEngine:
    def __init__(self):
        self.published_update_failed = False
        self.retry_recorded = False

    @contextmanager
    def connect(self):
        yield self

    @contextmanager
    def begin(self):
        yield self

    def execute(self, statement, params=None):
        sql = str(statement)
        if "SELECT outbox_id" in sql:
            return _Result([{
                "outbox_id": 7,
                "item_id": "item-1",
                "exchange_name": "tasks",
                "routing_key": "github",
                "payload": '{"task_id":"task-1","keyword":"alpha"}',
                "attempts": 0,
            }])
        if "SELECT run_id FROM task_run_items" in sql:
            return _Result(scalar="run-1")
        if "SELECT status FROM task_run_items" in sql:
            return _Result(["queued"])
        if "SET published_at" in sql and not self.published_update_failed:
            self.published_update_failed = True
            raise RuntimeError("simulated database failure after publish")
        if "SET attempts=:attempts" in sql:
            self.retry_recorded = True
        return _Result()


class _ItemEngine:
    def __init__(self):
        self.status = "queued"

    @contextmanager
    def begin(self):
        yield self

    def execute(self, statement, params=None):
        sql = str(statement)
        if "UPDATE task_run_items" in sql:
            if self.status in {"pending", "queued", "retrying"}:
                self.status = "running"
                return _Result(rowcount=1)
            return _Result(rowcount=0)
        return _Result()


class TaskTrackingReliabilityTests(unittest.TestCase):
    def test_outbox_retries_after_publish_succeeds_but_status_update_fails(self):
        engine = _OutboxEngine()
        publish = unittest.mock.Mock()

        with patch.object(task_tracking, "engine", engine):
            first_attempt = task_tracking.dispatch_outbox(publish)
            recovered_attempt = task_tracking.dispatch_outbox(publish)

        self.assertEqual(first_attempt, 0)
        self.assertEqual(recovered_attempt, 1)
        self.assertEqual(publish.call_count, 2)
        self.assertTrue(engine.published_update_failed)
        self.assertTrue(engine.retry_recorded)

    def test_duplicate_item_delivery_is_claimed_only_once(self):
        engine = _ItemEngine()

        with patch.object(task_tracking, "engine", engine):
            first = task_tracking.mark_item_started("item-1")
            duplicate = task_tracking.mark_item_started("item-1")

        self.assertTrue(first)
        self.assertFalse(duplicate)


if __name__ == "__main__":
    unittest.main()
