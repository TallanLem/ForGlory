import importlib.util
from datetime import datetime, timezone
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "tools" / "forglory_maintainer.py"
spec = importlib.util.spec_from_file_location("forglory_maintainer", MODULE)
maintainer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(maintainer)


class MaintainerTimingTests(unittest.TestCase):
    def test_before_22_moscow_is_early(self):
        self.assertFalse(maintainer.is_collection_due(
            datetime(2026, 11, 4, 18, 59, 59, tzinfo=timezone.utc), "2026-11-03"))

    def test_22_moscow_is_due(self):
        self.assertTrue(maintainer.is_collection_due(
            datetime(2026, 11, 4, 19, 0, 0, tzinfo=timezone.utc), "2026-11-03"))

    def test_same_day_cannot_dispatch_twice(self):
        self.assertFalse(maintainer.is_collection_due(
            datetime(2026, 11, 4, 20, 0, 0, tzinfo=timezone.utc), "2026-11-04"))

    def test_next_day_does_not_dispatch_at_midnight(self):
        self.assertFalse(maintainer.is_collection_due(
            datetime(2026, 11, 4, 21, 0, 0, tzinfo=timezone.utc), "2026-11-04"))

    def test_next_day_after_22_dispatches(self):
        self.assertTrue(maintainer.is_collection_due(
            datetime(2026, 11, 5, 19, 0, 0, tzinfo=timezone.utc), "2026-11-04"))

    def test_runner_rotation_held_during_collection_window(self):
        self.assertTrue(maintainer.hold_for_collection(
            datetime(2026, 11, 4, 18, 59, tzinfo=timezone.utc), 17200, 17100))

    def test_runner_rotation_allowed_after_window(self):
        self.assertFalse(maintainer.hold_for_collection(
            datetime(2026, 11, 4, 19, 5, tzinfo=timezone.utc), 17200, 17100))

    def test_runner_rotation_never_exceeds_hard_cap(self):
        self.assertFalse(maintainer.hold_for_collection(
            datetime(2026, 11, 4, 18, 59, tzinfo=timezone.utc), 21000, 17100))


if __name__ == "__main__":
    unittest.main()
