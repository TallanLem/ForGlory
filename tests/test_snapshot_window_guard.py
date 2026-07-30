from __future__ import annotations

import importlib.util
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "check_snapshot_window.py"
spec = importlib.util.spec_from_file_location("check_snapshot_window", MODULE_PATH)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)

MOSCOW = ZoneInfo("Europe/Moscow")


class SnapshotWindowGuardTests(unittest.TestCase):
    def make_db(self, path: Path, wall_clock: datetime) -> None:
        conn = sqlite3.connect(path)
        try:
            conn.execute(
                "CREATE TABLE snapshots(snapshot_id INTEGER PRIMARY KEY,filename TEXT,ts INTEGER)"
            )
            conn.execute(
                "INSERT INTO snapshots(filename,ts) VALUES(?,?)",
                (
                    "heroes_2026-07-30_21-32-00.json.gz",
                    module._wall_clock_timestamp(wall_clock),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def test_skips_snapshot_within_five_hours_across_midnight(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "ratings.sqlite"
            self.make_db(db, datetime(2026, 7, 30, 23, 30, tzinfo=MOSCOW))
            result = module.evaluate(
                db,
                window_hours=5,
                now=datetime(2026, 7, 31, 0, 30, tzinfo=MOSCOW),
            )
            self.assertEqual(result["should_run"], "false")
            self.assertEqual(result["delta_seconds"], "3600")

    def test_runs_when_latest_snapshot_is_more_than_five_hours_away(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "ratings.sqlite"
            self.make_db(db, datetime(2026, 7, 30, 0, 30, tzinfo=MOSCOW))
            result = module.evaluate(
                db,
                window_hours=5,
                now=datetime(2026, 7, 30, 23, 30, tzinfo=MOSCOW),
            )
            self.assertEqual(result["should_run"], "true")
            self.assertEqual(result["reason"], "outside_window")

    def test_force_bypasses_window(self) -> None:
        result = module.evaluate(
            Path("missing.sqlite"),
            window_hours=5,
            now=datetime(2026, 7, 30, 22, 0, tzinfo=MOSCOW),
            force=True,
        )
        self.assertEqual(result["should_run"], "true")
        self.assertEqual(result["reason"], "force")


if __name__ == "__main__":
    unittest.main()
