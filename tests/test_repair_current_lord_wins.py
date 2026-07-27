from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from collect_api_first import ApiCollection
from tools.repair_current_lord_wins import repair_latest_snapshot


class RepairCurrentLordWinsTests(unittest.TestCase):
    def test_repairs_latest_snapshot_without_creating_date(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "ratings.sqlite"
            conn = sqlite3.connect(db)
            conn.executescript(
                """
                CREATE TABLE snapshots(
                    snapshot_id INTEGER PRIMARY KEY,
                    filename TEXT NOT NULL,
                    ts INTEGER NOT NULL,
                    player_count INTEGER NOT NULL
                );
                CREATE TABLE observations(
                    snapshot_id INTEGER NOT NULL,
                    pid INTEGER NOT NULL,
                    PRIMARY KEY(snapshot_id,pid)
                );
                INSERT INTO snapshots VALUES(1,'heroes_2026-07-26_20-00-00.json.gz',1,2);
                INSERT INTO snapshots VALUES(2,'heroes_2026-07-27_20-00-00.json.gz',2,2);
                INSERT INTO observations VALUES(1,10);
                INSERT INTO observations VALUES(1,11);
                INSERT INTO observations VALUES(2,10);
                INSERT INTO observations VALUES(2,11);
                """
            )
            conn.commit()
            conn.close()

            collection = ApiCollection(
                heroes={
                    10: {"Побед над Владыкой": 140},
                    11: {"Побед над Владыкой": 0},
                },
                endpoint="https://playwekings.mobi/heroes/for-glory",
                attempts_used=1,
                meta={},
            )
            with patch(
                "tools.repair_current_lord_wins.collector.load_cookie_config",
                return_value=({"wekings_session": "x"}, "https://playwekings.mobi/"),
            ), patch(
                "tools.repair_current_lord_wins.collector.fetch_from_bulk_api",
                return_value=collection,
            ):
                result = repair_latest_snapshot(db, api_min_players=1)

            self.assertFalse(result["new_snapshot_created"])
            self.assertEqual(result["snapshot"], "heroes_2026-07-27_20-00-00.json.gz")
            conn = sqlite3.connect(db)
            try:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0], 2)
                values = dict(
                    conn.execute(
                        "SELECT pid,lord_wins FROM observations WHERE snapshot_id=2"
                    ).fetchall()
                )
                self.assertEqual(values, {10: 140, 11: 0})
                previous = dict(
                    conn.execute(
                        "SELECT pid,lord_wins FROM observations WHERE snapshot_id=1"
                    ).fetchall()
                )
                self.assertEqual(previous, {10: None, 11: None})
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
