from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from tools.repair_current_groups_from_endpoint import GroupValue, update_latest_groups


class RepairCurrentGroupsTests(unittest.TestCase):
    def test_updates_existing_latest_snapshot_without_new_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            db_path = Path(temporary) / "ratings.sqlite"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE snapshots(
                    snapshot_id INTEGER PRIMARY KEY,
                    filename TEXT NOT NULL,
                    ts INTEGER NOT NULL
                );
                CREATE TABLE text_values(
                    text_id INTEGER PRIMARY KEY,
                    value TEXT NOT NULL UNIQUE,
                    norm TEXT NOT NULL
                );
                CREATE TABLE observations(
                    snapshot_id INTEGER NOT NULL,
                    pid INTEGER NOT NULL,
                    clan_name_id INTEGER,
                    clan_game_id INTEGER,
                    brotherhood_name_id INTEGER,
                    brotherhood_game_id INTEGER,
                    PRIMARY KEY(snapshot_id,pid)
                );
                INSERT INTO snapshots VALUES(1,'heroes_2026-07-26_21-11-22.json.gz',1);
                INSERT INTO observations(snapshot_id,pid,clan_game_id,brotherhood_game_id)
                VALUES(1,10,0,0),(1,20,0,0);
                """
            )
            conn.commit()
            conn.close()

            update_latest_groups(
                db_path,
                1,
                {
                    10: GroupValue("Clan", 100, "Brotherhood", 200),
                    20: GroupValue(None, 0, None, 0),
                },
            )

            conn = sqlite3.connect(db_path)
            try:
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0],
                    1,
                )
                row = conn.execute(
                    """
                    SELECT cn.value,o.clan_game_id,bn.value,o.brotherhood_game_id
                    FROM observations o
                    LEFT JOIN text_values cn ON cn.text_id=o.clan_name_id
                    LEFT JOIN text_values bn ON bn.text_id=o.brotherhood_name_id
                    WHERE o.snapshot_id=1 AND o.pid=10
                    """
                ).fetchone()
                self.assertEqual(row, ("Clan", 100, "Brotherhood", 200))
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
