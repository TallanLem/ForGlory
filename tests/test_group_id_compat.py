from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from forglory import _repair_missing_group_ids


class GroupIdCompatibilityTests(unittest.TestCase):
    def test_named_groups_without_game_ids_are_restored(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ratings.sqlite"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE text_values(
                    text_id INTEGER PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE observations(
                    snapshot_id INTEGER NOT NULL,
                    pid INTEGER NOT NULL,
                    clan_name_id INTEGER,
                    clan_game_id INTEGER,
                    brotherhood_name_id INTEGER,
                    brotherhood_game_id INTEGER
                );
                INSERT INTO text_values(text_id,value) VALUES
                    (1,'Клан Альфа'),
                    (2,'Братство Бета'),
                    (3,'не состоит'),
                    (4,'Клан с реальным ID');
                INSERT INTO observations VALUES
                    (10,101,1,NULL,2,0),
                    (10,102,3,0,3,NULL),
                    (10,103,4,777,NULL,NULL),
                    (9,101,1,0,2,NULL);
                """
            )
            conn.commit()
            conn.close()

            self.assertEqual(_repair_missing_group_ids(db_path), (2, 2))

            conn = sqlite3.connect(db_path)
            rows = conn.execute(
                """
                SELECT snapshot_id,pid,clan_game_id,brotherhood_game_id
                FROM observations
                ORDER BY snapshot_id DESC,pid
                """
            ).fetchall()
            conn.close()

            self.assertEqual(
                rows,
                [
                    (10, 101, -1, -2),
                    (10, 102, 0, None),
                    (10, 103, 777, None),
                    (9, 101, -1, -2),
                ],
            )
            self.assertEqual(_repair_missing_group_ids(db_path), (0, 0))

    def test_unknown_or_incompatible_database_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ratings.sqlite"
            sqlite3.connect(db_path).close()
            self.assertEqual(_repair_missing_group_ids(db_path), (0, 0))


if __name__ == "__main__":
    unittest.main()
