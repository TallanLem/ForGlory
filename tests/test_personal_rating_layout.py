from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path

import forglory


ROOT = Path(__file__).resolve().parents[1]


class PersonalRatingLayoutTests(unittest.TestCase):
    def test_personal_stats_adds_rank_in_best_growth_rating(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            conn.executescript(
                """
                CREATE TABLE snapshots(
                    snapshot_id INTEGER PRIMARY KEY,
                    ts INTEGER NOT NULL
                );
                CREATE TABLE best_growth(
                    best_for_snapshot_id INTEGER NOT NULL,
                    param TEXT NOT NULL,
                    pid INTEGER NOT NULL,
                    diff INTEGER NOT NULL
                );
                INSERT INTO snapshots VALUES(10,1000);
                INSERT INTO best_growth VALUES(10,'Слава',2,20);
                INSERT INTO best_growth VALUES(10,'Слава',7,20);
                INSERT INTO best_growth VALUES(10,'Слава',9,15);
                INSERT INTO best_growth VALUES(10,'Побед',7,4);
                INSERT INTO best_growth VALUES(10,'Побед',3,5);
                """
            )

            def original_query(pid: int, _snap_from: str, _snap_to: str):
                return {
                    "pid": pid,
                    "rows": [
                        {"param": "Слава", "best_diff": 20},
                        {"param": "Побед", "best_diff": 4},
                        {"param": "Поражений", "best_diff": None},
                    ],
                }

            namespace = {
                "query_personal_stats": original_query,
                "get_db": lambda: conn,
            }
            self.assertTrue(forglory._patch_personal_stats_query(namespace))

            result = namespace["query_personal_stats"](7, "from", "to")
            ranks = {row["param"]: row["best_rank"] for row in result["rows"]}
            self.assertEqual(ranks["Слава"], 2)
            self.assertEqual(ranks["Побед"], 2)
            self.assertIsNone(ranks["Поражений"])
        finally:
            conn.close()

    def test_profile_template_uses_compact_four_column_layout(self) -> None:
        template = (ROOT / "templates" / "profile.html").read_text(encoding="utf-8")
        self.assertNotIn("<th>Было</th>", template)
        self.assertNotIn("<th>Стало</th>", template)
        self.assertNotIn("Лучшее за 30 дней", template)
        self.assertNotIn('class="profile-period"', template)
        self.assertIn("Общее<br><small>(место)</small>", template)
        self.assertIn("Прирост<br><small>(место)</small>", template)
        self.assertIn("Лучшее<br><small>(место)</small>", template)
        self.assertIn('data-label="Общее (место)"', template)
        self.assertIn("row.best_rank", template)


if __name__ == "__main__":
    unittest.main()
