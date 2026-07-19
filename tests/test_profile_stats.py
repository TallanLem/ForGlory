from __future__ import annotations

import gzip
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parents[1]


def hero(pid: int, name: str, glory: int) -> dict:
    return {
        "ID": pid,
        "Имя": name,
        "Уровень": 10,
        "Слава": glory,
        "Побед": glory,
        "Поражений": 0,
        "Побед над Драконом": 0,
        "Побед над Змеем": 0,
        "Убито зверей": 0,
        "Сила": 10,
        "Защита": 10,
        "Ловкость": 10,
        "Мастерство": 10,
        "Живучесть": 10,
        "Награбил (серебро)": 0,
        "Потерял (серебро)": 0,
        "Награбил (кристаллы)": 0,
        "Потерял (кристаллы)": 0,
    }


class PersonalStatsTests(unittest.TestCase):
    def test_new_player_is_shown_immediately_without_fake_delta(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "data"
            source.mkdir()
            db = root / "ratings.sqlite"
            snapshots = (
                ("heroes_2026-03-01_20-00-00.json.gz", {"1": hero(1, "Старый", 100)}),
                ("heroes_2026-03-02_20-00-00.json.gz", {
                    "1": hero(1, "Старый", 110),
                    "2": hero(2, "Новый", 500),
                }),
                ("heroes_2026-03-03_20-00-00.json.gz", {
                    "1": hero(1, "Старый", 120),
                    "2": hero(2, "Новый", 510),
                }),
            )
            for filename, data in snapshots:
                with gzip.open(source / filename, "wt", encoding="utf-8") as handle:
                    json.dump(data, handle, ensure_ascii=False)

            subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "tools" / "build_db.py"),
                    "--data-dir", str(source),
                    "--db-path", str(db),
                    "--rebuild",
                ],
                cwd=ROOT,
                check=True,
                stdout=subprocess.DEVNULL,
            )

            import app as app_module
            old_path = app_module.DB_PATH
            app_module.DB_PATH = str(db)
            try:
                client = app_module.app.test_client()
                second = snapshots[1][0]
                first = snapshots[0][0]
                third = snapshots[2][0]

                page = client.get("/?" + urlencode({
                    "mode": "Общий",
                    "param": "Слава",
                    "level": "Все",
                    "file": second,
                }))
                self.assertEqual(page.status_code, 200)
                self.assertIn("Новый".encode("utf-8"), page.data)

                with app_module.app.test_request_context("/"):
                    rows, total = app_module.query_rating_overall(second, first, "Слава", None, 100, 0)
                self.assertEqual(total, 2)
                new_row = next(row for row in rows if row[0] == 2)
                self.assertIsNone(new_row[4])

                profile = client.get("/profile?" + urlencode({"nickname": "Новый"}))
                self.assertEqual(profile.status_code, 200)
                self.assertNotIn(first.encode("utf-8"), profile.data)
                self.assertIn(second.encode("utf-8"), profile.data)
                self.assertIn(third.encode("utf-8"), profile.data)
                self.assertIn(b'class="personal-stats-table"', profile.data)
                self.assertIn(b'id="page-loading-overlay"', profile.data)
            finally:
                app_module.DB_PATH = old_path


if __name__ == "__main__":
    unittest.main()
