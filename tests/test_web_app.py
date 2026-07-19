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


class WebAppTests(unittest.TestCase):
    def test_player_pages_and_full_database_search(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "data"
            source.mkdir()
            db = root / "ratings.sqlite"
            first = {}
            second = {}
            for pid in range(1, 221):
                base = {
                    "ID": pid, "Имя": f"Игрок {pid:03d}", "Уровень": 1,
                    "Слава": pid, "Побед": 0, "Поражений": 0,
                    "Сила": 1, "Защита": 1, "Ловкость": 1, "Мастерство": 1, "Живучесть": 1,
                }
                first[str(pid)] = base
                second[str(pid)] = dict(base, Слава=pid + 1)
            for filename, data in (
                ("heroes_2026-02-01_20-00-00.json.gz", first),
                ("heroes_2026-02-02_20-00-00.json.gz", second),
            ):
                with gzip.open(source / filename, "wt", encoding="utf-8") as handle:
                    json.dump(data, handle, ensure_ascii=False)
            subprocess.run(
                [sys.executable, str(ROOT / "tools" / "build_db.py"), "--data-dir", str(source),
                 "--db-path", str(db), "--rebuild"],
                cwd=ROOT, check=True, stdout=subprocess.DEVNULL,
            )

            import app as app_module
            old_path = app_module.DB_PATH
            app_module.DB_PATH = str(db)
            try:
                client = app_module.app.test_client()
                page = client.get(
                    "/?" + urlencode({
                        "mode": "Общий", "param": "Слава", "level": "Все",
                        "file": "heroes_2026-02-02_20-00-00.json.gz", "page": 1,
                    })
                )
                self.assertEqual(page.status_code, 200)
                self.assertEqual(page.data.count(b'data-nickname='), 100)

                search = client.get(
                    "/api/player_search?" + urlencode({
                        "q": "Игрок 001", "mode": "Общий", "param": "Слава", "level": "Все",
                        "file": "heroes_2026-02-02_20-00-00.json.gz",
                    })
                )
                self.assertEqual(search.status_code, 200)
                result = search.get_json()["results"][0]
                self.assertEqual(result["pid"], 1)
                self.assertEqual(result["rank"], 220)
                self.assertIn("page=3", result["url"])
            finally:
                app_module.DB_PATH = old_path


if __name__ == "__main__":
    unittest.main()
