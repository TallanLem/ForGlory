from __future__ import annotations

import gzip
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def hero(pid: int, name: str, glory: int) -> dict:
    return {
        "ID": pid,
        "Имя": name,
        "Уровень": 1,
        "Слава": glory,
        "Побед": 0,
        "Поражений": 0,
        "Сила": 1,
        "Защита": 1,
        "Ловкость": 1,
        "Мастерство": 1,
        "Живучесть": 1,
    }


def write_snapshot(folder: Path, filename: str, data: dict[str, dict]) -> None:
    with gzip.open(folder / filename, "wt", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False)


class DatabaseRulesTests(unittest.TestCase):
    def test_baseline_is_visible_and_later_player_waits_for_second_observation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "data"
            source.mkdir()
            db = root / "ratings.sqlite"
            write_snapshot(source, "heroes_2026-01-01_20-00-00.json.gz", {"1": hero(1, "База", 10)})
            write_snapshot(source, "heroes_2026-01-02_20-00-00.json.gz", {"1": hero(1, "База", 20), "2": hero(2, "Новый", 5)})
            write_snapshot(source, "heroes_2026-01-03_20-00-00.json.gz", {"1": hero(1, "База", 30), "2": hero(2, "Новый", 8)})
            subprocess.run(
                [
                    sys.executable, str(ROOT / "tools" / "build_db.py"),
                    "--data-dir", str(source), "--db-path", str(db), "--rebuild",
                ],
                cwd=ROOT,
                check=True,
                stdout=subprocess.DEVNULL,
            )
            conn = sqlite3.connect(db)
            try:
                counts = dict(
                    conn.execute(
                        "SELECT snapshot_id,COUNT(*) FROM heroes WHERE visible=1 GROUP BY snapshot_id"
                    ).fetchall()
                )
                self.assertEqual(counts["heroes_2026-01-01_20-00-00.json.gz"], 1)
                self.assertEqual(counts["heroes_2026-01-02_20-00-00.json.gz"], 1)
                self.assertEqual(counts["heroes_2026-01-03_20-00-00.json.gz"], 2)
                player = conn.execute(
                    "SELECT successful_observations,first_snapshot_id,second_snapshot_id,visible_from_snapshot_id "
                    "FROM players WHERE pid=2"
                ).fetchone()
                self.assertEqual(player[0], 2)
                self.assertEqual(player[2], player[3])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
