from __future__ import annotations

import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "remove_latest_snapshot.py"
spec = importlib.util.spec_from_file_location("remove_latest_snapshot", MODULE_PATH)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)


class RemoveLatestSnapshotTests(unittest.TestCase):
    def make_db(self, path: Path) -> None:
        numeric_columns = {
            column
            for column in module.PARAM_TO_COLUMN.values()
            if column is not None
        } | set(module.STAT_COLUMNS) | {"level"}
        numeric_sql = ",".join(f"{column} INTEGER" for column in sorted(numeric_columns))
        conn = sqlite3.connect(path)
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript(
                f"""
                CREATE TABLE snapshots(
                    snapshot_id INTEGER PRIMARY KEY,
                    filename TEXT UNIQUE NOT NULL,
                    ts INTEGER UNIQUE NOT NULL
                );
                CREATE TABLE observations(
                    snapshot_id INTEGER NOT NULL,
                    pid INTEGER NOT NULL,
                    {numeric_sql},
                    PRIMARY KEY(snapshot_id,pid),
                    FOREIGN KEY(snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE
                );
                CREATE TABLE players(
                    pid INTEGER PRIMARY KEY,
                    first_snapshot_id INTEGER NOT NULL,
                    second_snapshot_id INTEGER,
                    visible_from_snapshot_id INTEGER,
                    last_snapshot_id INTEGER NOT NULL,
                    successful_observations INTEGER NOT NULL,
                    FOREIGN KEY(first_snapshot_id) REFERENCES snapshots(snapshot_id),
                    FOREIGN KEY(second_snapshot_id) REFERENCES snapshots(snapshot_id),
                    FOREIGN KEY(visible_from_snapshot_id) REFERENCES snapshots(snapshot_id),
                    FOREIGN KEY(last_snapshot_id) REFERENCES snapshots(snapshot_id)
                );
                CREATE TABLE best_growth(
                    best_for_snapshot_id INTEGER NOT NULL,
                    param TEXT NOT NULL,
                    pid INTEGER NOT NULL,
                    level INTEGER,
                    diff INTEGER NOT NULL,
                    best_snapshot_id INTEGER NOT NULL,
                    PRIMARY KEY(best_for_snapshot_id,param,pid),
                    FOREIGN KEY(best_for_snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE,
                    FOREIGN KEY(best_snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE
                );
                CREATE TABLE collection_failures(
                    snapshot_id INTEGER NOT NULL,
                    pid INTEGER NOT NULL,
                    PRIMARY KEY(snapshot_id,pid),
                    FOREIGN KEY(snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE
                );
                """
            )
            conn.executemany(
                "INSERT INTO snapshots(snapshot_id,filename,ts) VALUES(?,?,?)",
                [
                    (1, "heroes_2026-07-28_20-24-00.json.gz", 1000),
                    (2, "heroes_2026-07-29_20-24-00.json.gz", 2000),
                    (3, "heroes_2026-07-30_21-32-00.json.gz", 3000),
                ],
            )
            columns = sorted(numeric_columns)
            placeholders = ",".join("?" for _ in range(2 + len(columns)))
            sql = (
                f"INSERT INTO observations(snapshot_id,pid,{','.join(columns)}) "
                f"VALUES({placeholders})"
            )
            rows = []
            for sid, pid, base in [(1, 10, 10), (2, 10, 20), (3, 10, 30), (3, 99, 5)]:
                values = {column: base for column in columns}
                values["level"] = 10
                rows.append((sid, pid, *(values[column] for column in columns)))
            conn.executemany(sql, rows)
            module.rebuild_player_registry(conn)
            conn.execute(
                "INSERT INTO best_growth VALUES(3,'Слава',10,10,10,3)"
            )
            conn.execute("INSERT INTO collection_failures VALUES(3,10)")
            conn.commit()
        finally:
            conn.close()

    def test_removes_latest_and_rebuilds_dependent_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "ratings.sqlite"
            self.make_db(db)
            result = module.remove_latest_snapshot(
                db,
                expected_filename="heroes_2026-07-30_21-32-00.json.gz",
                window_days=30,
                max_gap_hours=26,
                vacuum=False,
            )
            self.assertEqual(result["new_latest_snapshot"], "heroes_2026-07-29_20-24-00.json.gz")
            self.assertEqual(result["snapshot_count"], 2)

            conn = sqlite3.connect(db)
            try:
                self.assertIsNone(
                    conn.execute("SELECT 1 FROM snapshots WHERE snapshot_id=3").fetchone()
                )
                self.assertEqual(
                    conn.execute(
                        "SELECT first_snapshot_id,second_snapshot_id,last_snapshot_id,successful_observations "
                        "FROM players WHERE pid=10"
                    ).fetchone(),
                    (1, 2, 2, 2),
                )
                self.assertIsNone(conn.execute("SELECT 1 FROM players WHERE pid=99").fetchone())
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM collection_failures").fetchone()[0], 0
                )
                self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])
            finally:
                conn.close()

    def test_expected_filename_prevents_wrong_deletion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "ratings.sqlite"
            self.make_db(db)
            with self.assertRaises(RuntimeError):
                module.remove_latest_snapshot(
                    db,
                    expected_filename="wrong.json.gz",
                    window_days=30,
                    max_gap_hours=26,
                    vacuum=False,
                )
            conn = sqlite3.connect(db)
            try:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0], 3)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
