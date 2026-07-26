from __future__ import annotations

import gzip
import importlib.util
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "tools" / "sync_snapshot_groups.py"
spec = importlib.util.spec_from_file_location("sync_snapshot_groups", MODULE_PATH)
module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(module)


class SnapshotGroupSyncTests(unittest.TestCase):
    def make_db(self, path: Path, snapshot_name: str) -> None:
        conn = sqlite3.connect(path)
        try:
            conn.executescript(
                """
                CREATE TABLE snapshots(snapshot_id INTEGER PRIMARY KEY, filename TEXT UNIQUE);
                CREATE TABLE text_values(
                    text_id INTEGER PRIMARY KEY,
                    value TEXT UNIQUE NOT NULL,
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
                """
            )
            conn.execute("INSERT INTO snapshots(snapshot_id,filename) VALUES(1,?)", (snapshot_name,))
            conn.executemany(
                "INSERT INTO observations(snapshot_id,pid) VALUES(1,?)",
                [(1,), (2,), (3,)],
            )
            conn.commit()
        finally:
            conn.close()

    def test_repairs_normalized_endpoint_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = root / "heroes_2026-07-26_22-00-00.json.gz"
            payload = {
                "1": {"ID": 1, "Клан": "Alpha", "clan_id": 10, "Братство": "North", "brotherhood_id": 20},
                "2": {"ID": 2, "Клан": "Alpha", "clan_id": 10, "Братство": "South", "brotherhood_id": 21},
                "3": {"ID": 3, "Клан": "не состоит", "clan_id": 0, "Братство": "North", "brotherhood_id": 20},
            }
            with gzip.open(snapshot, "wt", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False)
            db = root / "ratings.sqlite"
            self.make_db(db, snapshot.name)

            stats = module.sync_snapshot_groups(snapshot, db)
            self.assertEqual(stats["clan_members"], 2)
            self.assertEqual(stats["clans"], 1)
            self.assertEqual(stats["brotherhood_members"], 3)
            self.assertEqual(stats["brotherhoods"], 2)

            conn = sqlite3.connect(db)
            try:
                rows = conn.execute(
                    """
                    SELECT o.pid,cn.value,o.clan_game_id,bn.value,o.brotherhood_game_id
                    FROM observations o
                    LEFT JOIN text_values cn ON cn.text_id=o.clan_name_id
                    LEFT JOIN text_values bn ON bn.text_id=o.brotherhood_name_id
                    ORDER BY o.pid
                    """
                ).fetchall()
            finally:
                conn.close()
            self.assertEqual(
                rows,
                [(1, "Alpha", 10, "North", 20), (2, "Alpha", 10, "South", 21), (3, None, 0, "North", 20)],
            )

    def test_accepts_raw_nested_endpoint_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = root / "heroes_2026-07-26_22-01-00.json.gz"
            payload = {
                "success": True,
                "data": [
                    {"id": 1, "clan": {"id": 10, "name": "Alpha"}, "brotherhood": {"id": 20, "name": "North"}},
                    {"id": 2, "clan": {"id": 11, "name": "Beta"}, "brotherhood": {"id": 20, "name": "North"}},
                    {"id": 3, "clan": None, "brotherhood": {"id": 21, "name": "South"}},
                ],
            }
            with gzip.open(snapshot, "wt", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False)
            db = root / "ratings.sqlite"
            self.make_db(db, snapshot.name)
            stats = module.sync_snapshot_groups(snapshot, db)
            self.assertEqual(stats["clans"], 2)
            self.assertEqual(stats["brotherhoods"], 2)

    def test_rejects_snapshot_without_group_memberships(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = root / "heroes_2026-07-26_22-02-00.json.gz"
            with gzip.open(snapshot, "wt", encoding="utf-8") as handle:
                json.dump({"1": {"ID": 1}, "2": {"ID": 2}, "3": {"ID": 3}}, handle)
            db = root / "ratings.sqlite"
            self.make_db(db, snapshot.name)
            with self.assertRaises(RuntimeError):
                module.sync_snapshot_groups(snapshot, db)


if __name__ == "__main__":
    unittest.main()
