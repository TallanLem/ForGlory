from __future__ import annotations

import hashlib
import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "tools" / "fetch_db_from_release.py"
SPEC = importlib.util.spec_from_file_location("fetch_db_from_release", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ReleaseDatabaseValidationTests(unittest.TestCase):
    def make_db(self, root: Path, filenames: list[str]) -> Path:
        path = root / "ratings.sqlite"
        conn = sqlite3.connect(path)
        try:
            conn.execute(
                """
                CREATE TABLE snapshots(
                    snapshot_id INTEGER PRIMARY KEY,
                    filename TEXT NOT NULL,
                    ts INTEGER NOT NULL
                )
                """
            )
            for index, filename in enumerate(filenames, start=1):
                conn.execute(
                    "INSERT INTO snapshots(snapshot_id,filename,ts) VALUES(?,?,?)",
                    (index, filename, index),
                )
            conn.commit()
        finally:
            conn.close()
        return path

    def test_validate_returns_latest_count_and_exact_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self.make_db(
                Path(tmp),
                [
                    "heroes_2026-07-18_20-35-48.json.gz",
                    "heroes_2026-07-26_02-14-44.json.gz",
                ],
            )
            latest, count, digest = MODULE.validate(path)
            self.assertEqual(latest, "heroes_2026-07-26_02-14-44.json.gz")
            self.assertEqual(count, 2)
            self.assertEqual(digest, hashlib.sha256(path.read_bytes()).hexdigest())

    def test_hash_changes_when_database_history_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = self.make_db(
                root,
                ["heroes_2026-07-18_20-35-48.json.gz"],
            )
            first_hash = MODULE.file_sha256(first)

            conn = sqlite3.connect(first)
            try:
                conn.execute(
                    "INSERT INTO snapshots(snapshot_id,filename,ts) VALUES(2,?,2)",
                    ("heroes_2026-07-19_20-57-57.json.gz",),
                )
                conn.commit()
            finally:
                conn.close()

            second_hash = MODULE.file_sha256(first)
            self.assertNotEqual(first_hash, second_hash)


if __name__ == "__main__":
    unittest.main()
