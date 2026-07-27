from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from tools.ensure_observation_columns import ensure_columns


class ObservationSchemaUpgradeTests(unittest.TestCase):
    def test_adds_lord_wins_to_existing_observations_table(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "ratings.sqlite"
            connection = sqlite3.connect(db)
            try:
                connection.execute(
                    "CREATE TABLE observations(snapshot_id INTEGER, pid INTEGER, level INTEGER)"
                )
                connection.commit()
            finally:
                connection.close()

            added = ensure_columns(db)
            self.assertIn("lord_wins", added)

            connection = sqlite3.connect(db)
            try:
                columns = {
                    str(row[1])
                    for row in connection.execute("PRAGMA table_info(observations)")
                }
            finally:
                connection.close()
            self.assertIn("lord_wins", columns)


if __name__ == "__main__":
    unittest.main()
