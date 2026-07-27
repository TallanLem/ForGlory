#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from forglory.schema import NUMERIC_FIELDS  # noqa: E402


def ensure_columns(db_path: Path) -> list[str]:
    if not db_path.exists() or db_path.stat().st_size == 0:
        return []

    connection = sqlite3.connect(db_path)
    try:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='observations'"
        ).fetchone()
        if table is None:
            return []

        existing = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(observations)")
        }
        added: list[str] = []
        for field in NUMERIC_FIELDS:
            if field.column in existing:
                continue
            connection.execute(
                f'ALTER TABLE observations ADD COLUMN "{field.column}" INTEGER'
            )
            existing.add(field.column)
            added.append(field.column)
        connection.commit()
        return added
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Add newly introduced numeric observation columns to an existing database"
    )
    parser.add_argument("--db", required=True)
    args = parser.parse_args()

    added = ensure_columns(Path(args.db))
    print(
        "Observation schema checked; added columns: "
        + (", ".join(added) if added else "none")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
