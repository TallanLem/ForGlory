#!/usr/bin/env python3
"""Normalize mixed wrapped/unwrapped 32-bit counters in SQLite history."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from forglory.counter_math import (  # noqa: E402
    WRAP_COUNTER_COLUMN_SET,
    unwrap_cumulative_counter,
)
from forglory.schema import NUMERIC_FIELDS  # noqa: E402


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def normalize_counter_history(
    conn: sqlite3.Connection,
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    observation_columns = table_columns(conn, "observations")
    columns = [
        field.column
        for field in NUMERIC_FIELDS
        if field.column in observation_columns
        and field.column in WRAP_COUNTER_COLUMN_SET
    ]
    if not columns:
        raise RuntimeError("No numeric observation columns were found")

    select_columns = ",".join(f"o.{column}" for column in columns)
    cursor = conn.execute(
        f"""
        SELECT o.snapshot_id,o.pid,{select_columns}
        FROM observations o
        JOIN snapshots s ON s.snapshot_id=o.snapshot_id
        ORDER BY o.pid,s.ts,o.snapshot_id
        """
    )

    previous_by_column: dict[str, int | None] = {column: None for column in columns}
    updates: dict[str, list[tuple[int, int, int]]] = defaultdict(list)
    current_pid: int | None = None

    for row in cursor:
        snapshot_id = int(row[0])
        pid = int(row[1])
        if pid != current_pid:
            current_pid = pid
            previous_by_column = {column: None for column in columns}

        for index, column in enumerate(columns, start=2):
            raw = row[index]
            if raw is None:
                continue
            raw_value = int(raw)
            normalized = unwrap_cumulative_counter(
                raw_value, previous_by_column[column]
            )
            previous_by_column[column] = normalized
            if normalized != raw_value:
                updates[column].append((normalized, snapshot_id, pid))

    if not dry_run:
        for column, rows in updates.items():
            conn.executemany(
                f"UPDATE observations SET {column}=? "
                "WHERE snapshot_id=? AND pid=?",
                rows,
            )

    return {column: len(rows) for column, rows in updates.items()}


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize 32-bit counter representation jumps across snapshot history"
        )
    )
    parser.add_argument("--db", default="data/db/ratings.sqlite")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(db_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        required = {"snapshots", "observations"}
        actual = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        missing = sorted(required - actual)
        if missing:
            raise RuntimeError(
                "Database is missing required tables: " + ", ".join(missing)
            )

        conn.execute("BEGIN IMMEDIATE")
        changes = normalize_counter_history(conn, dry_run=args.dry_run)
        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()

        quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
        if quick_check != "ok":
            raise RuntimeError(f"Database quick_check failed: {quick_check}")
        foreign_key_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_key_errors:
            raise RuntimeError(
                f"Database foreign_key_check returned {len(foreign_key_errors)} errors"
            )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    total = sum(changes.values())
    mode = "would change" if args.dry_run else "changed"
    details = ", ".join(
        f"{column}={count}" for column, count in sorted(changes.items())
    ) or "none"
    print(f"Counter normalization {mode} {total} cells; {details}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
