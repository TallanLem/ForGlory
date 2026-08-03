#!/usr/bin/env python3
"""Rebuild best-growth rows with wrap-safe 32-bit counter deltas."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from forglory.counter_math import (  # noqa: E402
    INT32_MIN,
    WRAP_COUNTER_COLUMN_SET,
    sql_cumulative_delta32,
)
from forglory.schema import BEST_PARAMS, PARAM_TO_COLUMN, STAT_COLUMNS  # noqa: E402


def value_expr(param: str, alias: str) -> str:
    column = PARAM_TO_COLUMN[param]
    if column:
        return f"{alias}.{column}"
    return "(" + "+".join(f"{alias}.{column}" for column in STAT_COLUMNS) + ")"


def growth_expr(param: str, current_alias: str, previous_alias: str) -> str:
    current = value_expr(param, current_alias)
    previous = value_expr(param, previous_alias)
    column = PARAM_TO_COLUMN[param]
    if column in WRAP_COUNTER_COLUMN_SET:
        return sql_cumulative_delta32(current, previous)
    return f"(({current})-({previous}))"


def validate_schema(conn: sqlite3.Connection) -> None:
    required = {"snapshots", "observations", "players", "best_growth"}
    actual = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    missing = sorted(required - actual)
    if missing:
        raise RuntimeError("Database is missing required tables: " + ", ".join(missing))


def count_wrap_candidates(
    conn: sqlite3.Connection,
    previous_sid: int,
    current_sid: int,
) -> int:
    conditions = []
    for param in BEST_PARAMS:
        column = PARAM_TO_COLUMN[param]
        if column not in WRAP_COUNTER_COLUMN_SET:
            continue
        current = value_expr(param, "c")
        previous = value_expr(param, "p")
        raw = f"(({current})-({previous}))"
        conditions.append(
            f"CASE WHEN {current} IS NOT NULL AND {previous} IS NOT NULL "
            f"AND {raw}<{INT32_MIN} THEN 1 ELSE 0 END"
        )
    if not conditions:
        return 0
    row = conn.execute(
        f"""
        SELECT COALESCE(SUM({'+'.join(conditions)}),0)
        FROM observations c
        JOIN observations p ON p.pid=c.pid AND p.snapshot_id=?
        WHERE c.snapshot_id=?
        """,
        (previous_sid, current_sid),
    ).fetchone()
    return int(row[0] or 0)


def rebuild_best_growth(
    conn: sqlite3.Connection,
    *,
    window_days: int = 30,
    max_gap_hours: float = 26.0,
) -> tuple[int | None, int, int]:
    latest = conn.execute(
        "SELECT snapshot_id,ts FROM snapshots ORDER BY ts DESC LIMIT 1"
    ).fetchone()
    if latest is None:
        conn.execute("DELETE FROM best_growth")
        return None, 0, 0

    latest_sid = int(latest[0])
    latest_ts = int(latest[1])
    min_ts = latest_ts - max(1, int(window_days)) * 86400
    snapshots = conn.execute(
        "SELECT snapshot_id,ts FROM snapshots WHERE ts BETWEEN ? AND ? ORDER BY ts",
        (min_ts, latest_ts),
    ).fetchall()

    conn.execute("DELETE FROM best_growth")
    if len(snapshots) < 2:
        return latest_sid, 0, 0

    dynamic_columns: list[str] = []
    for index, _param in enumerate(BEST_PARAMS):
        dynamic_columns.extend(
            (f"d{index} INTEGER", f"s{index} INTEGER", f"l{index} INTEGER")
        )
    conn.execute("DROP TABLE IF EXISTS temp.best_work_safe")
    conn.execute(
        "CREATE TEMP TABLE best_work_safe("
        f"pid INTEGER PRIMARY KEY,{','.join(dynamic_columns)}"
        ") WITHOUT ROWID"
    )

    select_values: list[str] = ["c.pid"]
    insert_columns: list[str] = ["pid"]
    updates: list[str] = []
    for index, param in enumerate(BEST_PARAMS):
        diff = growth_expr(param, "c", "p")
        select_values.extend((diff, "c.snapshot_id", "c.level"))
        insert_columns.extend((f"d{index}", f"s{index}", f"l{index}"))
        better = (
            f"excluded.d{index} IS NOT NULL AND "
            f"(best_work_safe.d{index} IS NULL "
            f"OR excluded.d{index}>best_work_safe.d{index})"
        )
        updates.extend(
            (
                f"d{index}=CASE WHEN {better} THEN excluded.d{index} "
                f"ELSE best_work_safe.d{index} END",
                f"s{index}=CASE WHEN {better} THEN excluded.s{index} "
                f"ELSE best_work_safe.s{index} END",
                f"l{index}=CASE WHEN {better} THEN excluded.l{index} "
                f"ELSE best_work_safe.l{index} END",
            )
        )

    upsert_sql = f"""
        INSERT INTO best_work_safe({','.join(insert_columns)})
        SELECT {','.join(select_values)}
        FROM observations c
        JOIN observations p ON p.pid=c.pid AND p.snapshot_id=?
        JOIN players registry ON registry.pid=c.pid
        WHERE c.snapshot_id=?
          AND registry.visible_from_snapshot_id IS NOT NULL
          AND c.snapshot_id>=registry.visible_from_snapshot_id
        ON CONFLICT(pid) DO UPDATE SET {','.join(updates)}
    """

    wrap_candidates = 0
    processed_pairs = 0
    previous_sid, previous_ts = map(int, snapshots[0])
    for current_row in snapshots[1:]:
        current_sid, current_ts = map(int, current_row)
        gap_hours = (current_ts - previous_ts) / 3600.0
        if gap_hours <= float(max_gap_hours):
            wrap_candidates += count_wrap_candidates(conn, previous_sid, current_sid)
            conn.execute(upsert_sql, (previous_sid, current_sid))
            processed_pairs += 1
        previous_sid, previous_ts = current_sid, current_ts

    for index, param in enumerate(BEST_PARAMS):
        conn.execute(
            f"""
            INSERT INTO best_growth(
                best_for_snapshot_id,param,pid,level,diff,best_snapshot_id
            )
            SELECT ?,?,pid,l{index},d{index},s{index}
            FROM best_work_safe
            WHERE d{index} IS NOT NULL
            """,
            (latest_sid, param),
        )
    conn.execute("DROP TABLE temp.best_work_safe")
    return latest_sid, processed_pairs, wrap_candidates


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rebuild best-growth rankings with 32-bit wrap-safe deltas"
    )
    parser.add_argument("--db", default="data/db/ratings.sqlite")
    parser.add_argument("--window-days", type=int, default=30)
    parser.add_argument("--max-gap-hours", type=float, default=26.0)
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(db_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        validate_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        latest_sid, processed_pairs, wrap_candidates = rebuild_best_growth(
            conn,
            window_days=args.window_days,
            max_gap_hours=args.max_gap_hours,
        )
        conn.commit()

        quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
        if quick_check != "ok":
            raise RuntimeError(f"Database quick_check failed: {quick_check}")
        foreign_key_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_key_errors:
            raise RuntimeError(
                f"Database foreign_key_check returned {len(foreign_key_errors)} errors"
            )
        row_count = int(conn.execute("SELECT COUNT(*) FROM best_growth").fetchone()[0])
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(
        "Wrap-safe best growth rebuilt: "
        f"latest_snapshot_id={latest_sid}, pairs={processed_pairs}, "
        f"corrected_candidates={wrap_candidates}, rows={row_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
