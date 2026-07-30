#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

PARAM_TO_COLUMN: dict[str, str | None] = {
    "Слава": "glory",
    "Побед": "wins",
    "Поражений": "losses",
    "Побед над Драконом": "dragon_wins",
    "Побед над Змеем": "snake_wins",
    "Побед над Владыкой": "lord_wins",
    "Убито зверей": "beasts_killed",
    "Сила": "strength",
    "Защита": "defense",
    "Ловкость": "dexterity",
    "Мастерство": "mastery",
    "Живучесть": "vitality",
    "Сумма статов": None,
    "Награбил (серебро)": "rob_silver",
    "Потерял (серебро)": "lost_silver",
    "Награбил (кристаллы)": "rob_crystals",
    "Потерял (кристаллы)": "lost_crystals",
}
STAT_COLUMNS = ("strength", "defense", "dexterity", "mastery", "vitality")


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def rebuild_player_registry(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM players")
    conn.execute(
        """
        INSERT INTO players(
            pid,first_snapshot_id,second_snapshot_id,visible_from_snapshot_id,
            last_snapshot_id,successful_observations
        )
        WITH ranked AS (
            SELECT
                pid,
                snapshot_id,
                ROW_NUMBER() OVER(PARTITION BY pid ORDER BY snapshot_id) AS rn
            FROM observations
        ), aggregated AS (
            SELECT
                pid,
                MIN(snapshot_id) AS first_sid,
                MIN(CASE WHEN rn=2 THEN snapshot_id END) AS second_sid,
                MAX(snapshot_id) AS last_sid,
                COUNT(*) AS observations
            FROM ranked
            GROUP BY pid
        )
        SELECT
            pid,
            first_sid,
            second_sid,
            CASE
                WHEN first_sid=(SELECT MIN(snapshot_id) FROM snapshots) THEN first_sid
                ELSE second_sid
            END,
            last_sid,
            observations
        FROM aggregated
        """
    )


def value_expr(param: str, alias: str) -> str:
    column = PARAM_TO_COLUMN[param]
    if column:
        return f"{alias}.{column}"
    return "(" + "+".join(f"{alias}.{column}" for column in STAT_COLUMNS) + ")"


def available_params(conn: sqlite3.Connection) -> list[str]:
    observation_columns = table_columns(conn, "observations")
    result: list[str] = []
    for param, column in PARAM_TO_COLUMN.items():
        required = {column} if column else set(STAT_COLUMNS)
        if required.issubset(observation_columns):
            result.append(param)
    return result


def compute_best_growth(
    conn: sqlite3.Connection,
    best_for_snapshot_id: int,
    *,
    window_days: int,
    max_gap_hours: float,
) -> None:
    latest = conn.execute(
        "SELECT ts FROM snapshots WHERE snapshot_id=?", (best_for_snapshot_id,)
    ).fetchone()
    conn.execute("DELETE FROM best_growth")
    if not latest:
        return

    min_ts = int(latest[0]) - max(1, window_days) * 86400
    snapshots = conn.execute(
        "SELECT snapshot_id,ts FROM snapshots WHERE ts BETWEEN ? AND ? ORDER BY ts,snapshot_id",
        (min_ts, int(latest[0])),
    ).fetchall()
    if len(snapshots) < 2:
        return

    params = available_params(conn)
    if not params:
        return

    dynamic_columns: list[str] = []
    for index, _param in enumerate(params):
        dynamic_columns.extend((f"d{index} INTEGER", f"s{index} INTEGER", f"l{index} INTEGER"))
    conn.execute("DROP TABLE IF EXISTS temp.best_work")
    conn.execute(
        f"CREATE TEMP TABLE best_work(pid INTEGER PRIMARY KEY,{','.join(dynamic_columns)}) WITHOUT ROWID"
    )

    select_values: list[str] = ["c.pid"]
    insert_columns: list[str] = ["pid"]
    updates: list[str] = []
    for index, param in enumerate(params):
        current = value_expr(param, "c")
        previous = value_expr(param, "p")
        diff = f"({current}-{previous})"
        select_values.extend((diff, "c.snapshot_id", "c.level"))
        insert_columns.extend((f"d{index}", f"s{index}", f"l{index}"))
        better = (
            f"excluded.d{index} IS NOT NULL AND "
            f"(best_work.d{index} IS NULL OR excluded.d{index}>best_work.d{index})"
        )
        updates.extend(
            (
                f"d{index}=CASE WHEN {better} THEN excluded.d{index} ELSE best_work.d{index} END",
                f"s{index}=CASE WHEN {better} THEN excluded.s{index} ELSE best_work.s{index} END",
                f"l{index}=CASE WHEN {better} THEN excluded.l{index} ELSE best_work.l{index} END",
            )
        )

    upsert_sql = f"""
        INSERT INTO best_work({','.join(insert_columns)})
        SELECT {','.join(select_values)}
        FROM observations c
        JOIN observations p ON p.pid=c.pid AND p.snapshot_id=?
        JOIN players registry ON registry.pid=c.pid
        WHERE c.snapshot_id=?
          AND registry.visible_from_snapshot_id IS NOT NULL
          AND c.snapshot_id>=registry.visible_from_snapshot_id
        ON CONFLICT(pid) DO UPDATE SET {','.join(updates)}
    """

    previous_sid, previous_ts = snapshots[0]
    for current_sid, current_ts in snapshots[1:]:
        gap_hours = (int(current_ts) - int(previous_ts)) / 3600.0
        if gap_hours <= max_gap_hours:
            conn.execute(upsert_sql, (int(previous_sid), int(current_sid)))
        previous_sid, previous_ts = current_sid, current_ts

    for index, param in enumerate(params):
        conn.execute(
            f"""
            INSERT INTO best_growth(
                best_for_snapshot_id,param,pid,level,diff,best_snapshot_id
            )
            SELECT ?,?,pid,l{index},d{index},s{index}
            FROM best_work
            WHERE d{index} IS NOT NULL
            """,
            (best_for_snapshot_id, param),
        )
    conn.execute("DROP TABLE temp.best_work")


def validate_database(conn: sqlite3.Connection) -> None:
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity != "ok":
        raise RuntimeError(f"SQLite integrity_check failed: {integrity}")
    foreign_key_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
    if foreign_key_errors:
        raise RuntimeError(f"SQLite foreign_key_check failed: {foreign_key_errors[:10]}")
    bad_registry = int(
        conn.execute(
            """
            SELECT COUNT(*) FROM players p
            WHERE p.second_snapshot_id IS NOT NULL
              AND p.second_snapshot_id<=p.first_snapshot_id
            """
        ).fetchone()[0]
    )
    if bad_registry:
        raise RuntimeError(f"Invalid player registry rows: {bad_registry}")


def write_github_output(path: Path, values: dict[str, str | int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for key, value in values.items():
            safe = str(value).replace("\r", " ").replace("\n", " ")
            handle.write(f"{key}={safe}\n")


def remove_latest_snapshot(
    db_path: Path,
    *,
    expected_filename: str | None,
    window_days: int,
    max_gap_hours: float,
    vacuum: bool,
) -> dict[str, str | int]:
    if not db_path.exists() or db_path.stat().st_size == 0:
        raise RuntimeError(f"Database does not exist: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")

        required_tables = {"snapshots", "observations", "players", "best_growth"}
        present_tables = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        missing = sorted(required_tables - present_tables)
        if missing:
            raise RuntimeError(f"Database is missing required tables: {missing}")

        rows = conn.execute(
            "SELECT snapshot_id,filename,ts FROM snapshots ORDER BY ts DESC,snapshot_id DESC LIMIT 2"
        ).fetchall()
        if len(rows) < 2:
            raise RuntimeError("Refusing to remove the only snapshot in the database")

        latest = rows[0]
        latest_filename = str(latest["filename"])
        if expected_filename and latest_filename != expected_filename:
            raise RuntimeError(
                f"Latest snapshot is {latest_filename!r}, expected {expected_filename!r}; nothing was deleted"
            )

        conn.execute("BEGIN IMMEDIATE")
        try:
            # players references snapshots without ON DELETE CASCADE, so rebuild it
            # from surviving observations after the latest snapshot is removed.
            conn.execute("DELETE FROM players")
            deleted = conn.execute(
                "DELETE FROM snapshots WHERE snapshot_id=?", (int(latest["snapshot_id"]),)
            ).rowcount
            if deleted != 1:
                raise RuntimeError(f"Expected to delete one snapshot, deleted={deleted}")
            rebuild_player_registry(conn)
            new_latest = conn.execute(
                "SELECT snapshot_id,filename FROM snapshots ORDER BY ts DESC,snapshot_id DESC LIMIT 1"
            ).fetchone()
            if new_latest is None:
                raise RuntimeError("Database became empty after rollback")
            compute_best_growth(
                conn,
                int(new_latest["snapshot_id"]),
                window_days=window_days,
                max_gap_hours=max_gap_hours,
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        conn.execute("ANALYZE")
        conn.execute("PRAGMA optimize")
        validate_database(conn)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA journal_mode=DELETE")
        if vacuum:
            conn.execute("VACUUM")
            validate_database(conn)

        result = {
            "removed_snapshot": latest_filename,
            "new_latest_snapshot": str(new_latest["filename"]),
            "snapshot_count": int(conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]),
            "observation_count": int(
                conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
            ),
            "player_count": int(conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]),
        }
        return result
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Remove exactly the latest snapshot and rebuild dependent database state"
    )
    parser.add_argument("--db", default="data/db/ratings.sqlite")
    parser.add_argument("--expect-filename")
    parser.add_argument("--best-window-days", type=int, default=30)
    parser.add_argument("--max-gap-hours", type=float, default=26.0)
    parser.add_argument("--vacuum", action="store_true")
    parser.add_argument("--github-output")
    args = parser.parse_args()

    result = remove_latest_snapshot(
        Path(args.db),
        expected_filename=(args.expect_filename or "").strip() or None,
        window_days=args.best_window_days,
        max_gap_hours=args.max_gap_hours,
        vacuum=args.vacuum,
    )
    print(
        "Removed latest snapshot: "
        f"removed={result['removed_snapshot']}, new_latest={result['new_latest_snapshot']}, "
        f"snapshots={result['snapshot_count']}, observations={result['observation_count']}, "
        f"players={result['player_count']}"
    )
    if args.github_output:
        write_github_output(Path(args.github_output), result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
