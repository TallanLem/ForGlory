#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

MOSCOW = ZoneInfo("Europe/Moscow")


def _wall_clock_timestamp(value: datetime) -> int:
    """Store a local wall clock in the same sortable form used by build_db.py."""
    if value.tzinfo is not None:
        value = value.astimezone(MOSCOW).replace(tzinfo=None)
    return int(value.replace(tzinfo=timezone.utc).timestamp())


def _parse_now(value: str | None) -> datetime:
    if not value:
        return datetime.now(MOSCOW)
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=MOSCOW)
    return parsed.astimezone(MOSCOW)


def evaluate(
    db_path: Path,
    *,
    window_hours: float,
    now: datetime,
    force: bool = False,
) -> dict[str, str]:
    if force:
        return {
            "should_run": "true",
            "reason": "force",
            "latest_snapshot": "",
            "delta_seconds": "",
        }

    if not db_path.exists() or db_path.stat().st_size == 0:
        return {
            "should_run": "true",
            "reason": "database_missing",
            "latest_snapshot": "",
            "delta_seconds": "",
        }

    conn = sqlite3.connect(f"file:{db_path.resolve().as_posix()}?mode=ro", uri=True)
    try:
        check = conn.execute("PRAGMA quick_check").fetchone()[0]
        if check != "ok":
            raise RuntimeError(f"Database quick_check failed: {check}")
        row = conn.execute(
            "SELECT filename,ts FROM snapshots ORDER BY ts DESC,snapshot_id DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return {
            "should_run": "true",
            "reason": "database_has_no_snapshots",
            "latest_snapshot": "",
            "delta_seconds": "",
        }

    latest_filename = str(row[0])
    latest_ts = int(row[1])
    current_ts = _wall_clock_timestamp(now)
    delta_seconds = abs(current_ts - latest_ts)
    window_seconds = int(max(0.0, window_hours) * 3600)
    should_run = delta_seconds > window_seconds
    return {
        "should_run": "true" if should_run else "false",
        "reason": "outside_window" if should_run else "snapshot_within_window",
        "latest_snapshot": latest_filename,
        "delta_seconds": str(delta_seconds),
    }


def write_github_output(path: Path, values: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for key, value in values.items():
            safe = str(value).replace("\r", " ").replace("\n", " ")
            handle.write(f"{key}={safe}\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Skip a collection when the release database already has a nearby snapshot"
    )
    parser.add_argument("--db", default="data/db/ratings.sqlite")
    parser.add_argument("--window-hours", type=float, default=5.0)
    parser.add_argument("--now", help="ISO datetime used only for deterministic testing")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--github-output")
    args = parser.parse_args()

    values = evaluate(
        Path(args.db),
        window_hours=args.window_hours,
        now=_parse_now(args.now),
        force=args.force,
    )
    delta = values["delta_seconds"] or "n/a"
    print(
        "Snapshot-window guard: "
        f"should_run={values['should_run']}, reason={values['reason']}, "
        f"latest={values['latest_snapshot'] or 'none'}, delta_seconds={delta}"
    )
    if args.github_output:
        write_github_output(Path(args.github_output), values)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
