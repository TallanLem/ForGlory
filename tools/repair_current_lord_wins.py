#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import collect_api_first as collector  # noqa: E402
from forglory.schema import parse_int  # noqa: E402


def ensure_lord_wins_column(conn: sqlite3.Connection) -> bool:
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(observations)")}
    if "lord_wins" in columns:
        return False
    conn.execute("ALTER TABLE observations ADD COLUMN lord_wins INTEGER")
    return True


def repair_latest_snapshot(
    db_path: Path,
    *,
    min_match_ratio: float = 0.995,
    api_attempts: int = 4,
    api_retry_delay: float = 5.0,
    api_timeout: float = 45.0,
    api_min_players: int = 100,
) -> dict[str, Any]:
    if not db_path.exists() or db_path.stat().st_size == 0:
        raise RuntimeError(f"Database does not exist or is empty: {db_path}")

    cookies, domain = collector.load_cookie_config()
    collection = collector.fetch_from_bulk_api(
        domain,
        cookies,
        attempts=api_attempts,
        retry_delay_seconds=api_retry_delay,
        timeout_seconds=api_timeout,
        min_player_count=api_min_players,
    )

    endpoint_values: dict[int, int] = {}
    missing_endpoint_values: list[int] = []
    for pid, hero in collection.heroes.items():
        value = parse_int(hero.get("Побед над Владыкой"))
        if value is None:
            missing_endpoint_values.append(pid)
        else:
            endpoint_values[int(pid)] = int(value)
    if missing_endpoint_values:
        raise RuntimeError(
            "Bulk endpoint returned players without lord_wins: "
            f"count={len(missing_endpoint_values)}, first={missing_endpoint_values[:10]}"
        )
    if not endpoint_values:
        raise RuntimeError("Bulk endpoint returned no lord_wins values")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        column_added = ensure_lord_wins_column(conn)

        latest = conn.execute(
            "SELECT snapshot_id,filename,player_count FROM snapshots "
            "ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        if latest is None:
            raise RuntimeError("Database contains no snapshots")
        snapshot_id = int(latest["snapshot_id"])
        filename = str(latest["filename"])

        rows = conn.execute(
            "SELECT pid FROM observations WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchall()
        snapshot_pids = {int(row["pid"]) for row in rows}
        if not snapshot_pids:
            raise RuntimeError(f"Latest snapshot {filename!r} contains no observations")

        matched = sorted(snapshot_pids.intersection(endpoint_values))
        missing_from_endpoint = sorted(snapshot_pids - set(endpoint_values))
        match_ratio = len(matched) / len(snapshot_pids)
        if match_ratio < min_match_ratio:
            raise RuntimeError(
                "Endpoint/latest-snapshot player match is too low: "
                f"{match_ratio:.2%} < {min_match_ratio:.2%}; "
                f"matched={len(matched)}, snapshot={len(snapshot_pids)}, "
                f"first_missing={missing_from_endpoint[:10]}"
            )

        conn.executemany(
            "UPDATE observations SET lord_wins=? WHERE snapshot_id=? AND pid=?",
            ((endpoint_values[pid], snapshot_id, pid) for pid in matched),
        )

        verified = conn.execute(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN lord_wins IS NOT NULL THEN 1 ELSE 0 END) AS populated, "
            "SUM(CASE WHEN lord_wins>0 THEN 1 ELSE 0 END) AS positive, "
            "MIN(lord_wins) AS minimum, MAX(lord_wins) AS maximum "
            "FROM observations WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone()
        populated = int(verified["populated"] or 0)
        positive = int(verified["positive"] or 0)
        if populated < len(matched):
            raise RuntimeError(
                f"Lord-wins verification failed: populated={populated}, matched={len(matched)}"
            )
        if positive <= 0:
            raise RuntimeError("Lord-wins verification failed: all repaired values are zero")

        check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
        if check != "ok":
            raise RuntimeError(f"SQLite quick_check failed: {check}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "snapshot_id": snapshot_id,
        "snapshot": filename,
        "snapshot_players": len(snapshot_pids),
        "endpoint_players": len(endpoint_values),
        "matched_players": len(matched),
        "match_ratio": match_ratio,
        "missing_from_endpoint": len(missing_from_endpoint),
        "column_added": column_added,
        "populated": populated,
        "positive": positive,
        "minimum": verified["minimum"],
        "maximum": verified["maximum"],
        "new_snapshot_created": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Repair lord_wins only in the current latest SQLite snapshot"
    )
    parser.add_argument("--db", default="data/db/ratings.sqlite")
    parser.add_argument("--min-match-ratio", type=float, default=0.995)
    parser.add_argument("--diagnostics", default="data/lord_wins_repair_diagnostics.json")
    args = parser.parse_args()

    diagnostics_path = Path(args.diagnostics)
    diagnostics_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        result = repair_latest_snapshot(
            Path(args.db),
            min_match_ratio=max(0.0, min(1.0, args.min_match_ratio)),
            api_attempts=int(collector.legacy.env_get("HEROES_API_ATTEMPTS", "4")),
            api_retry_delay=float(
                collector.legacy.env_get("HEROES_API_RETRY_DELAY_SECONDS", "5")
            ),
            api_timeout=float(
                collector.legacy.env_get("HEROES_API_TIMEOUT_SECONDS", "45")
            ),
            api_min_players=int(
                collector.legacy.env_get("HEROES_API_MIN_PLAYER_COUNT", "100")
            ),
        )
    except Exception as exc:
        diagnostics_path.write_text(
            json.dumps({"success": False, "error": str(exc)}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        raise

    diagnostics_path.write_text(
        json.dumps({"success": True, **result}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(
        "Current snapshot lord_wins repaired without creating a new date: "
        f"snapshot={result['snapshot']}, matched={result['matched_players']}/"
        f"{result['snapshot_players']}, positive={result['positive']}, "
        f"range={result['minimum']}..{result['maximum']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
