#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
from pathlib import Path
from typing import Any

EMPTY_GROUP_NAMES = {"", "не состоит", "none", "null", "нет"}


def parse_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip().replace("\xa0", "").replace(" ", "")
    if not text or text.casefold() in {"—", "-", "none", "null", "нет"}:
        return None
    try:
        return int(float(text.replace(",", ".")))
    except (TypeError, ValueError):
        return None


def clean_text(value: Any) -> str | None:
    if value is None or isinstance(value, (dict, list, tuple, set)):
        return None
    text = " ".join(str(value).split()).strip()
    if not text or text.casefold() in EMPTY_GROUP_NAMES:
        return None
    return text


def extract_group(
    hero: dict[str, Any],
    localized_key: str,
    api_key: str,
    id_keys: tuple[str, ...],
) -> tuple[str | None, int]:
    candidates = [hero.get(localized_key), hero.get(api_key)]
    nested = next((value for value in candidates if isinstance(value, dict)), None)

    name = None
    if nested is not None:
        name = clean_text(nested.get("name") or nested.get("title"))
    if name is None:
        for value in candidates:
            name = clean_text(value)
            if name is not None:
                break

    group_id = None
    for key in id_keys:
        group_id = parse_int(hero.get(key))
        if group_id is not None:
            break
    if group_id is None and nested is not None:
        group_id = parse_int(nested.get("id"))

    if name is None:
        return None, 0
    if group_id is None or group_id <= 0:
        raise ValueError(
            f"Group {name!r} is named but has no positive numeric ID; "
            "the endpoint snapshot is incomplete"
        )
    return name, group_id


def load_snapshot(path: Path) -> dict[int, dict[str, Any]]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        raw = json.load(handle)

    if isinstance(raw, dict) and isinstance(raw.get("data"), list):
        source_rows = raw["data"]
        items = []
        for row in source_rows:
            if not isinstance(row, dict):
                continue
            pid = parse_int(row.get("id") or row.get("ID"))
            if pid is not None:
                items.append((pid, row))
    elif isinstance(raw, dict):
        items = []
        for pid_raw, row in raw.items():
            if not isinstance(row, dict):
                continue
            pid = parse_int(pid_raw)
            if pid is None:
                pid = parse_int(row.get("ID") or row.get("id"))
            if pid is not None:
                items.append((pid, row))
    else:
        raise ValueError("Snapshot must be a player object or an endpoint payload")

    result: dict[int, dict[str, Any]] = {}
    for pid, row in items:
        if pid <= 0:
            continue
        if pid in result:
            raise ValueError(f"Duplicate player ID {pid} in snapshot")
        result[pid] = row
    if not result:
        raise ValueError("Snapshot contains no players")
    return result


def get_text_id(
    conn: sqlite3.Connection,
    cache: dict[str, int],
    value: str | None,
) -> int | None:
    if value is None:
        return None
    cached = cache.get(value)
    if cached is not None:
        return cached
    conn.execute(
        "INSERT OR IGNORE INTO text_values(value,norm) VALUES(?,?)",
        (value, " ".join(value.casefold().split())),
    )
    row = conn.execute("SELECT text_id FROM text_values WHERE value=?", (value,)).fetchone()
    if row is None:
        raise RuntimeError(f"Failed to store text value {value!r}")
    cache[value] = int(row[0])
    return int(row[0])


def sync_snapshot_groups(snapshot_path: Path, db_path: Path) -> dict[str, int]:
    players = load_snapshot(snapshot_path)
    expected: dict[int, tuple[str | None, int, str | None, int]] = {}
    clan_members = 0
    brotherhood_members = 0
    clan_ids: set[int] = set()
    brotherhood_ids: set[int] = set()

    for pid, hero in players.items():
        clan_name, clan_id = extract_group(
            hero,
            "Клан",
            "clan",
            ("clan_id", "Клан_id", "клан_id"),
        )
        brotherhood_name, brotherhood_id = extract_group(
            hero,
            "Братство",
            "brotherhood",
            ("brotherhood_id", "Братство_id", "братство_id"),
        )
        expected[pid] = (clan_name, clan_id, brotherhood_name, brotherhood_id)
        if clan_id > 0:
            clan_members += 1
            clan_ids.add(clan_id)
        if brotherhood_id > 0:
            brotherhood_members += 1
            brotherhood_ids.add(brotherhood_id)

    if clan_members == 0 or brotherhood_members == 0:
        raise RuntimeError(
            "Endpoint snapshot has no usable group memberships: "
            f"clans={clan_members}, brotherhoods={brotherhood_members}"
        )

    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT snapshot_id FROM snapshots WHERE filename=?",
            (snapshot_path.name,),
        ).fetchone()
        if row is None:
            raise RuntimeError(
                f"Snapshot {snapshot_path.name!r} is not present in SQLite; "
                "run tools/build_db.py first"
            )
        snapshot_id = int(row["snapshot_id"])

        existing_pids = {
            int(item[0])
            for item in conn.execute(
                "SELECT pid FROM observations WHERE snapshot_id=?", (snapshot_id,)
            )
        }
        missing = sorted(set(expected) - existing_pids)
        if missing:
            raise RuntimeError(
                f"SQLite is missing {len(missing)} snapshot players; first IDs: {missing[:10]}"
            )

        text_cache = {
            str(item[1]): int(item[0])
            for item in conn.execute("SELECT text_id,value FROM text_values")
        }
        updates = []
        for pid, (clan_name, clan_id, brotherhood_name, brotherhood_id) in expected.items():
            updates.append(
                (
                    get_text_id(conn, text_cache, clan_name),
                    clan_id,
                    get_text_id(conn, text_cache, brotherhood_name),
                    brotherhood_id,
                    snapshot_id,
                    pid,
                )
            )
        conn.executemany(
            """
            UPDATE observations
            SET clan_name_id=?, clan_game_id=?,
                brotherhood_name_id=?, brotherhood_game_id=?
            WHERE snapshot_id=? AND pid=?
            """,
            updates,
        )

        actual_rows = conn.execute(
            """
            SELECT o.pid,
                   cn.value AS clan_name,o.clan_game_id,
                   bn.value AS brotherhood_name,o.brotherhood_game_id
            FROM observations o
            LEFT JOIN text_values cn ON cn.text_id=o.clan_name_id
            LEFT JOIN text_values bn ON bn.text_id=o.brotherhood_name_id
            WHERE o.snapshot_id=?
            """,
            (snapshot_id,),
        ).fetchall()
        actual = {
            int(item["pid"]): (
                item["clan_name"], int(item["clan_game_id"] or 0),
                item["brotherhood_name"], int(item["brotherhood_game_id"] or 0),
            )
            for item in actual_rows
        }
        mismatches = [
            pid for pid, expected_value in expected.items()
            if actual.get(pid) != expected_value
        ]
        if mismatches:
            raise RuntimeError(
                f"Group synchronization verification failed for {len(mismatches)} players; "
                f"first IDs: {mismatches[:10]}"
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "players": len(expected),
        "clan_members": clan_members,
        "clans": len(clan_ids),
        "brotherhood_members": brotherhood_members,
        "brotherhoods": len(brotherhood_ids),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Synchronize clan and brotherhood fields from a raw snapshot into SQLite"
    )
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--db", required=True)
    args = parser.parse_args()

    stats = sync_snapshot_groups(Path(args.snapshot), Path(args.db))
    print(
        "Group data synchronized and verified: "
        f"players={stats['players']}, "
        f"clan_members={stats['clan_members']}, clans={stats['clans']}, "
        f"brotherhood_members={stats['brotherhood_members']}, "
        f"brotherhoods={stats['brotherhoods']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
