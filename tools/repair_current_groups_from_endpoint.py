#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp

import collect_api_first as bulk
import get_data as legacy
from forglory.parsing import parse_hero, profile_url_matches
from forglory.schema import parse_int

LOG = logging.getLogger("forglory.repair_current_groups")
EMPTY_GROUP_NAMES = {"", "не состоит", "none", "null", "нет", "-", "—"}


@dataclass(frozen=True)
class GroupValue:
    clan_name: str | None
    clan_id: int
    brotherhood_name: str | None
    brotherhood_id: int


@dataclass(frozen=True)
class ProfileResult:
    pid: int
    value: GroupValue | None
    error: str | None = None
    diagnostic_html: str | None = None


def clean_group_name(value: Any) -> str | None:
    if value is None or isinstance(value, (dict, list, tuple, set, bool)):
        return None
    text = " ".join(str(value).split()).strip()
    if not text or text.casefold() in EMPTY_GROUP_NAMES:
        return None
    return text


def group_value_from_hero(hero: dict[str, Any]) -> GroupValue:
    clan_name = clean_group_name(hero.get("Клан") or hero.get("clan_name"))
    clan_id = parse_int(hero.get("clan_id")) or 0
    brotherhood_name = clean_group_name(
        hero.get("Братство") or hero.get("brotherhood_name")
    )
    brotherhood_id = parse_int(hero.get("brotherhood_id")) or 0

    if bool(clan_name) != (clan_id > 0):
        raise ValueError(f"incomplete clan membership: name={clan_name!r}, id={clan_id}")
    if bool(brotherhood_name) != (brotherhood_id > 0):
        raise ValueError(
            "incomplete brotherhood membership: "
            f"name={brotherhood_name!r}, id={brotherhood_id}"
        )
    return GroupValue(clan_name, clan_id, brotherhood_name, brotherhood_id)


def open_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def latest_snapshot(conn: sqlite3.Connection) -> tuple[int, str, dict[int, str | None]]:
    row = conn.execute(
        "SELECT snapshot_id,filename FROM snapshots ORDER BY ts DESC LIMIT 1"
    ).fetchone()
    if row is None:
        raise RuntimeError("SQLite contains no snapshots")
    sid = int(row["snapshot_id"])
    filename = str(row["filename"])
    rows = conn.execute(
        """
        SELECT o.pid,n.value AS name
        FROM observations o
        LEFT JOIN text_values n ON n.text_id=o.name_id
        WHERE o.snapshot_id=?
        ORDER BY o.pid
        """,
        (sid,),
    ).fetchall()
    players = {int(item["pid"]): item["name"] for item in rows}
    if not players:
        raise RuntimeError(f"Latest snapshot {filename!r} contains no players")
    return sid, filename, players


def previous_snapshot_groups(
    conn: sqlite3.Connection,
    current_sid: int,
) -> tuple[int | None, dict[int, GroupValue]]:
    row = conn.execute(
        """
        SELECT snapshot_id
        FROM snapshots
        WHERE ts < (SELECT ts FROM snapshots WHERE snapshot_id=?)
        ORDER BY ts DESC
        LIMIT 1
        """,
        (current_sid,),
    ).fetchone()
    if row is None:
        return None, {}
    previous_sid = int(row["snapshot_id"])
    rows = conn.execute(
        """
        SELECT o.pid,cn.value AS clan_name,o.clan_game_id,
               bn.value AS brotherhood_name,o.brotherhood_game_id
        FROM observations o
        LEFT JOIN text_values cn ON cn.text_id=o.clan_name_id
        LEFT JOIN text_values bn ON bn.text_id=o.brotherhood_name_id
        WHERE o.snapshot_id=?
        """,
        (previous_sid,),
    ).fetchall()
    result: dict[int, GroupValue] = {}
    for item in rows:
        clan_id = int(item["clan_game_id"] or 0)
        brotherhood_id = int(item["brotherhood_game_id"] or 0)
        clan_name = clean_group_name(item["clan_name"]) if clan_id > 0 else None
        brotherhood_name = (
            clean_group_name(item["brotherhood_name"])
            if brotherhood_id > 0
            else None
        )
        if clan_id > 0 and clan_name is None:
            continue
        if brotherhood_id > 0 and brotherhood_name is None:
            continue
        result[int(item["pid"])] = GroupValue(
            clan_name,
            clan_id if clan_name else 0,
            brotherhood_name,
            brotherhood_id if brotherhood_name else 0,
        )
    return previous_sid, result


def coverage(values: dict[int, GroupValue]) -> dict[str, int]:
    clan_ids = {item.clan_id for item in values.values() if item.clan_id > 0}
    brotherhood_ids = {
        item.brotherhood_id
        for item in values.values()
        if item.brotherhood_id > 0
    }
    return {
        "players": len(values),
        "clan_members": sum(item.clan_id > 0 for item in values.values()),
        "clans": len(clan_ids),
        "brotherhood_members": sum(
            item.brotherhood_id > 0 for item in values.values()
        ),
        "brotherhoods": len(brotherhood_ids),
    }


def endpoint_groups(
    player_ids: set[int],
    cookies: dict[str, str],
    domain: str,
    attempts: int,
) -> tuple[dict[int, GroupValue], dict[str, Any]]:
    collection = bulk.fetch_from_bulk_api(
        domain,
        cookies,
        attempts=max(1, attempts),
        retry_delay_seconds=3,
        timeout_seconds=60,
        min_player_count=max(100, int(len(player_ids) * 0.9)),
    )
    missing = sorted(player_ids - set(collection.heroes))
    if missing:
        raise RuntimeError(
            f"Endpoint is missing {len(missing)} players from the current snapshot; "
            f"first IDs: {missing[:10]}"
        )
    values = {
        pid: group_value_from_hero(collection.heroes[pid])
        for pid in player_ids
    }
    return values, collection.meta


async def request_profile(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    domain: str,
    pid: int,
    fallback_name: str | None,
    retries: int,
) -> ProfileResult:
    url = f"{domain}hero/detail?player={pid}"
    last_error = "unknown error"
    diagnostic_html = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            async with semaphore:
                async with session.get(url, allow_redirects=True) as response:
                    text = await response.text(errors="replace")
                    status = int(response.status)
                    final_url = str(response.url)
            if status != 200:
                last_error = f"HTTP {status}"
            elif not profile_url_matches(final_url, pid):
                last_error = f"unexpected redirect: {final_url}"
                diagnostic_html = text
            elif not text or "Что-то пошло не так" in text:
                last_error = "profile not found"
                diagnostic_html = text
            else:
                try:
                    hero = parse_hero(text, pid, fallback_name=fallback_name)
                    return ProfileResult(pid, group_value_from_hero(hero))
                except Exception as exc:
                    last_error = f"parse error: {exc}"
                    diagnostic_html = text
        except asyncio.TimeoutError:
            last_error = "timeout"
        except aiohttp.ClientError as exc:
            last_error = f"network error: {exc}"
        except Exception as exc:  # defensive: keep one bad profile from killing the batch
            last_error = f"unexpected error: {exc!r}"
        if attempt < max(1, retries):
            await asyncio.sleep((2 ** (attempt - 1)) + random.random())
    return ProfileResult(pid, None, last_error, diagnostic_html)


async def profile_groups_async(
    players: dict[int, str | None],
    cookies: dict[str, str],
    domain: str,
    concurrency: int,
    retries: int,
    diagnostics_dir: Path,
) -> tuple[dict[int, GroupValue], list[dict[str, Any]]]:
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    for old in diagnostics_dir.glob("*.html"):
        old.unlink(missing_ok=True)

    timeout = aiohttp.ClientTimeout(total=35, connect=10, sock_read=22)
    connector = aiohttp.TCPConnector(
        limit=max(concurrency + 10, 30),
        limit_per_host=max(concurrency, 20),
        ttl_dns_cache=600,
        keepalive_timeout=30,
    )
    semaphore = asyncio.Semaphore(max(1, concurrency))
    values: dict[int, GroupValue] = {}
    failures: list[dict[str, Any]] = []
    diagnostic_count = 0

    async with aiohttp.ClientSession(
        cookies=cookies,
        headers=legacy.HEADERS,
        timeout=timeout,
        connector=connector,
    ) as session:
        tasks = [
            asyncio.create_task(
                request_profile(
                    session,
                    semaphore,
                    domain,
                    pid,
                    name,
                    retries,
                )
            )
            for pid, name in players.items()
        ]
        completed = 0
        for future in asyncio.as_completed(tasks):
            result = await future
            completed += 1
            if result.value is not None:
                values[result.pid] = result.value
            else:
                failures.append({"pid": result.pid, "error": result.error})
                if result.diagnostic_html and diagnostic_count < 5:
                    diagnostic_count += 1
                    (diagnostics_dir / f"{diagnostic_count:02d}_pid_{result.pid}.html").write_text(
                        result.diagnostic_html,
                        encoding="utf-8",
                        errors="replace",
                    )
            if completed % 500 == 0 or completed == len(tasks):
                LOG.info(
                    "Profile group progress %s/%s: success=%s, failed=%s",
                    completed,
                    len(tasks),
                    len(values),
                    len(failures),
                )
    return values, failures


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
        raise RuntimeError(f"Could not store text value {value!r}")
    cache[value] = int(row[0])
    return int(row[0])


def validate_coverage(
    current: dict[str, int],
    previous: dict[str, int],
    min_previous_ratio: float,
) -> None:
    if current["clan_members"] <= 0 or current["brotherhood_members"] <= 0:
        raise RuntimeError(f"Repaired group coverage is empty: {current}")
    for key in ("clan_members", "brotherhood_members"):
        previous_count = previous.get(key, 0)
        if previous_count <= 0:
            continue
        ratio = current[key] / previous_count
        if ratio < min_previous_ratio:
            raise RuntimeError(
                f"Repaired {key} coverage {current[key]}/{previous_count} "
                f"({ratio:.2%}) is below {min_previous_ratio:.2%}"
            )


def update_latest_groups(
    db_path: Path,
    snapshot_id: int,
    values: dict[int, GroupValue],
) -> None:
    conn = open_db(db_path)
    try:
        existing = {
            int(row[0])
            for row in conn.execute(
                "SELECT pid FROM observations WHERE snapshot_id=?", (snapshot_id,)
            )
        }
        if set(values) != existing:
            missing = sorted(existing - set(values))
            extra = sorted(set(values) - existing)
            raise RuntimeError(
                "Repair mapping does not exactly match latest snapshot: "
                f"missing={len(missing)} {missing[:10]}, extra={len(extra)} {extra[:10]}"
            )
        text_cache = {
            str(row[1]): int(row[0])
            for row in conn.execute("SELECT text_id,value FROM text_values")
        }
        updates = [
            (
                get_text_id(conn, text_cache, item.clan_name),
                item.clan_id,
                get_text_id(conn, text_cache, item.brotherhood_name),
                item.brotherhood_id,
                snapshot_id,
                pid,
            )
            for pid, item in values.items()
        ]
        conn.executemany(
            """
            UPDATE observations
            SET clan_name_id=?,clan_game_id=?,
                brotherhood_name_id=?,brotherhood_game_id=?
            WHERE snapshot_id=? AND pid=?
            """,
            updates,
        )
        conn.commit()
        if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("SQLite quick_check failed after group repair")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Repair clan and brotherhood data in the existing latest SQLite snapshot"
    )
    parser.add_argument("--db", default=os.getenv("DB_PATH", "data/db/ratings.sqlite"))
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.getenv("GROUP_REPAIR_CONCURRENCY", "40")),
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=int(os.getenv("GROUP_REPAIR_RETRIES", "3")),
    )
    parser.add_argument(
        "--min-profile-success-ratio",
        type=float,
        default=float(os.getenv("GROUP_REPAIR_MIN_PROFILE_SUCCESS_RATIO", "0.995")),
    )
    parser.add_argument(
        "--min-previous-group-ratio",
        type=float,
        default=float(os.getenv("GROUP_REPAIR_MIN_PREVIOUS_GROUP_RATIO", "0.70")),
    )
    parser.add_argument(
        "--endpoint-attempts",
        type=int,
        default=int(os.getenv("GROUP_REPAIR_ENDPOINT_ATTEMPTS", "2")),
    )
    parser.add_argument(
        "--diagnostics",
        default="data/group_repair_diagnostics.json",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    db_path = Path(args.db)
    diagnostics_path = Path(args.diagnostics)
    diagnostics_path.parent.mkdir(parents=True, exist_ok=True)
    cookies, domain = bulk.load_cookie_config()

    conn = open_db(db_path)
    try:
        current_sid, current_filename, players = latest_snapshot(conn)
        previous_sid, previous_values = previous_snapshot_groups(conn, current_sid)
    finally:
        conn.close()

    previous_stats = coverage(previous_values)
    source = "endpoint"
    endpoint_error = None
    endpoint_meta: dict[str, Any] | None = None
    profile_failures: list[dict[str, Any]] = []

    try:
        values, endpoint_meta = endpoint_groups(
            set(players), cookies, domain, args.endpoint_attempts
        )
        LOG.info("Endpoint group extraction succeeded: %s", coverage(values))
    except Exception as exc:
        endpoint_error = str(exc)
        source = "profile_pages"
        LOG.warning(
            "Endpoint cannot provide usable group data; using profile pages: %s",
            endpoint_error,
        )
        profile_values, profile_failures = asyncio.run(
            profile_groups_async(
                players,
                cookies,
                domain,
                max(1, args.concurrency),
                max(1, args.retries),
                diagnostics_path.parent / "group_repair_failed_html",
            )
        )
        success_ratio = len(profile_values) / len(players)
        if success_ratio < args.min_profile_success_ratio:
            raise RuntimeError(
                f"Profile group repair success {success_ratio:.2%} "
                f"({len(profile_values)}/{len(players)}) is below "
                f"{args.min_profile_success_ratio:.2%}"
            )
        values = dict(profile_values)
        for pid in players:
            if pid in values:
                continue
            previous = previous_values.get(pid)
            if previous is not None:
                values[pid] = previous
            else:
                values[pid] = GroupValue(None, 0, None, 0)
        LOG.info(
            "Profile group extraction completed: success=%s/%s; previous fallback=%s",
            len(profile_values),
            len(players),
            len(players) - len(profile_values),
        )

    current_stats = coverage(values)
    validate_coverage(current_stats, previous_stats, args.min_previous_group_ratio)
    update_latest_groups(db_path, current_sid, values)

    diagnostics = {
        "latest_snapshot": current_filename,
        "latest_snapshot_id": current_sid,
        "previous_snapshot_id": previous_sid,
        "source": source,
        "endpoint_error": endpoint_error,
        "endpoint_meta": endpoint_meta,
        "previous_coverage": previous_stats,
        "repaired_coverage": current_stats,
        "profile_failures_count": len(profile_failures),
        "profile_failures": profile_failures[:100],
    }
    diagnostics_path.write_text(
        json.dumps(diagnostics, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(
        "Current snapshot repaired without creating a new date: "
        f"snapshot={current_filename}, source={source}, coverage={current_stats}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
