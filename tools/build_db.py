#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from forglory.schema import (  # noqa: E402
    BEST_PARAMS,
    NUMERIC_FIELDS,
    PARAM_TO_COLUMN,
    STAT_COLUMNS,
    parse_int,
    pick_numeric,
    pick_text,
)

SCHEMA_VERSION = 3
DT_RE = re.compile(r"heroes_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})\.(?:json|json\.gz)$")


def parse_dt_from_name(name: str) -> datetime | None:
    match = DT_RE.search(name)
    if not match:
        return None
    try:
        # Filenames historically contain Moscow local time. Store their sortable wall-clock value.
        return datetime.strptime(
            f"{match.group(1)} {match.group(2).replace('-', ':')}",
            "%Y-%m-%d %H:%M:%S",
        ).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def list_snapshot_files(data_dir: Path) -> list[tuple[Path, int]]:
    best_by_base: dict[str, Path] = {}
    for path in [*data_dir.glob("heroes_*.json"), *data_dir.glob("heroes_*.json.gz")]:
        dt = parse_dt_from_name(path.name)
        if not dt:
            continue
        base = path.name[:-3] if path.name.endswith(".gz") else path.name
        previous = best_by_base.get(base)
        if previous is None or (path.name.endswith(".gz") and not previous.name.endswith(".gz")):
            best_by_base[base] = path
    items = [(path, int(parse_dt_from_name(path.name).timestamp())) for path in best_by_base.values()]
    return sorted(items, key=lambda item: item[1])


def load_snapshot(path: Path) -> dict[str, dict[str, Any]]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        return json.load(handle)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_text(value: str) -> str:
    return " ".join(value.casefold().split())


def configure_write_connection(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=FILE")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA cache_size=-65536")
    conn.execute("PRAGMA busy_timeout=30000")


def init_db(conn: sqlite3.Connection) -> None:
    configure_write_connection(conn)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS schema_meta(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS snapshots(
            snapshot_id INTEGER PRIMARY KEY,
            filename TEXT NOT NULL UNIQUE,
            ts INTEGER NOT NULL UNIQUE,
            player_count INTEGER NOT NULL DEFAULT 0,
            source_sha256 TEXT,
            imported_at INTEGER NOT NULL DEFAULT (unixepoch())
        );

        CREATE TABLE IF NOT EXISTS text_values(
            text_id INTEGER PRIMARY KEY,
            value TEXT NOT NULL UNIQUE,
            norm TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_text_values_norm ON text_values(norm);

        CREATE TABLE IF NOT EXISTS players(
            pid INTEGER PRIMARY KEY,
            first_snapshot_id INTEGER NOT NULL,
            second_snapshot_id INTEGER,
            visible_from_snapshot_id INTEGER,
            last_snapshot_id INTEGER NOT NULL,
            successful_observations INTEGER NOT NULL,
            FOREIGN KEY(first_snapshot_id) REFERENCES snapshots(snapshot_id),
            FOREIGN KEY(second_snapshot_id) REFERENCES snapshots(snapshot_id),
            FOREIGN KEY(visible_from_snapshot_id) REFERENCES snapshots(snapshot_id),
            FOREIGN KEY(last_snapshot_id) REFERENCES snapshots(snapshot_id)
        );

        CREATE TABLE IF NOT EXISTS scan_state(
            key TEXT PRIMARY KEY,
            value INTEGER NOT NULL
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS collection_failures(
            snapshot_id INTEGER NOT NULL,
            pid INTEGER NOT NULL,
            stage TEXT NOT NULL,
            error_type TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 1,
            http_status INTEGER,
            message TEXT,
            PRIMARY KEY(snapshot_id, pid, stage, error_type),
            FOREIGN KEY(snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE
        ) WITHOUT ROWID;
        """
    )

    numeric_columns = ",\n".join(f"{field.column} INTEGER" for field in NUMERIC_FIELDS)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS observations(
            snapshot_id INTEGER NOT NULL,
            pid INTEGER NOT NULL,
            name_id INTEGER,
            clan_name_id INTEGER,
            clan_game_id INTEGER,
            brotherhood_name_id INTEGER,
            brotherhood_game_id INTEGER,
            {numeric_columns},
            PRIMARY KEY(snapshot_id, pid),
            FOREIGN KEY(snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE,
            FOREIGN KEY(name_id) REFERENCES text_values(text_id),
            FOREIGN KEY(clan_name_id) REFERENCES text_values(text_id),
            FOREIGN KEY(brotherhood_name_id) REFERENCES text_values(text_id)
        ) WITHOUT ROWID
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_observations_snapshot_level "
        "ON observations(snapshot_id, level)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_observations_snapshot_name "
        "ON observations(snapshot_id, name_id)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS best_growth(
            best_for_snapshot_id INTEGER NOT NULL,
            param TEXT NOT NULL,
            pid INTEGER NOT NULL,
            level INTEGER,
            diff INTEGER NOT NULL,
            best_snapshot_id INTEGER NOT NULL,
            PRIMARY KEY(best_for_snapshot_id, param, pid),
            FOREIGN KEY(best_for_snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE,
            FOREIGN KEY(best_snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE
        ) WITHOUT ROWID
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_best_growth_lookup "
        "ON best_growth(best_for_snapshot_id, param, level, diff DESC, pid)"
    )
    player_columns = {row[1] for row in conn.execute("PRAGMA table_info(players)")}
    if "visible_from_snapshot_id" not in player_columns:
        conn.execute("ALTER TABLE players ADD COLUMN visible_from_snapshot_id INTEGER")
    conn.execute(
        """
        UPDATE players
        SET visible_from_snapshot_id=CASE
            WHEN first_snapshot_id=(SELECT MIN(snapshot_id) FROM snapshots) THEN first_snapshot_id
            ELSE second_snapshot_id
        END
        WHERE visible_from_snapshot_id IS NULL
        """
    )
    conn.execute(
        "INSERT OR REPLACE INTO schema_meta(key,value) VALUES('schema_version',?)",
        (str(SCHEMA_VERSION),),
    )
    recreate_views(conn)


def recreate_views(conn: sqlite3.Connection) -> None:
    conn.executescript("DROP VIEW IF EXISTS heroes; DROP VIEW IF EXISTS best30;")
    numeric_select = ",\n".join(f"o.{field.column}" for field in NUMERIC_FIELDS)
    conn.execute(
        f"""
        CREATE VIEW heroes AS
        SELECT
            s.filename AS snapshot_id,
            s.snapshot_id AS snapshot_num,
            o.pid,
            n.value AS name,
            n.norm AS name_norm,
            {numeric_select},
            cn.value AS clan,
            o.clan_game_id AS clan_id,
            bn.value AS brotherhood,
            o.brotherhood_game_id AS brotherhood_id,
            CASE
                WHEN p.visible_from_snapshot_id IS NOT NULL
                 AND o.snapshot_id >= p.visible_from_snapshot_id THEN 1
                ELSE 0
            END AS visible
        FROM observations o
        JOIN snapshots s ON s.snapshot_id=o.snapshot_id
        JOIN players p ON p.pid=o.pid
        LEFT JOIN text_values n ON n.text_id=o.name_id
        LEFT JOIN text_values cn ON cn.text_id=o.clan_name_id
        LEFT JOIN text_values bn ON bn.text_id=o.brotherhood_name_id
        """
    )
    conn.execute(
        """
        CREATE VIEW best30 AS
        SELECT
            bf.filename AS best_for_snapshot_id,
            bg.param,
            bg.pid,
            n.value AS name,
            n.norm AS name_norm,
            bg.level,
            bg.diff,
            bs.filename AS best_snapshot_id
        FROM best_growth bg
        JOIN snapshots bf ON bf.snapshot_id=bg.best_for_snapshot_id
        JOIN snapshots bs ON bs.snapshot_id=bg.best_snapshot_id
        JOIN observations o ON o.snapshot_id=bg.best_snapshot_id AND o.pid=bg.pid
        LEFT JOIN text_values n ON n.text_id=o.name_id
        """
    )


def schema_version(conn: sqlite3.Connection) -> int | None:
    try:
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key='schema_version'"
        ).fetchone()
        return int(row[0]) if row else None
    except sqlite3.Error:
        return None


def legacy_database(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False
    conn = sqlite3.connect(path)
    try:
        if schema_version(conn) == SCHEMA_VERSION:
            return False
        row = conn.execute(
            "SELECT type FROM sqlite_master WHERE name='heroes'"
        ).fetchone()
        return bool(row and row[0] == "table")
    finally:
        conn.close()


def text_id(conn: sqlite3.Connection, cache: dict[str, int], value: str | None) -> int | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    cached = cache.get(value)
    if cached is not None:
        return cached
    conn.execute(
        "INSERT OR IGNORE INTO text_values(value,norm) VALUES(?,?)",
        (value, normalize_text(value)),
    )
    row = conn.execute("SELECT text_id FROM text_values WHERE value=?", (value,)).fetchone()
    assert row is not None
    cache[value] = int(row[0])
    return int(row[0])


def load_text_cache(conn: sqlite3.Connection) -> dict[str, int]:
    return {str(row[1]): int(row[0]) for row in conn.execute("SELECT text_id,value FROM text_values")}


def metadata_for_snapshot(path: Path) -> dict[str, Any] | None:
    base = path.name
    if base.endswith(".json.gz"):
        meta_name = base[:-8] + ".meta.json"
    elif base.endswith(".json"):
        meta_name = base[:-5] + ".meta.json"
    else:
        return None
    meta_path = path.with_name(meta_name)
    if not meta_path.exists():
        return None
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def import_snapshot_dict(
    conn: sqlite3.Connection,
    filename: str,
    ts: int,
    data: dict[str, dict[str, Any]],
    source_hash: str | None,
    metadata: dict[str, Any] | None,
    text_cache: dict[str, int],
    replace: bool = False,
) -> tuple[int, list[int]]:
    existing = conn.execute(
        "SELECT snapshot_id,source_sha256 FROM snapshots WHERE filename=?", (filename,)
    ).fetchone()
    if existing and not replace and (not source_hash or existing[1] == source_hash):
        return int(existing[0]), []

    if existing:
        snapshot_id = int(existing[0])
        conn.execute("DELETE FROM observations WHERE snapshot_id=?", (snapshot_id,))
        conn.execute("DELETE FROM collection_failures WHERE snapshot_id=?", (snapshot_id,))
        conn.execute(
            "UPDATE snapshots SET ts=?,player_count=?,source_sha256=?,imported_at=unixepoch() "
            "WHERE snapshot_id=?",
            (ts, len(data), source_hash, snapshot_id),
        )
    else:
        cursor = conn.execute(
            "INSERT INTO snapshots(filename,ts,player_count,source_sha256) VALUES(?,?,?,?)",
            (filename, ts, len(data), source_hash),
        )
        snapshot_id = int(cursor.lastrowid)

    columns = [field.column for field in NUMERIC_FIELDS]
    insert_columns = [
        "snapshot_id", "pid", "name_id", "clan_name_id", "clan_game_id",
        "brotherhood_name_id", "brotherhood_game_id", *columns,
    ]
    placeholders = ",".join("?" for _ in insert_columns)
    sql = f"INSERT INTO observations({','.join(insert_columns)}) VALUES({placeholders})"

    rows: list[tuple[Any, ...]] = []
    pids: list[int] = []
    for pid_raw, hero in data.items():
        pid = parse_int(pid_raw)
        if pid is None:
            pid = parse_int(hero.get("ID"))
        if pid is None:
            continue
        name = pick_text(hero, ("Имя", "имя", "name", "nick", "Ник"))
        clan = pick_text(hero, ("Клан", "clan"))
        brotherhood = pick_text(hero, ("Братство", "brotherhood"))
        clan_game_id = parse_int(hero.get("clan_id") or hero.get("Клан_id") or hero.get("клан_id"))
        brotherhood_game_id = parse_int(
            hero.get("brotherhood_id") or hero.get("Братство_id") or hero.get("братство_id")
        )
        numeric = [pick_numeric(hero, field) for field in NUMERIC_FIELDS]
        rows.append(
            (
                snapshot_id,
                pid,
                text_id(conn, text_cache, name),
                text_id(conn, text_cache, clan),
                clan_game_id,
                text_id(conn, text_cache, brotherhood),
                brotherhood_game_id,
                *numeric,
            )
        )
        pids.append(pid)
        if len(rows) >= 2000:
            conn.executemany(sql, rows)
            rows.clear()
    if rows:
        conn.executemany(sql, rows)

    if metadata:
        failure_rows = []
        for group in (metadata.get("failures", []), metadata.get("achievement_failures", [])):
            for item in group:
                failure_rows.append(
                    (
                        snapshot_id,
                        parse_int(item.get("pid")) or 0,
                        str(item.get("stage") or "unknown"),
                        str(item.get("error_type") or "unknown"),
                        parse_int(item.get("attempts")) or 1,
                        parse_int(item.get("http_status")),
                        str(item.get("message") or "")[:300],
                    )
                )
        conn.executemany(
            "INSERT OR REPLACE INTO collection_failures("
            "snapshot_id,pid,stage,error_type,attempts,http_status,message) VALUES(?,?,?,?,?,?,?)",
            failure_rows,
        )
        highest = parse_int(metadata.get("highest_probed_id"))
        if highest is not None:
            conn.execute(
                "INSERT INTO scan_state(key,value) VALUES('highest_probed_id',?) "
                "ON CONFLICT(key) DO UPDATE SET value=MAX(value,excluded.value)",
                (highest,),
            )
    return snapshot_id, pids


def update_registry_incremental(
    conn: sqlite3.Connection,
    snapshot_id: int,
    pids: Iterable[int],
    baseline_snapshot: bool = False,
) -> None:
    visible_from = snapshot_id if baseline_snapshot else None
    conn.executemany(
        """
        INSERT INTO players(
            pid,first_snapshot_id,second_snapshot_id,visible_from_snapshot_id,
            last_snapshot_id,successful_observations
        ) VALUES(?,?,NULL,?,?,1)
        ON CONFLICT(pid) DO UPDATE SET
            second_snapshot_id=CASE
                WHEN players.second_snapshot_id IS NULL
                 AND players.successful_observations >= 1
                THEN excluded.last_snapshot_id
                ELSE players.second_snapshot_id
            END,
            visible_from_snapshot_id=CASE
                WHEN players.visible_from_snapshot_id IS NULL
                 AND players.successful_observations >= 1
                THEN excluded.last_snapshot_id
                ELSE players.visible_from_snapshot_id
            END,
            last_snapshot_id=MAX(players.last_snapshot_id,excluded.last_snapshot_id),
            successful_observations=players.successful_observations+1
        """,
        ((pid, snapshot_id, visible_from, snapshot_id) for pid in pids),
    )


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


def compute_best_growth(
    conn: sqlite3.Connection,
    best_for_snapshot_id: int,
    window_days: int = 30,
    max_gap_hours: float = 26.0,
) -> None:
    latest = conn.execute(
        "SELECT ts FROM snapshots WHERE snapshot_id=?", (best_for_snapshot_id,)
    ).fetchone()
    if not latest:
        return
    min_ts = int(latest[0]) - window_days * 86400
    snapshots = conn.execute(
        "SELECT snapshot_id,ts FROM snapshots WHERE ts BETWEEN ? AND ? ORDER BY ts",
        (min_ts, int(latest[0])),
    ).fetchall()

    conn.execute("DELETE FROM best_growth")
    if len(snapshots) < 2:
        return

    dynamic_columns = []
    for index, _param in enumerate(BEST_PARAMS):
        dynamic_columns.extend((f"d{index} INTEGER", f"s{index} INTEGER", f"l{index} INTEGER"))
    conn.execute("DROP TABLE IF EXISTS temp.best_work")
    conn.execute(
        f"CREATE TEMP TABLE best_work(pid INTEGER PRIMARY KEY,{','.join(dynamic_columns)}) WITHOUT ROWID"
    )

    select_values: list[str] = ["c.pid"]
    insert_columns: list[str] = ["pid"]
    updates: list[str] = []
    for index, param in enumerate(BEST_PARAMS):
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

    for index, param in enumerate(BEST_PARAMS):
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


def import_legacy_database(db_path: Path) -> None:
    if not legacy_database(db_path):
        return
    print(f"Upgrading legacy database: {db_path}")
    legacy_path = db_path.with_suffix(db_path.suffix + ".legacy")
    temp_path = db_path.with_suffix(db_path.suffix + ".v2.tmp")
    for path in (legacy_path, temp_path):
        if path.exists():
            path.unlink()
    os.replace(db_path, legacy_path)

    source = sqlite3.connect(legacy_path)
    source.row_factory = sqlite3.Row
    target = sqlite3.connect(temp_path)
    try:
        init_db(target)
        text_cache: dict[str, int] = {}
        columns = {row[1] for row in source.execute("PRAGMA table_info(heroes)")}
        numeric_legacy = {
            "level": "Уровень", "glory": "Слава", "wins": "Побед", "losses": "Поражений",
            "dragon_wins": "Побед над Драконом", "snake_wins": "Побед над Змеем",
            "beasts_killed": "Убито зверей", "strength": "Сила", "defense": "Защита",
            "dexterity": "Ловкость", "mastery": "Мастерство", "vitality": "Живучесть",
            "rob_silver": "Награбил (серебро)", "lost_silver": "Потерял (серебро)",
            "rob_crystals": "Награбил (кристаллы)", "lost_crystals": "Потерял (кристаллы)",
        }
        target.execute("BEGIN")
        legacy_snapshots = source.execute("SELECT id,ts FROM snapshots ORDER BY ts").fetchall()
        for legacy_index, snap in enumerate(legacy_snapshots):
            data: dict[str, dict[str, Any]] = {}
            for row in source.execute("SELECT * FROM heroes WHERE snapshot_id=?", (snap["id"],)):
                hero: dict[str, Any] = {
                    "ID": row["pid"],
                    "Имя": row["name"] if "name" in columns else None,
                    "Клан": row["clan"] if "clan" in columns else None,
                    "clan_id": row["clan_id"] if "clan_id" in columns else None,
                    "Братство": row["brotherhood"] if "brotherhood" in columns else None,
                    "brotherhood_id": row["brotherhood_id"] if "brotherhood_id" in columns else None,
                }
                for column, key in numeric_legacy.items():
                    if column in columns:
                        hero[key] = row[column]
                data[str(row["pid"])] = hero
            sid, pids = import_snapshot_dict(
                target, str(snap["id"]), int(snap["ts"]), data, None, None, text_cache
            )
            update_registry_incremental(target, sid, pids, baseline_snapshot=(legacy_index == 0))
        target.execute("COMMIT")
        latest = target.execute(
            "SELECT snapshot_id FROM snapshots ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        if latest:
            target.execute("BEGIN")
            compute_best_growth(target, int(latest[0]))
            target.execute("COMMIT")
        target.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        target.execute("PRAGMA journal_mode=DELETE")
        check = target.execute("PRAGMA integrity_check").fetchone()[0]
        if check != "ok":
            raise RuntimeError(f"Legacy database upgrade failed integrity check: {check}")
    except Exception:
        target.close()
        source.close()
        temp_path.unlink(missing_ok=True)
        os.replace(legacy_path, db_path)
        raise
    else:
        target.close()
        source.close()
        os.replace(temp_path, db_path)
        legacy_path.unlink(missing_ok=True)


def validate_database(conn: sqlite3.Connection) -> None:
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity != "ok":
        raise RuntimeError(f"SQLite integrity check failed: {integrity}")
    bad = conn.execute(
        """
        SELECT COUNT(*) FROM players p
        WHERE p.second_snapshot_id IS NOT NULL
          AND p.second_snapshot_id<=p.first_snapshot_id
        """
    ).fetchone()[0]
    if bad:
        raise RuntimeError(f"Invalid first/second observation registry rows: {bad}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incrementally import ForGlory snapshots into SQLite")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--db-path", default="data/db/ratings.sqlite")
    parser.add_argument("--best-window-days", type=int, default=30)
    parser.add_argument("--max-gap-hours", type=float, default=26.0)
    parser.add_argument("--replace", action="store_true", help="Replace snapshots whose files changed")
    parser.add_argument("--rebuild", action="store_true", help="Delete and rebuild the database")
    parser.add_argument("--vacuum", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir)
    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if args.rebuild:
        for suffix in ("", "-wal", "-shm"):
            Path(str(db_path) + suffix).unlink(missing_ok=True)
    else:
        import_legacy_database(db_path)

    files = list_snapshot_files(data_dir)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    imported: list[str] = []
    skipped = 0
    try:
        init_db(conn)
        text_cache = load_text_cache(conn)
        existing_max_ts_row = conn.execute("SELECT MAX(ts) FROM snapshots").fetchone()
        existing_max_ts = existing_max_ts_row[0] if existing_max_ts_row else None
        initial_snapshot_count = int(conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0])
        out_of_order = False

        for path, ts in files:
            current = conn.execute(
                "SELECT source_sha256 FROM snapshots WHERE filename=?", (path.name,)
            ).fetchone()
            digest = file_sha256(path)
            if current and not args.replace and current[0] == digest:
                skipped += 1
                continue
            if existing_max_ts is not None and ts <= int(existing_max_ts):
                out_of_order = True
            data = load_snapshot(path)
            metadata = metadata_for_snapshot(path)
            conn.execute("BEGIN")
            try:
                sid, pids = import_snapshot_dict(
                    conn,
                    path.name,
                    ts,
                    data,
                    digest,
                    metadata,
                    text_cache,
                    replace=args.replace,
                )
                if pids and not out_of_order:
                    update_registry_incremental(
                        conn,
                        sid,
                        pids,
                        baseline_snapshot=(initial_snapshot_count == 0 and not imported),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
            imported.append(path.name)
            print(f"Imported {path.name}: {len(data)} players")
            del data

        if out_of_order or args.rebuild:
            conn.execute("BEGIN")
            rebuild_player_registry(conn)
            conn.execute("COMMIT")

        latest = conn.execute(
            "SELECT snapshot_id FROM snapshots ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        if latest:
            conn.execute("BEGIN")
            compute_best_growth(
                conn,
                int(latest[0]),
                window_days=args.best_window_days,
                max_gap_hours=args.max_gap_hours,
            )
            conn.execute("COMMIT")

        recreate_views(conn)
        conn.execute("ANALYZE")
        conn.execute("PRAGMA optimize")
        validate_database(conn)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA journal_mode=DELETE")
        if args.vacuum:
            conn.execute("VACUUM")
        snapshots = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        observations = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
        players = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
        print(
            f"OK: {db_path}; imported={len(imported)}, skipped={skipped}, "
            f"snapshots={snapshots}, observations={observations}, players={players}"
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
