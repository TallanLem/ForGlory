#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import gzip
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

DT_RE = re.compile(r"heroes_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})\.(?:json|json\.gz)$")

# Параметры, для которых считаем best30 (совпадает с тем, что реально есть у тебя на сайте)
BEST_PARAMS = [
    "Слава", "Побед", "Поражений", "Побед над Драконом", "Побед над Змеем", "Убито зверей",
    "Сила", "Защита", "Ловкость", "Мастерство", "Живучесть", "Сумма статов",
    "Награбил (серебро)", "Потерял (серебро)",
    "Награбил (кристаллы)", "Потерял (кристаллы)",
]

STAT_KEYS = ("Сила", "Защита", "Ловкость", "Мастерство", "Живучесть")


def parse_dt_from_name(name: str) -> Optional[datetime]:
    m = DT_RE.search(name)
    if not m:
        return None
    ds, ts = m.group(1), m.group(2).replace("-", ":")
    try:
        dt = datetime.strptime(f"{ds} {ts}", "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def list_snapshots(data_dir: Path) -> List[Tuple[str, int]]:
    """
    Возвращает список (filename, utc_ts) от новых к старым.
    Дедупает .json и .json.gz для одного и того же base, предпочитает .json.gz.
    """
    candidates = list(data_dir.glob("heroes_*.json")) + list(data_dir.glob("heroes_*.json.gz"))
    best_by_base: Dict[str, Path] = {}

    for p in candidates:
        dt = parse_dt_from_name(p.name)
        if not dt:
            continue
        base = p.name[:-3] if p.name.endswith(".gz") else p.name  # heroes_....json
        prev = best_by_base.get(base)
        if prev is None:
            best_by_base[base] = p
        else:
            if (not prev.name.endswith(".gz")) and p.name.endswith(".gz"):
                best_by_base[base] = p

    out: List[Tuple[str, int]] = []
    for _base, p in best_by_base.items():
        dt = parse_dt_from_name(p.name)
        if not dt:
            continue
        out.append((p.name, int(dt.timestamp())))

    out.sort(key=lambda x: x[1], reverse=True)
    return out


def load_snapshot(path: Path) -> Dict[str, dict]:
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA foreign_keys=ON;")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS snapshots(
        id TEXT PRIMARY KEY,
        ts INTEGER NOT NULL
    );
    """)

    # ВАЖНО: схема heroes соответствует тому, что ожидает app.py (wins/losses/dexterity и т.д.)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS heroes(
        snapshot_id TEXT NOT NULL,
        pid INTEGER NOT NULL,
        name TEXT,
        level INTEGER,
        glory INTEGER,
        wins INTEGER,
        losses INTEGER,
        dragon_wins INTEGER,
        snake_wins INTEGER,
        beasts_killed INTEGER,
        strength INTEGER,
        defense INTEGER,
        dexterity INTEGER,
        mastery INTEGER,
        vitality INTEGER,
        rob_silver INTEGER,
        lost_silver INTEGER,
        rob_crystals INTEGER,
        lost_crystals INTEGER,
        clan TEXT,
        clan_id INTEGER,
        brotherhood TEXT,
        brotherhood_id INTEGER,
        PRIMARY KEY(snapshot_id, pid),
        FOREIGN KEY(snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_heroes_snap_level ON heroes(snapshot_id, level);")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS best30(
        best_for_snapshot_id TEXT NOT NULL,
        param TEXT NOT NULL,
        pid INTEGER NOT NULL,
        name TEXT,
        level INTEGER,
        diff INTEGER,
        PRIMARY KEY(best_for_snapshot_id, param, pid)
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_best30_lookup ON best30(best_for_snapshot_id, param, diff);")


def upsert_snapshot(conn: sqlite3.Connection, snapshot_id: str, ts: int) -> None:
    conn.execute("INSERT OR REPLACE INTO snapshots(id, ts) VALUES(?,?)", (snapshot_id, ts))


def replace_heroes(conn: sqlite3.Connection, snapshot_id: str, rows: List[Tuple]) -> None:
    conn.execute("DELETE FROM heroes WHERE snapshot_id=?", (snapshot_id,))
    conn.executemany("""
    INSERT INTO heroes(
        snapshot_id, pid, name, level,
        glory, wins, losses, dragon_wins, snake_wins, beasts_killed,
        strength, defense, dexterity, mastery, vitality,
        rob_silver, lost_silver, rob_crystals, lost_crystals,
        clan, clan_id, brotherhood, brotherhood_id
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)


def sync_db_to_disk(conn: sqlite3.Connection, keep_ids: List[str]) -> None:
    """
    Удаляет из БД snapshots, которых нет среди keep_ids.
    Это убирает "призраков" старых snapshot_id (например старые .json),
    даже если файлов на диске уже нет.
    """
    if not keep_ids:
        conn.execute("DELETE FROM snapshots;")
        return
    placeholders = ",".join(["?"] * len(keep_ids))
    conn.execute(f"DELETE FROM snapshots WHERE id NOT IN ({placeholders})", tuple(keep_ids))


def compute_best30(conn: sqlite3.Connection, best_for_snapshot_id: str,
                   window_days: int = 30, max_gap_hours: float = 26.0, top_n: int = 1000) -> None:
    # Держим best30 только для текущего best_for
    conn.execute("DELETE FROM best30 WHERE best_for_snapshot_id != ?", (best_for_snapshot_id,))
    conn.execute("DELETE FROM best30 WHERE best_for_snapshot_id = ?", (best_for_snapshot_id,))

    latest_ts_row = conn.execute("SELECT ts FROM snapshots WHERE id=?", (best_for_snapshot_id,)).fetchone()
    if not latest_ts_row:
        return

    latest_ts = int(latest_ts_row[0])
    min_ts = latest_ts - window_days * 86400

    snaps = conn.execute(
        "SELECT id, ts FROM snapshots WHERE ts>=? AND ts<=? ORDER BY ts ASC",
        (min_ts, latest_ts)
    ).fetchall()
    if len(snaps) < 2:
        return

    def fetch_map(sid: str) -> Dict[int, sqlite3.Row]:
        cur = conn.execute("""
            SELECT pid, name, level,
                   glory, wins, losses, dragon_wins, snake_wins, beasts_killed,
                   strength, defense, dexterity, mastery, vitality,
                   rob_silver, lost_silver, rob_crystals, lost_crystals
            FROM heroes
            WHERE snapshot_id=?
        """, (sid,))
        rows = cur.fetchall()
        return {int(r[0]): r for r in rows}

    # param -> pid -> (diff, name, level, pid)
    best: Dict[str, Dict[int, Tuple[int, str, int, int]]] = {p: {} for p in BEST_PARAMS}

    prev_sid, prev_ts = snaps[0]
    prev_map = fetch_map(prev_sid)

    for sid, ts in snaps[1:]:
        gap_h = (int(ts) - int(prev_ts)) / 3600.0
        cur_map = fetch_map(sid)

        if gap_h <= max_gap_hours:
            common = prev_map.keys() & cur_map.keys()
            for pid in common:
                a = cur_map[pid]
                b = prev_map[pid]

                name = (a[1] or "").strip()
                level = int(a[2] or 0)

                cur_glory = int(a[3] or 0); prev_glory = int(b[3] or 0)
                cur_wins  = int(a[4] or 0); prev_wins  = int(b[4] or 0)
                cur_losses= int(a[5] or 0); prev_losses= int(b[5] or 0)
                cur_dw    = int(a[6] or 0); prev_dw    = int(b[6] or 0)
                cur_sw    = int(a[7] or 0); prev_sw    = int(b[7] or 0)
                cur_bk    = int(a[8] or 0); prev_bk    = int(b[8] or 0)

                cur_stats = [int(a[i] or 0) for i in range(9, 14)]
                prev_stats= [int(b[i] or 0) for i in range(9, 14)]

                cur_rob_s = int(a[14] or 0); prev_rob_s = int(b[14] or 0)
                cur_lost_s= int(a[15] or 0); prev_lost_s= int(b[15] or 0)
                cur_rob_c = int(a[16] or 0); prev_rob_c = int(b[16] or 0)
                cur_lost_c= int(a[17] or 0); prev_lost_c= int(b[17] or 0)

                diffs = {
                    "Слава": cur_glory - prev_glory,
                    "Побед": cur_wins - prev_wins,
                    "Поражений": cur_losses - prev_losses,
                    "Побед над Драконом": cur_dw - prev_dw,
                    "Побед над Змеем": cur_sw - prev_sw,
                    "Убито зверей": cur_bk - prev_bk,
                    "Награбил (серебро)": cur_rob_s - prev_rob_s,
                    "Потерял (серебро)": cur_lost_s - prev_lost_s,
                    "Награбил (кристаллы)": cur_rob_c - prev_rob_c,
                    "Потерял (кристаллы)": cur_lost_c - prev_lost_c,
                }

                for idx, k in enumerate(STAT_KEYS):
                    diffs[k] = cur_stats[idx] - prev_stats[idx]

                diffs["Сумма статов"] = sum(cur_stats) - sum(prev_stats)

                for param, diff in diffs.items():
                    old = best[param].get(pid)
                    if old is None or diff > old[0]:
                        best[param][pid] = (diff, name, level, pid)

        prev_sid, prev_ts = sid, ts
        prev_map = cur_map

    for param, by_pid in best.items():
        rows = sorted(by_pid.values(), key=lambda t: t[0], reverse=True)[:top_n]
        conn.executemany(
            "INSERT OR REPLACE INTO best30(best_for_snapshot_id, param, pid, name, level, diff) VALUES(?,?,?,?,?,?)",
            [(best_for_snapshot_id, param, pid, name, level, diff) for (diff, name, level, pid) in rows]
        )


def pick_int(hero: dict, keys: List[str], default: int = 0) -> int:
    for k in keys:
        v = hero.get(k)
        if v is None:
            continue
        try:
            return int(v)
        except Exception:
            pass
    return default


def pick_str(hero: dict, keys: List[str], default: str = "") -> str:
    for k in keys:
        v = hero.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return default


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--db-path", default="data/db/ratings.sqlite")
    ap.add_argument("--keep", type=int, default=40)
    ap.add_argument("--best-window-days", type=int, default=30)
    ap.add_argument("--delete-pruned-files", action="store_true")  # НЕ используй в GitHub Action, если не хочешь удалений
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    snaps = list_snapshots(data_dir)
    if not snaps:
        print("No snapshots found in", data_dir)
        return 0

    import_snaps = snaps[: max(args.keep, 2)]
    keep_ids = [sid for sid, _ in import_snaps]

    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        init_db(conn)

        conn.execute("BEGIN;")

        # Sync DB to current "keep window" on disk (убирает старые/лишние snapshot_id в БД)
        sync_db_to_disk(conn, keep_ids)

        # Импортируем от старых к новым
        for sid, ts in reversed(import_snaps):
            upsert_snapshot(conn, sid, ts)
            path = data_dir / sid
            if not path.exists():
                continue

            snap = load_snapshot(path)
            rows: List[Tuple] = []

            for pid_str, hero in snap.items():
                try:
                    pid = int(pid_str)
                except ValueError:
                    continue

                # ВАЖНО: это твой реальный формат снапшота
                name  = pick_str(hero, ["Имя", "имя", "name", "nick", "Ник"])
                level = pick_int(hero, ["Уровень", "уровень", "level"])

                glory = pick_int(hero, ["Слава", "glory"])
                wins  = pick_int(hero, ["Побед", "wins"])
                losses= pick_int(hero, ["Поражений", "losses"])
                dragon_wins = pick_int(hero, ["Побед над Драконом", "dragon_wins"])
                snake_wins  = pick_int(hero, ["Побед над Змеем", "snake_wins"])
                beasts_killed = pick_int(hero, ["Убито зверей", "beasts_killed"])

                strength = pick_int(hero, ["Сила", "strength"])
                defense  = pick_int(hero, ["Защита", "defense"])
                dexterity= pick_int(hero, ["Ловкость", "dexterity"])
                mastery  = pick_int(hero, ["Мастерство", "mastery"])
                vitality = pick_int(hero, ["Живучесть", "vitality"])

                rob_silver = pick_int(hero, ["Награбил (серебро)", "rob_silver"])
                lost_silver= pick_int(hero, ["Потерял (серебро)", "lost_silver"])
                rob_crystals = pick_int(hero, ["Награбил (кристаллы)", "rob_crystals"])
                lost_crystals= pick_int(hero, ["Потерял (кристаллы)", "lost_crystals"])

                clan = pick_str(hero, ["Клан", "clan"], default=None)  # type: ignore
                clan_id = hero.get("clan_id") or hero.get("Клан_id") or hero.get("клан_id")
                brotherhood = pick_str(hero, ["Братство", "brotherhood"], default=None)  # type: ignore
                brotherhood_id = hero.get("brotherhood_id") or hero.get("Братство_id") or hero.get("братство_id")

                try:
                    clan_id = int(clan_id) if clan_id is not None else None
                except Exception:
                    clan_id = None
                try:
                    brotherhood_id = int(brotherhood_id) if brotherhood_id is not None else None
                except Exception:
                    brotherhood_id = None

                rows.append((
                    sid, pid, name, level,
                    glory, wins, losses, dragon_wins, snake_wins, beasts_killed,
                    strength, defense, dexterity, mastery, vitality,
                    rob_silver, lost_silver, rob_crystals, lost_crystals,
                    clan, clan_id, brotherhood, brotherhood_id
                ))

            replace_heroes(conn, sid, rows)

        conn.execute("COMMIT;")

        # Файлы удаляем только если ты явно попросил
        if args.delete_pruned_files:
            keep_set = set(keep_ids)
            for p in data_dir.glob("heroes_*.json*"):
                if p.name not in keep_set:
                    try:
                        p.unlink()
                    except Exception:
                        pass

        # Best30 для самого свежего snapshot_id
        latest = conn.execute("SELECT id FROM snapshots ORDER BY ts DESC LIMIT 1").fetchone()
        if latest:
            best_for = latest[0]
            conn.execute("BEGIN;")
            compute_best30(conn, best_for_snapshot_id=best_for,
                           window_days=args.best_window_days, top_n=1000)
            conn.execute("COMMIT;")

        print(f"OK: DB updated at {db_path} (snapshots kept: {args.keep})")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
