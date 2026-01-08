#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import gzip
import json
import sqlite3
from pathlib import Path


def export_snapshot(db_path: Path, snapshot_id: str, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / snapshot_id

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        # Проверим, что снапшот есть в БД
        s = conn.execute("SELECT 1 FROM snapshots WHERE id=? LIMIT 1", (snapshot_id,)).fetchone()
        if not s:
            raise SystemExit(f"Snapshot id not found in DB: {snapshot_id}")

        rows = conn.execute(
            """
            SELECT
              pid, name, level,
              glory, wins, losses, dragon_wins, snake_wins, beasts_killed,
              strength, defense, dexterity, mastery, vitality,
              rob_silver, lost_silver, rob_crystals, lost_crystals,
              clan, clan_id, brotherhood, brotherhood_id
            FROM heroes
            WHERE snapshot_id=?
            ORDER BY pid
            """,
            (snapshot_id,),
        ).fetchall()

        if not rows:
            raise SystemExit(f"No heroes rows for snapshot: {snapshot_id}")

        # Восстанавливаем формат снапшота (русские ключи)
        data = {}
        for r in rows:
            pid = int(r["pid"])
            hero = {
                "Имя": r["name"] or "",
                "Уровень": int(r["level"] or 0),

                "Слава": int(r["glory"] or 0),
                "Побед": int(r["wins"] or 0),
                "Поражений": int(r["losses"] or 0),
                "Побед над Драконом": int(r["dragon_wins"] or 0),
                "Побед над Змеем": int(r["snake_wins"] or 0),
                "Убито зверей": int(r["beasts_killed"] or 0),

                "Сила": int(r["strength"] or 0),
                "Защита": int(r["defense"] or 0),
                "Ловкость": int(r["dexterity"] or 0),
                "Мастерство": int(r["mastery"] or 0),
                "Живучесть": int(r["vitality"] or 0),

                "Награбил (серебро)": int(r["rob_silver"] or 0),
                "Потерял (серебро)": int(r["lost_silver"] or 0),
                "Награбил (кристаллы)": int(r["rob_crystals"] or 0),
                "Потерял (кристаллы)": int(r["lost_crystals"] or 0),
            }

            # Кланы/братства — если есть
            if r["clan"]:
                hero["Клан"] = r["clan"]
            if r["clan_id"] is not None:
                hero["clan_id"] = int(r["clan_id"])
            if r["brotherhood"]:
                hero["Братство"] = r["brotherhood"]
            if r["brotherhood_id"] is not None:
                hero["brotherhood_id"] = int(r["brotherhood_id"])

            data[str(pid)] = hero

        # Пишем как .json.gz
        with gzip.open(out_path, "wt", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

        return out_path
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/db/ratings.sqlite", help="Path to SQLite DB")
    ap.add_argument("--snapshot-id", required=True, help="Snapshot id, e.g. heroes_YYYY-MM-DD_HH-MM-SS.json.gz")
    ap.add_argument("--out-dir", default="data", help="Directory to write snapshot file into")
    args = ap.parse_args()

    out = export_snapshot(Path(args.db), args.snapshot_id, Path(args.out_dir))
    print("OK exported:", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
