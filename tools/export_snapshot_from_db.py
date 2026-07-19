#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from forglory.schema import NUMERIC_FIELDS  # noqa: E402


def export_snapshot(db_path: Path, snapshot_id: str, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    output_name = snapshot_id if snapshot_id.endswith(".gz") else snapshot_id + ".gz"
    out_path = out_dir / output_name
    conn = sqlite3.connect(f"file:{db_path.resolve().as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        exists = conn.execute(
            "SELECT 1 FROM snapshots WHERE filename=?", (snapshot_id,)
        ).fetchone()
        if not exists:
            raise SystemExit(f"Snapshot not found: {snapshot_id}")
        numeric = ",".join(field.column for field in NUMERIC_FIELDS)
        rows = conn.execute(
            f"""
            SELECT pid,name,clan,clan_id,brotherhood,brotherhood_id,{numeric}
            FROM heroes
            WHERE snapshot_id=?
            ORDER BY pid
            """,
            (snapshot_id,),
        ).fetchall()
        data = {}
        for row in rows:
            hero = {"ID": int(row["pid"]), "Имя": row["name"] or ""}
            for field in NUMERIC_FIELDS:
                if row[field.column] is not None:
                    hero[field.json_key] = int(row[field.column])
            if row["clan"] is not None:
                hero["Клан"] = row["clan"]
            if row["clan_id"] is not None:
                hero["clan_id"] = int(row["clan_id"])
            if row["brotherhood"] is not None:
                hero["Братство"] = row["brotherhood"]
            if row["brotherhood_id"] is not None:
                hero["brotherhood_id"] = int(row["brotherhood_id"])
            data[str(row["pid"])] = hero
        with gzip.open(out_path, "wt", encoding="utf-8", compresslevel=6) as handle:
            json.dump(data, handle, ensure_ascii=False, separators=(",", ":"))
        return out_path
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/db/ratings.sqlite")
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--out-dir", default="data/exported")
    args = parser.parse_args()
    output = export_snapshot(Path(args.db), args.snapshot_id, Path(args.out_dir))
    print("OK exported:", output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
