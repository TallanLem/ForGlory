#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import os
import shutil
import sqlite3
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate and deterministically gzip a SQLite database")
    parser.add_argument("--db", default="data/db/ratings.sqlite")
    parser.add_argument("--out", default="data/db/ratings.sqlite.gz")
    parser.add_argument("--level", type=int, default=6)
    args = parser.parse_args()

    db_path = Path(args.db)
    out_path = Path(args.out)
    if not db_path.exists():
        raise SystemExit(f"Database does not exist: {db_path}")

    conn = sqlite3.connect(f"file:{db_path.resolve().as_posix()}?mode=ro", uri=True)
    try:
        result = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if result != "ok":
            raise SystemExit(f"Database integrity check failed: {result}")
    finally:
        conn.close()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with db_path.open("rb") as source, temp_path.open("wb") as raw_target:
        with gzip.GzipFile(
            filename=db_path.name,
            mode="wb",
            fileobj=raw_target,
            compresslevel=max(1, min(9, args.level)),
            mtime=0,
        ) as target:
            shutil.copyfileobj(source, target, length=1024 * 1024)
    os.replace(temp_path, out_path)
    print(f"OK: {db_path} -> {out_path} ({out_path.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
