#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(command: list[str]) -> None:
    print("+", " ".join(command))
    subprocess.run(command, cwd=ROOT, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "One-time history migration: import every heroes_*.json(.gz) into a compact SQLite DB, "
            "rebuild first/second-seen metadata, and optionally publish it to GitHub Release."
        )
    )
    parser.add_argument("--source-dir", default="data")
    parser.add_argument("--db", default="data/db/ratings.sqlite")
    parser.add_argument("--compressed", default="data/db/ratings.sqlite.gz")
    parser.add_argument("--incremental", action="store_true", help="Keep an existing DB instead of rebuilding it")
    parser.add_argument("--publish", action="store_true", help="Upload the compressed DB with GitHub CLI")
    parser.add_argument("--repo", default="TallanLem/ForGlory")
    parser.add_argument("--tag", default="db-latest")
    args = parser.parse_args()

    source_dir = Path(args.source_dir)
    files = list(source_dir.glob("heroes_*.json")) + list(source_dir.glob("heroes_*.json.gz"))
    if not files:
        raise SystemExit(f"No history files found in {source_dir}")
    print(f"Found {len(files)} snapshot files in {source_dir}")

    build = [
        sys.executable,
        "tools/build_db.py",
        "--data-dir", str(source_dir),
        "--db-path", args.db,
        "--best-window-days", "30",
        "--vacuum",
    ]
    if not args.incremental:
        build.append("--rebuild")
    run(build)
    run([sys.executable, "tools/compress_db.py", "--db", args.db, "--out", args.compressed])

    if args.publish:
        gh = shutil.which("gh")
        if not gh:
            raise SystemExit("GitHub CLI (gh) is required for --publish")
        view = subprocess.run([gh, "release", "view", args.tag, "--repo", args.repo], cwd=ROOT)
        if view.returncode != 0:
            run([gh, "release", "create", args.tag, "--repo", args.repo, "--title", "Database (latest)"])
        run([gh, "release", "upload", args.tag, args.compressed, "--repo", args.repo, "--clobber"])
        print("Published database release asset.")
    else:
        print(
            "Migration complete. Upload the compressed file to GitHub Release tag "
            f"'{args.tag}', or rerun with --publish after authenticating GitHub CLI."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
