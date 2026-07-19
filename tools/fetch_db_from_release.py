#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import os
import shutil
import sqlite3
import sys
import urllib.error
import urllib.request
from pathlib import Path


def request(url: str, token: str = "") -> urllib.request.Request:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "ForGlory-DB-Loader",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return urllib.request.Request(url, headers=headers)


def validate(path: Path, required_schema_version: int | None = None) -> None:
    with path.open("rb") as handle:
        header = handle.read(16)
    if header != b"SQLite format 3\x00":
        raise RuntimeError("Downloaded file is not a SQLite database")
    conn = sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)
    try:
        result = conn.execute("PRAGMA quick_check").fetchone()[0]
        if result != "ok":
            raise RuntimeError(f"Downloaded database failed quick_check: {result}")
        if required_schema_version is not None:
            try:
                row = conn.execute(
                    "SELECT value FROM schema_meta WHERE key='schema_version'"
                ).fetchone()
                actual = int(row[0]) if row else None
            except sqlite3.Error:
                actual = None
            if actual != required_schema_version:
                raise RuntimeError(
                    f"Database schema version is {actual}; required {required_schema_version}. "
                    "Run the GitHub data workflow once to upgrade the Release database."
                )
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Stream the latest SQLite release asset to disk")
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPO", "TallanLem/ForGlory"))
    parser.add_argument("--tag", default=os.environ.get("DB_RELEASE_TAG", "db-latest"))
    parser.add_argument("--out", default=os.environ.get("DB_PATH", "data/db/ratings.sqlite"))
    parser.add_argument("--asset", default=os.environ.get("DB_ASSET_NAME", "ratings.sqlite.gz"))
    parser.add_argument("--optional", action="store_true")
    parser.add_argument("--require-schema-version", type=int)
    args = parser.parse_args()

    token = os.environ.get("GITHUB_TOKEN", "")
    api_url = f"https://api.github.com/repos/{args.repo}/releases/tags/{args.tag}"
    try:
        with urllib.request.urlopen(request(api_url, token), timeout=30) as response:
            release = json.load(response)
    except urllib.error.HTTPError as exc:
        if args.optional and exc.code == 404:
            print("Database release does not exist yet; continuing without it.")
            return 0
        raise

    preferred = [args.asset]
    if args.asset.endswith(".gz"):
        preferred.append(args.asset[:-3])
    else:
        preferred.append(args.asset + ".gz")
    assets = {asset.get("name"): asset.get("browser_download_url") for asset in release.get("assets", [])}
    asset_name = next((name for name in preferred if assets.get(name)), None)
    if not asset_name:
        if args.optional:
            print(f"No supported DB asset found in release {args.tag}; continuing without it.")
            return 0
        raise SystemExit(f"No supported DB asset found. Tried: {', '.join(preferred)}")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    temp_download = out_path.with_suffix(out_path.suffix + ".download")
    temp_db = out_path.with_suffix(out_path.suffix + ".tmp")
    for path in (temp_download, temp_db):
        path.unlink(missing_ok=True)

    print(f"Downloading {asset_name} from GitHub Release {args.tag}")
    with urllib.request.urlopen(request(str(assets[asset_name]), token), timeout=120) as response:
        with temp_download.open("wb") as target:
            shutil.copyfileobj(response, target, length=1024 * 1024)

    if asset_name.endswith(".gz"):
        with gzip.open(temp_download, "rb") as source, temp_db.open("wb") as target:
            shutil.copyfileobj(source, target, length=1024 * 1024)
    else:
        os.replace(temp_download, temp_db)
    temp_download.unlink(missing_ok=True)

    validate(temp_db, args.require_schema_version)
    os.replace(temp_db, out_path)
    print(f"Saved database to {out_path} ({out_path.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
