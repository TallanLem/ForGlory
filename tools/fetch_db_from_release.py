#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


def request(
    url: str,
    token: str = "",
    accept: str = "application/vnd.github+json",
) -> urllib.request.Request:
    headers = {
        "Accept": accept,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "User-Agent": "ForGlory-DB-Loader",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return urllib.request.Request(url, headers=headers)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate(
    path: Path,
    required_schema_version: int | None = None,
) -> tuple[str | None, int, str]:
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
                    f"Database schema version is {actual}; required "
                    f"{required_schema_version}. Run the GitHub data workflow "
                    "once to upgrade the Release database."
                )

        try:
            row = conn.execute(
                "SELECT filename FROM snapshots ORDER BY ts DESC LIMIT 1"
            ).fetchone()
            snapshot_count = int(
                conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
            )
        except sqlite3.Error:
            row = None
            snapshot_count = 0
        return (str(row[0]) if row else None, snapshot_count, file_sha256(path))
    finally:
        conn.close()


def fetch_release(repo: str, tag: str, token: str) -> dict:
    url = f"https://api.github.com/repos/{repo}/releases/tags/{tag}"
    with urllib.request.urlopen(request(url, token), timeout=30) as response:
        return json.load(response)


def choose_asset(release: dict, preferred: list[str]) -> tuple[str, dict] | None:
    assets = {
        asset.get("name"): asset
        for asset in release.get("assets", [])
        if asset.get("name")
    }
    for name in preferred:
        if assets.get(name):
            return name, assets[name]
    return None


def download_asset(asset: dict, token: str, target: Path) -> None:
    # The browser_download_url can serve a stale cached file when an asset with
    # the same name is replaced. The API asset URL is tied to the unique asset ID.
    asset_api_url = str(asset.get("url") or "")
    if not asset_api_url:
        asset_id = asset.get("id")
        if not asset_id:
            raise RuntimeError("Release asset has no API URL or asset ID")
        raise RuntimeError(f"Release asset {asset_id} has no API URL")

    with urllib.request.urlopen(
        request(asset_api_url, token, "application/octet-stream"),
        timeout=240,
    ) as response:
        content_type = str(response.headers.get("Content-Type") or "")
        if "json" in content_type.casefold():
            body = response.read(500).decode("utf-8", errors="replace")
            raise RuntimeError(
                "GitHub returned JSON instead of the release asset: " + body
            )
        with target.open("wb") as output:
            shutil.copyfileobj(response, output, length=1024 * 1024)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download and validate the latest SQLite release asset"
    )
    parser.add_argument(
        "--repo", default=os.environ.get("GITHUB_REPO", "TallanLem/ForGlory")
    )
    parser.add_argument(
        "--tag", default=os.environ.get("DB_RELEASE_TAG", "db-latest")
    )
    parser.add_argument(
        "--out", default=os.environ.get("DB_PATH", "data/db/ratings.sqlite")
    )
    parser.add_argument(
        "--asset", default=os.environ.get("DB_ASSET_NAME", "ratings.sqlite.gz")
    )
    parser.add_argument("--optional", action="store_true")
    parser.add_argument("--require-schema-version", type=int)
    parser.add_argument("--expect-latest-snapshot")
    parser.add_argument("--expect-sha256")
    parser.add_argument("--minimum-snapshot-count", type=int)
    parser.add_argument("--attempts", type=int, default=1)
    parser.add_argument("--retry-delay", type=float, default=10.0)
    args = parser.parse_args()

    token = os.environ.get("GITHUB_TOKEN", "")
    preferred = [args.asset]
    if args.asset.endswith(".gz"):
        preferred.append(args.asset[:-3])
    else:
        preferred.append(args.asset + ".gz")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    temp_download = out_path.with_suffix(out_path.suffix + ".download")
    temp_db = out_path.with_suffix(out_path.suffix + ".tmp")

    last_error: Exception | None = None
    attempts = max(1, args.attempts)
    for attempt in range(1, attempts + 1):
        for path in (temp_download, temp_db):
            path.unlink(missing_ok=True)
        try:
            try:
                release = fetch_release(args.repo, args.tag, token)
            except urllib.error.HTTPError as exc:
                if args.optional and exc.code == 404:
                    print("Database release does not exist yet; continuing without it.")
                    return 0
                raise

            selected = choose_asset(release, preferred)
            if selected is None:
                if args.optional:
                    print(
                        f"No supported DB asset found in release {args.tag}; "
                        "continuing without it."
                    )
                    return 0
                raise RuntimeError(
                    f"No supported DB asset found. Tried: {', '.join(preferred)}"
                )

            asset_name, asset = selected
            print(
                f"Downloading {asset_name} from GitHub Release {args.tag}; "
                f"asset_id={asset.get('id')}, updated_at={asset.get('updated_at')}, "
                f"attempt={attempt}/{attempts}"
            )
            download_asset(asset, token, temp_download)

            if asset_name.endswith(".gz"):
                with gzip.open(temp_download, "rb") as source, temp_db.open("wb") as target:
                    shutil.copyfileobj(source, target, length=1024 * 1024)
            else:
                os.replace(temp_download, temp_db)
            temp_download.unlink(missing_ok=True)

            latest_snapshot, snapshot_count, database_sha256 = validate(
                temp_db, args.require_schema_version
            )
            if (
                args.expect_latest_snapshot
                and latest_snapshot != args.expect_latest_snapshot
            ):
                raise RuntimeError(
                    "Published database is stale: "
                    f"latest_snapshot={latest_snapshot!r}, "
                    f"expected={args.expect_latest_snapshot!r}"
                )
            if (
                args.minimum_snapshot_count is not None
                and snapshot_count < args.minimum_snapshot_count
            ):
                raise RuntimeError(
                    "Published database lost history: "
                    f"snapshot_count={snapshot_count}, "
                    f"minimum={args.minimum_snapshot_count}"
                )
            if (
                args.expect_sha256
                and database_sha256.casefold() != args.expect_sha256.casefold()
            ):
                raise RuntimeError(
                    "Published database content differs from the local database: "
                    f"sha256={database_sha256}, expected={args.expect_sha256}"
                )

            os.replace(temp_db, out_path)
            print(
                f"Saved database to {out_path} ({out_path.stat().st_size} bytes); "
                f"latest_snapshot={latest_snapshot or 'unknown'}, "
                f"snapshot_count={snapshot_count}, sha256={database_sha256}"
            )
            return 0
        except Exception as exc:
            last_error = exc
            print(f"Database download attempt {attempt}/{attempts} failed: {exc}", file=sys.stderr)
            if attempt < attempts:
                time.sleep(max(0.0, args.retry_delay))

    for path in (temp_download, temp_db):
        path.unlink(missing_ok=True)
    if last_error is not None:
        raise last_error
    return 1


if __name__ == "__main__":
    sys.exit(main())
