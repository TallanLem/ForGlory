"""Shared ForGlory collector and database helpers."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _is_enabled(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().casefold() not in {"", "0", "false", "no", "off"}


def _refresh_render_database() -> None:
    """Download db-latest once per Render runtime instance before Flask imports."""
    root = Path(__file__).resolve().parents[1]
    is_render = (
        _is_enabled(os.environ.get("RENDER"))
        or bool(os.environ.get("RENDER_SERVICE_ID"))
        or "/opt/render/" in root.as_posix()
    )
    if not is_render:
        return

    if not _is_enabled(os.environ.get("REFRESH_DB_ON_START"), default=True):
        print("Render database refresh is disabled by REFRESH_DB_ON_START.", flush=True)
        return

    script = root / "tools" / "fetch_db_from_release.py"
    db_path = Path(
        os.environ.get("DB_PATH", str(root / "data" / "db" / "ratings.sqlite"))
    ).expanduser()

    if not script.exists():
        raise RuntimeError(f"Database loader not found: {script}")

    marker = Path("/tmp/forglory-db-refresh.done")
    lock_path = Path("/tmp/forglory-db-refresh.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    # Gunicorn currently preloads the app, but the lock+marker also protects
    # against duplicate downloads if several workers import it independently.
    import fcntl

    with lock_path.open("w", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if marker.exists():
            return

        print(
            f"Render startup: refreshing SQLite from GitHub Release db-latest -> {db_path}",
            flush=True,
        )
        timeout = max(60, int(os.environ.get("DB_DOWNLOAD_TIMEOUT_SECONDS", "600")))
        subprocess.run(
            [
                sys.executable,
                str(script),
                "--out",
                str(db_path),
                "--attempts",
                "3",
                "--retry-delay",
                "5",
            ],
            cwd=root,
            check=True,
            timeout=timeout,
        )
        marker.write_text(str(db_path), encoding="utf-8")
        print("Render startup: SQLite refresh completed.", flush=True)


_refresh_render_database()
