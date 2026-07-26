"""Shared ForGlory collector and database helpers."""

from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import sys
from pathlib import Path


_EMPTY_GROUP_NAMES = {"", "не состоит", "none", "null"}


def _is_enabled(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().casefold() not in {"", "0", "false", "no", "off"}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _repair_missing_group_ids(db_path: Path) -> tuple[int, int]:
    """Restore stable fallback IDs for named groups that have no game ID.

    Historical snapshots may contain a clan/brotherhood name while their numeric
    game ID is NULL or zero. The web group query intentionally ignores zero IDs,
    which previously made the whole rating appear empty. A negative text ID is a
    deterministic local fallback and cannot collide with real positive game IDs.
    """
    if not db_path.exists() or db_path.stat().st_size == 0:
        return 0, 0

    conn = sqlite3.connect(db_path)
    try:
        observation_columns = _table_columns(conn, "observations")
        text_columns = _table_columns(conn, "text_values")
        required_observation_columns = {
            "clan_name_id",
            "clan_game_id",
            "brotherhood_name_id",
            "brotherhood_game_id",
        }
        if not required_observation_columns.issubset(observation_columns):
            return 0, 0
        if not {"text_id", "value"}.issubset(text_columns):
            return 0, 0

        placeholders = ",".join("?" for _ in _EMPTY_GROUP_NAMES)
        excluded_names = tuple(sorted(_EMPTY_GROUP_NAMES))

        clan_cursor = conn.execute(
            f"""
            UPDATE observations
            SET clan_game_id = -ABS(clan_name_id)
            WHERE (clan_game_id IS NULL OR clan_game_id = 0)
              AND clan_name_id IS NOT NULL
              AND EXISTS (
                  SELECT 1
                  FROM text_values t
                  WHERE t.text_id = observations.clan_name_id
                    AND LOWER(TRIM(t.value)) NOT IN ({placeholders})
              )
            """,
            excluded_names,
        )
        brotherhood_cursor = conn.execute(
            f"""
            UPDATE observations
            SET brotherhood_game_id = -ABS(brotherhood_name_id)
            WHERE (brotherhood_game_id IS NULL OR brotherhood_game_id = 0)
              AND brotherhood_name_id IS NOT NULL
              AND EXISTS (
                  SELECT 1
                  FROM text_values t
                  WHERE t.text_id = observations.brotherhood_name_id
                    AND LOWER(TRIM(t.value)) NOT IN ({placeholders})
              )
            """,
            excluded_names,
        )
        conn.commit()
        return max(0, clan_cursor.rowcount), max(0, brotherhood_cursor.rowcount)
    finally:
        conn.close()


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

    # Render runs on Linux. Import here so local Windows development can still
    # import the package without requiring fcntl.
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

        repaired_clans, repaired_brotherhoods = _repair_missing_group_ids(db_path)
        print(
            "Render startup: group identifiers checked; "
            f"repaired clans={repaired_clans}, "
            f"brotherhoods={repaired_brotherhoods}.",
            flush=True,
        )

        marker.write_text(str(db_path), encoding="utf-8")
        print("Render startup: SQLite refresh completed.", flush=True)


def _normalize_sql(sql: str) -> str:
    compact = " ".join(str(sql).split()).casefold()
    return re.sub(r"\s*,\s*", ",", compact)


_BALANCE_MAX_QUERY = _normalize_sql(
    """
    SELECT level,
           MAX(strength) AS strength,
           MAX(defense) AS defense,
           MAX(dexterity) AS dexterity,
           MAX(mastery) AS mastery,
           MAX(vitality) AS vitality
    FROM observations
    WHERE snapshot_id=? AND level IS NOT NULL
    GROUP BY level
    """
)

_BALANCE_TOP10_AVERAGE_QUERY = """
WITH ranked AS (
    SELECT
        level,
        strength,
        defense,
        dexterity,
        mastery,
        vitality,
        ROW_NUMBER() OVER (
            PARTITION BY level ORDER BY strength DESC, pid ASC
        ) AS strength_rank,
        ROW_NUMBER() OVER (
            PARTITION BY level ORDER BY defense DESC, pid ASC
        ) AS defense_rank,
        ROW_NUMBER() OVER (
            PARTITION BY level ORDER BY dexterity DESC, pid ASC
        ) AS dexterity_rank,
        ROW_NUMBER() OVER (
            PARTITION BY level ORDER BY mastery DESC, pid ASC
        ) AS mastery_rank,
        ROW_NUMBER() OVER (
            PARTITION BY level ORDER BY vitality DESC, pid ASC
        ) AS vitality_rank
    FROM observations
    WHERE snapshot_id=? AND level IS NOT NULL
)
SELECT
    level,
    AVG(CASE WHEN strength_rank <= 10 THEN strength END) AS strength,
    AVG(CASE WHEN defense_rank <= 10 THEN defense END) AS defense,
    AVG(CASE WHEN dexterity_rank <= 10 THEN dexterity END) AS dexterity,
    AVG(CASE WHEN mastery_rank <= 10 THEN mastery END) AS mastery,
    AVG(CASE WHEN vitality_rank <= 10 THEN vitality END) AS vitality
FROM ranked
GROUP BY level
"""


def _install_balance_top10_query() -> None:
    """Use the top-10 average for the Ritual of Balance 75% cap.

    ``app.query_level_balance`` historically requests MAX() values and then
    multiplies them by 75%. The game description instead limits every stat to
    75% of the average of the ten strongest values on the current level. The
    connection subclass rewrites only that exact read-only aggregate query;
    all other SQLite statements retain their normal behaviour.
    """
    if getattr(sqlite3.connect, "_forglory_balance_top10", False):
        return

    original_connect = sqlite3.connect

    class ForGloryConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):  # type: ignore[override]
            normalized = _normalize_sql(sql)
            if normalized == _BALANCE_MAX_QUERY:
                sql = _BALANCE_TOP10_AVERAGE_QUERY
            return super().execute(sql, parameters)

    def connect_with_balance_top10(*args, **kwargs):
        kwargs.setdefault("factory", ForGloryConnection)
        return original_connect(*args, **kwargs)

    connect_with_balance_top10._forglory_balance_top10 = True
    connect_with_balance_top10.__wrapped__ = original_connect
    sqlite3.connect = connect_with_balance_top10


_refresh_render_database()
_install_balance_top10_query()
