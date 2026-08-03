"""Shared ForGlory collector and database helpers."""

from __future__ import annotations

import importlib.abc
import importlib.machinery
import os
import re
import sqlite3
import subprocess
import sys
from functools import wraps
from pathlib import Path


_LORD_WINS_PARAM = "Побед над Владыкой"
_SERPENT_WINS_PARAM = "Побед над Змеем"


def _insert_after(values: list[str], anchor: str, value: str) -> None:
    if value in values:
        return
    try:
        index = values.index(anchor) + 1
    except ValueError:
        values.append(value)
    else:
        values.insert(index, value)


def _patch_personal_stats_query(namespace: dict) -> bool:
    query = namespace.get("query_personal_stats")
    get_db = namespace.get("get_db")
    if not callable(query) or not callable(get_db):
        return False
    if getattr(query, "_forglory_best_rank", False):
        return True

    @wraps(query)
    def query_with_best_rank(pid: int, snap_from: str, snap_to: str):
        result = query(pid, snap_from, snap_to)
        if not result:
            return result

        latest = get_db().execute(
            "SELECT snapshot_id FROM snapshots ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        ranks: dict[str, int] = {}
        if latest is not None:
            ranked_rows = get_db().execute(
                """
                WITH ranked AS (
                    SELECT param,pid,
                           ROW_NUMBER() OVER(
                               PARTITION BY param
                               ORDER BY diff DESC,pid ASC
                           ) AS rank
                    FROM best_growth
                    WHERE best_for_snapshot_id=?
                )
                SELECT param,rank
                FROM ranked
                WHERE pid=?
                """,
                (int(latest[0]), int(pid)),
            ).fetchall()
            ranks = {str(row["param"]): int(row["rank"]) for row in ranked_rows}

        for row in result.get("rows", []):
            row["best_rank"] = (
                ranks.get(str(row.get("param")))
                if row.get("best_diff") is not None
                else None
            )
        return result

    query_with_best_rank._forglory_best_rank = True
    namespace["query_personal_stats"] = query_with_best_rank
    return True


def _patch_app_parameter_lists(namespace: dict) -> bool:
    options = namespace.get("param_options")
    personal = namespace.get("PERSONAL_PARAMS")
    lists_ready = isinstance(options, list) and isinstance(personal, list)
    if lists_ready:
        _insert_after(options, _SERPENT_WINS_PARAM, _LORD_WINS_PARAM)
        _insert_after(personal, _SERPENT_WINS_PARAM, _LORD_WINS_PARAM)

    personal_query_ready = _patch_personal_stats_query(namespace)
    return lists_ready and personal_query_ready


class _AppParameterLoader(importlib.abc.Loader):
    def __init__(self, original_loader) -> None:
        self.original_loader = original_loader

    def create_module(self, spec):
        creator = getattr(self.original_loader, "create_module", None)
        return creator(spec) if creator is not None else None

    def exec_module(self, module) -> None:
        self.original_loader.exec_module(module)
        _patch_app_parameter_lists(module.__dict__)


class _AppParameterFinder(importlib.abc.MetaPathFinder):
    _forglory_app_parameter_finder = True

    def find_spec(self, fullname, path=None, target=None):
        if fullname != "app":
            return None
        spec = importlib.machinery.PathFinder.find_spec(fullname, path, target)
        if spec is None or spec.loader is None:
            return spec
        if not isinstance(spec.loader, _AppParameterLoader):
            spec.loader = _AppParameterLoader(spec.loader)
        return spec


def _install_app_parameter_patch() -> None:
    """Patch app.py selectors and personal best-growth ranks at import time.

    The web module keeps its selector lists and personal-stat query in app.py.
    A small import wrapper patches future ``import app`` calls after the module
    has finished loading. When this package is first imported from inside app.py
    itself, a one-shot trace waits until both targets have been defined.
    """
    loaded_app = sys.modules.get("app")
    if loaded_app is not None and _patch_app_parameter_lists(vars(loaded_app)):
        return

    if not any(
        getattr(finder, "_forglory_app_parameter_finder", False)
        for finder in sys.meta_path
    ):
        sys.meta_path.insert(0, _AppParameterFinder())

    frame = sys._getframe()
    app_frame = None
    while frame is not None:
        module_name = frame.f_globals.get("__name__")
        module_file = str(frame.f_globals.get("__file__") or "").replace("\\", "/")
        if module_name == "app" or (
            module_name == "__main__" and module_file.endswith("/app.py")
        ):
            app_frame = frame
            break
        frame = frame.f_back
    if app_frame is None:
        return

    previous_trace = sys.gettrace()

    def trace(target, event, arg):
        if target is app_frame and event == "line":
            if _patch_app_parameter_lists(target.f_globals):
                target.f_trace = None
                sys.settrace(previous_trace)
                return None
        return trace

    app_frame.f_trace = trace
    sys.settrace(trace)


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
_install_app_parameter_patch()
