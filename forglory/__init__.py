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


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _patch_personal_stats_query(namespace: dict) -> bool:
    """Replace the N+1 personal-rating query with four batched SQL reads."""
    query = namespace.get("query_personal_stats")
    get_db = namespace.get("get_db")
    if not callable(query) or not callable(get_db):
        return False
    if getattr(query, "_forglory_personal_optimized", False):
        return True

    snapshot_info = namespace.get("snapshot_info")
    row_value = namespace.get("_row_value")
    player_value_expr = namespace.get("_player_value_expr")
    personal_params = namespace.get("PERSONAL_PARAMS")
    cached_query = namespace.get("cached_query")

    optimized_ready = (
        callable(snapshot_info)
        and callable(row_value)
        and callable(player_value_expr)
        and isinstance(personal_params, list)
        and bool(personal_params)
    )

    # Keep the former best-rank-only patch as a compatibility fallback for
    # partial test/import namespaces. Production app.py always takes the
    # optimized branch below.
    if not optimized_ready:
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

    params = [str(param) for param in personal_params]
    values_sql = ",".join(f"({_sql_literal(param)})" for param in params)

    def value_case(alias: str) -> str:
        clauses = " ".join(
            f"WHEN {_sql_literal(param)} THEN {player_value_expr(param, alias)}"
            for param in params
        )
        return f"CASE pv.param {clauses} END"

    overall_value = value_case("o")
    growth_clauses = " ".join(
        f"WHEN {_sql_literal(param)} THEN "
        f"({player_value_expr(param, 'c')}-{player_value_expr(param, 'p')})"
        for param in params
    )
    growth_value = f"CASE pv.param {growth_clauses} END"

    @wraps(query)
    def query_optimized(pid: int, snap_from: str, snap_to: str):
        from_info = snapshot_info(snap_from)
        to_info = snapshot_info(snap_to)
        if not from_info or not to_info:
            return None

        from_sid, from_ts = from_info
        to_sid, to_ts = to_info
        if from_ts > to_ts:
            from_sid, to_sid = to_sid, from_sid
            snap_from, snap_to = snap_to, snap_from

        db = get_db()
        observation_rows = db.execute(
            """
            SELECT o.*,n.value AS _player_name
            FROM observations o
            LEFT JOIN text_values n ON n.text_id=o.name_id
            WHERE o.pid=? AND o.snapshot_id IN (?,?)
            """,
            (int(pid), int(from_sid), int(to_sid)),
        ).fetchall()
        observations = {
            int(row["snapshot_id"]): row for row in observation_rows
        }
        start = observations.get(int(from_sid))
        end = observations.get(int(to_sid))
        if start is None or end is None:
            return None

        overall_rows = db.execute(
            f"""
            WITH param_values(param) AS (VALUES {values_sql}),
            values_by_param AS (
                SELECT pv.param,o.pid,{overall_value} AS value
                FROM observations o
                CROSS JOIN param_values pv
                WHERE o.snapshot_id=?
            ),
            ranked AS (
                SELECT param,pid,
                       ROW_NUMBER() OVER(
                           PARTITION BY param
                           ORDER BY value DESC,pid ASC
                       ) AS rank
                FROM values_by_param
                WHERE value IS NOT NULL
            )
            SELECT param,rank FROM ranked WHERE pid=?
            """,
            (int(to_sid), int(pid)),
        ).fetchall()
        overall_ranks = {
            str(row["param"]): int(row["rank"]) for row in overall_rows
        }

        growth_rows = db.execute(
            f"""
            WITH param_values(param) AS (VALUES {values_sql}),
            values_by_param AS (
                SELECT pv.param,c.pid,{growth_value} AS value
                FROM observations c
                JOIN observations p
                  ON p.snapshot_id=? AND p.pid=c.pid
                CROSS JOIN param_values pv
                WHERE c.snapshot_id=?
            ),
            ranked AS (
                SELECT param,pid,
                       ROW_NUMBER() OVER(
                           PARTITION BY param
                           ORDER BY value DESC,pid ASC
                       ) AS rank
                FROM values_by_param
                WHERE value IS NOT NULL
            )
            SELECT param,rank FROM ranked WHERE pid=?
            """,
            (int(from_sid), int(to_sid), int(pid)),
        ).fetchall()
        growth_ranks = {
            str(row["param"]): int(row["rank"]) for row in growth_rows
        }

        best_rows = db.execute(
            """
            WITH latest AS (
                SELECT snapshot_id
                FROM snapshots
                ORDER BY ts DESC
                LIMIT 1
            ),
            ranked AS (
                SELECT bg.param,bg.pid,bg.diff,bg.best_snapshot_id,
                       ROW_NUMBER() OVER(
                           PARTITION BY bg.param
                           ORDER BY bg.diff DESC,bg.pid ASC
                       ) AS rank
                FROM best_growth bg
                WHERE bg.best_for_snapshot_id=(SELECT snapshot_id FROM latest)
            )
            SELECT r.param,r.diff,s.filename AS best_snapshot,r.rank AS best_rank
            FROM ranked r
            JOIN snapshots s ON s.snapshot_id=r.best_snapshot_id
            WHERE r.pid=?
            """,
            (int(pid),),
        ).fetchall()
        best_by_param = {str(row["param"]): dict(row) for row in best_rows}

        rows_out = []
        for param in params:
            start_value = row_value(start, param)
            end_value = row_value(end, param)
            delta = (
                end_value - start_value
                if start_value is not None and end_value is not None
                else None
            )
            best = best_by_param.get(param)
            rows_out.append(
                {
                    "param": param,
                    "start": start_value,
                    "end": end_value,
                    "delta": delta,
                    "growth_rank": (
                        growth_ranks.get(param) if delta is not None else None
                    ),
                    "overall_rank": (
                        overall_ranks.get(param) if end_value is not None else None
                    ),
                    "best_diff": int(best["diff"]) if best else None,
                    "best_snapshot": best["best_snapshot"] if best else None,
                    "best_rank": int(best["best_rank"]) if best else None,
                }
            )

        return {
            "pid": int(pid),
            "name": end["_player_name"] or str(pid),
            "level": int(end["level"]) if end["level"] is not None else None,
            "file1": snap_from,
            "file2": snap_to,
            "rows": rows_out,
        }

    patched_query = (
        cached_query(query_optimized) if callable(cached_query) else query_optimized
    )
    patched_query._forglory_personal_optimized = True
    patched_query._forglory_best_rank = True
    namespace["query_personal_stats"] = patched_query
    return True


def _patch_player_suggestion_route(namespace: dict) -> bool:
    """Return at most three current players with their current levels."""
    flask_app = namespace.get("app")
    get_db = namespace.get("get_db")
    normalize_name = namespace.get("normalize_name")
    db_available = namespace.get("_db_available")
    request = namespace.get("request")
    jsonify = namespace.get("jsonify")
    if not all(
        callable(value)
        for value in (get_db, normalize_name, db_available, jsonify)
    ) or flask_app is None or request is None:
        return False

    current = getattr(flask_app, "view_functions", {}).get("api_player_suggest_all")
    if not callable(current):
        return False
    if getattr(current, "_forglory_compact_suggestions", False):
        return True

    @wraps(current)
    def compact_player_suggestions():
        if not db_available():
            return jsonify([])
        query_text = normalize_name(request.args.get("q") or "")
        if len(query_text) < 2:
            return jsonify([])

        rows = get_db().execute(
            """
            WITH latest AS (
                SELECT snapshot_id
                FROM snapshots
                ORDER BY ts DESC
                LIMIT 1
            ),
            matches AS (
                SELECT n.value AS name,n.norm AS name_norm,o.level,o.pid,
                       ROW_NUMBER() OVER(
                           PARTITION BY n.norm
                           ORDER BY o.pid ASC
                       ) AS same_name_rank
                FROM observations o
                JOIN latest l ON l.snapshot_id=o.snapshot_id
                JOIN text_values n ON n.text_id=o.name_id
                WHERE n.norm LIKE ?
            )
            SELECT name,name_norm,level,pid
            FROM matches
            WHERE same_name_rank=1
            ORDER BY CASE WHEN name_norm=? THEN 0 ELSE 1 END,name,pid
            LIMIT 3
            """,
            (f"%{query_text}%", query_text),
        ).fetchall()
        return jsonify(
            [
                {
                    "name": str(row["name"]),
                    "level": (
                        int(row["level"]) if row["level"] is not None else None
                    ),
                }
                for row in rows
            ]
        )

    compact_player_suggestions._forglory_compact_suggestions = True
    flask_app.view_functions["api_player_suggest_all"] = compact_player_suggestions
    namespace["api_player_suggest_all"] = compact_player_suggestions
    return True


def _patch_app_parameter_lists(namespace: dict) -> bool:
    options = namespace.get("param_options")
    personal = namespace.get("PERSONAL_PARAMS")
    lists_ready = isinstance(options, list) and isinstance(personal, list)
    if lists_ready:
        _insert_after(options, _SERPENT_WINS_PARAM, _LORD_WINS_PARAM)
        _insert_after(personal, _SERPENT_WINS_PARAM, _LORD_WINS_PARAM)

    personal_query_ready = _patch_personal_stats_query(namespace)
    suggestions_ready = _patch_player_suggestion_route(namespace)
    return lists_ready and personal_query_ready and suggestions_ready


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
    """Patch app.py selectors, personal queries, and profile suggestions.

    The web module keeps its selector lists and personal-stat query in app.py.
    A small import wrapper patches future ``import app`` calls after the module
    has finished loading. When this package is first imported from inside app.py
    itself, a one-shot trace waits until all targets have been defined.
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
