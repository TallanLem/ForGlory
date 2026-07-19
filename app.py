from __future__ import annotations

import logging
import math
import os
import re
import sqlite3
from datetime import datetime
from urllib.parse import urlencode

from flask import Flask, g, jsonify, render_template, request, send_from_directory, url_for

try:
    from flask_compress import Compress
except Exception:
    Compress = None

from forglory.schema import PARAM_TO_COLUMN, STAT_COLUMNS

app = Flask(__name__)
if Compress:
    Compress(app)

app.jinja_env.globals.update(enumerate=enumerate)
logging.basicConfig(level=logging.INFO)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DB_PATH = os.environ.get("DB_PATH", os.path.join(DATA_DIR, "db", "ratings.sqlite"))
PAGE_SIZE = max(10, min(500, int(os.environ.get("PAGE_SIZE", "100"))))
LEVEL_PAGE_SIZE = max(10, min(500, int(os.environ.get("LEVEL_PAGE_SIZE", "100"))))
DATETIME_RE = re.compile(r"heroes_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})")

stat_keys = ["Сила", "Защита", "Ловкость", "Мастерство", "Живучесть"]
param_options = [
    "Слава", "Побед", "Поражений", "Побед над Драконом", "Побед над Змеем", "Убито зверей",
    "По уровню", "Сила", "Защита", "Ловкость", "Мастерство", "Живучесть", "Сумма статов",
    "Награбил (серебро)", "Потерял (серебро)",
    "Награбил (кристаллы)", "Потерял (кристаллы)",
    "Братства по славе", "Братства по статам", "Кланы по славе", "Кланы по статам",
]
PARAM_EXCLUDE_BY_MODE = {
    "Прирост": {"По уровню", "Кланы по славе", "Кланы по статам", "Братства по славе", "Братства по статам"},
    "Лучшие (приросты)": {"По уровню", "Кланы по славе", "Кланы по статам", "Братства по славе", "Братства по статам"},
}


@app.template_global()
def extract_datetime_from_filename(filename: str) -> datetime:
    match = DATETIME_RE.search(filename or "")
    if not match:
        return datetime.min
    return datetime.strptime(
        f"{match.group(1)} {match.group(2).replace('-', ':')}", "%Y-%m-%d %H:%M:%S"
    )


def params_for_mode(mode: str, all_params: list[str]) -> list[str]:
    banned = PARAM_EXCLUDE_BY_MODE.get(mode, set())
    return [param for param in all_params if param not in banned]


def _db_available() -> bool:
    return os.path.exists(DB_PATH)


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        uri = f"file:{os.path.abspath(DB_PATH)}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("PRAGMA cache_size=-8192")
        conn.execute("PRAGMA temp_store=FILE")
        conn.execute("PRAGMA mmap_size=0")
        conn.execute("PRAGMA busy_timeout=5000")
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(_exc) -> None:
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


def list_snapshot_ids() -> list[str]:
    rows = get_db().execute("SELECT filename FROM snapshots ORDER BY ts DESC").fetchall()
    return [str(row["filename"]) for row in rows]


def snapshot_ts(filename: str | None) -> int | None:
    if not filename:
        return None
    row = get_db().execute("SELECT ts FROM snapshots WHERE filename=?", (filename,)).fetchone()
    return int(row[0]) if row else None


def prev_snapshot_id(current_id: str, ids_desc: list[str]) -> str | None:
    try:
        index = ids_desc.index(current_id)
    except ValueError:
        return None
    return ids_desc[index + 1] if index + 1 < len(ids_desc) else None


def all_levels_for_snapshot(snapshot_id: str) -> list[int]:
    rows = get_db().execute(
        "SELECT DISTINCT level FROM heroes WHERE snapshot_id=? AND visible=1 "
        "AND level IS NOT NULL ORDER BY level",
        (snapshot_id,),
    ).fetchall()
    return [int(row["level"]) for row in rows]


def _player_value_expr(param: str, alias: str = "h") -> str:
    if param == "Сумма статов":
        return "(" + "+".join(f"{alias}.{column}" for column in STAT_COLUMNS) + ")"
    column = PARAM_TO_COLUMN.get(param) or "glory"
    return f"{alias}.{column}"


def _level_clause(alias: str, level: int | None) -> tuple[str, list[int]]:
    if level is None:
        return "", []
    return f" AND {alias}.level=?", [level]


def query_rating_overall(
    snapshot_id: str,
    prev_id: str | None,
    param: str,
    level: int | None,
    limit: int,
    offset: int,
) -> tuple[list[tuple], int]:
    db = get_db()
    current_value = _player_value_expr(param, "c")
    previous_value = _player_value_expr(param, "p")
    level_sql, level_args = _level_clause("c", level)
    count = int(
        db.execute(
            f"SELECT COUNT(*) FROM heroes c WHERE c.snapshot_id=? AND c.visible=1{level_sql}",
            [snapshot_id, *level_args],
        ).fetchone()[0]
    )
    if prev_id:
        rows = db.execute(
            f"""
            SELECT c.pid,c.name,c.level,{current_value} AS value,
                   CASE WHEN p.pid IS NULL THEN NULL ELSE ({current_value}-{previous_value}) END AS delta
            FROM heroes c
            LEFT JOIN heroes p ON p.snapshot_id=? AND p.pid=c.pid
            WHERE c.snapshot_id=? AND c.visible=1{level_sql}
            ORDER BY value DESC,c.pid ASC
            LIMIT ? OFFSET ?
            """,
            [prev_id, snapshot_id, *level_args, limit, offset],
        ).fetchall()
    else:
        rows = db.execute(
            f"""
            SELECT c.pid,c.name,c.level,{current_value} AS value,NULL AS delta
            FROM heroes c
            WHERE c.snapshot_id=? AND c.visible=1{level_sql}
            ORDER BY value DESC,c.pid ASC
            LIMIT ? OFFSET ?
            """,
            [snapshot_id, *level_args, limit, offset],
        ).fetchall()
    return [
        (
            int(row["pid"]), row["name"], row["level"],
            int(row["value"]) if row["value"] is not None else None,
            int(row["delta"]) if row["delta"] is not None else None,
        )
        for row in rows
    ], count


def query_growth_between(
    snap_from: str,
    snap_to: str,
    param: str,
    level: int | None,
    limit: int,
    offset: int,
) -> tuple[list[tuple], int]:
    db = get_db()
    current_value = _player_value_expr(param, "c")
    previous_value = _player_value_expr(param, "p")
    diff = f"({current_value}-{previous_value})"
    level_sql, level_args = _level_clause("c", level)
    common_where = (
        f"c.snapshot_id=? AND p.snapshot_id=? AND c.visible=1{level_sql} "
        f"AND {current_value} IS NOT NULL AND {previous_value} IS NOT NULL"
    )
    count = int(
        db.execute(
            f"SELECT COUNT(*) FROM heroes c JOIN heroes p ON p.pid=c.pid WHERE {common_where}",
            [snap_to, snap_from, *level_args],
        ).fetchone()[0]
    )
    extra = "NULL"
    if param.startswith("Награбил"):
        extra = f"CASE WHEN (c.wins-p.wins)>0 THEN ROUND(({diff})*1.0/(c.wins-p.wins)) END"
    elif param.startswith("Потерял"):
        extra = f"CASE WHEN (c.losses-p.losses)>0 THEN ROUND(({diff})*1.0/(c.losses-p.losses)) END"
    rows = db.execute(
        f"""
        SELECT c.pid,c.name,c.level,{diff} AS diff,{extra} AS extra
        FROM heroes c
        JOIN heroes p ON p.pid=c.pid
        WHERE {common_where}
        ORDER BY diff DESC,c.pid ASC
        LIMIT ? OFFSET ?
        """,
        [snap_to, snap_from, *level_args, limit, offset],
    ).fetchall()
    return [
        (
            int(row["pid"]), row["name"], row["level"], int(row["diff"]),
            int(row["extra"]) if row["extra"] is not None else None,
        )
        for row in rows
    ], count


def query_group_overall(
    snapshot_id: str,
    prev_id: str | None,
    group_kind: str,
    score_param: str,
    level: int | None,
) -> list[dict]:
    db = get_db()
    gid_col, gname_col = ("clan_id", "clan") if group_kind == "Клан" else ("brotherhood_id", "brotherhood")
    value = _player_value_expr(score_param, "h")
    level_sql, level_args = _level_clause("h", level)
    current_rows = db.execute(
        f"""
        SELECT h.pid,h.name,h.level,{value} AS value,h.{gid_col} AS gid,h.{gname_col} AS gname
        FROM heroes h
        WHERE h.snapshot_id=? AND h.visible=1 AND h.{gid_col} IS NOT NULL AND h.{gid_col}!=0
          AND TRIM(COALESCE(h.{gname_col},''))!=''{level_sql}
        """,
        [snapshot_id, *level_args],
    ).fetchall()

    previous_groups: dict[int, dict] = {}
    previous_values: dict[int, int] = {}
    if prev_id:
        previous_value = _player_value_expr(score_param, "h")
        previous_rows = db.execute(
            f"""
            SELECT h.pid,{previous_value} AS value,h.{gid_col} AS gid,h.{gname_col} AS gname,h.visible
            FROM heroes h
            WHERE h.snapshot_id=? AND h.{gid_col} IS NOT NULL AND h.{gid_col}!=0
              AND TRIM(COALESCE(h.{gname_col},''))!=''{level_sql}
            """,
            [prev_id, *level_args],
        ).fetchall()
        for row in previous_rows:
            if row["value"] is not None:
                previous_values[int(row["pid"])] = int(row["value"])
            if not row["visible"]:
                continue
            gid = int(row["gid"])
            group = previous_groups.setdefault(gid, {"name": row["gname"], "score": 0, "count": 0})
            group["score"] += int(row["value"] or 0)
            group["count"] += 1

    groups: dict[int, dict] = {}
    for row in current_rows:
        gid = int(row["gid"])
        group = groups.setdefault(gid, {"name": row["gname"], "score": 0, "members": []})
        current = int(row["value"] or 0)
        group["score"] += current
        previous = previous_values.get(int(row["pid"]))
        group["members"].append(
            {
                "pid": int(row["pid"]), "name": row["name"], "level": row["level"],
                "value": current, "delta": current - previous if previous is not None else None,
            }
        )

    result = []
    for gid in set(groups) | set(previous_groups):
        current = groups.get(gid, {"name": previous_groups.get(gid, {}).get("name", str(gid)), "score": 0, "members": []})
        previous = previous_groups.get(gid, {"score": 0, "count": 0})
        members = sorted(current["members"], key=lambda item: item["value"], reverse=True)
        for rank, member in enumerate(members, 1):
            member["_rank"] = rank
        result.append(
            {
                "name": current.get("name") or previous.get("name") or str(gid),
                "score": int(current.get("score", 0)),
                "delta": int(current.get("score", 0)) - int(previous.get("score", 0)),
                "count": len(members),
                "count_delta": len(members) - int(previous.get("count", 0)),
                "members": members,
            }
        )
    return sorted(result, key=lambda item: item["score"], reverse=True)


def query_level_summaries(snapshot_id: str, prev_id: str | None) -> tuple[list[dict], dict]:
    db = get_db()
    current_rows = db.execute(
        "SELECT level,COUNT(*) cnt FROM heroes WHERE snapshot_id=? AND visible=1 "
        "AND level IS NOT NULL GROUP BY level",
        (snapshot_id,),
    ).fetchall()
    previous_rows = db.execute(
        "SELECT level,COUNT(*) cnt FROM heroes WHERE snapshot_id=? AND visible=1 "
        "AND level IS NOT NULL GROUP BY level",
        (prev_id,),
    ).fetchall() if prev_id else []
    current = {int(row["level"]): int(row["cnt"]) for row in current_rows}
    previous = {int(row["level"]): int(row["cnt"]) for row in previous_rows}
    groups = [
        {"level": level, "count": current.get(level, 0), "count_delta": current.get(level, 0) - previous.get(level, 0)}
        for level in sorted(set(current) | set(previous), reverse=True)
    ]
    total = sum(current.values())
    total_previous = sum(previous.values())
    return groups, {"count": total, "prev_count": total_previous, "delta": total - total_previous}


BALANCE_STATS = ("strength", "defense", "dexterity", "mastery", "vitality")


def query_level_balance(snapshot_id: str) -> dict[int, dict]:
    db = get_db()
    select_stats = ",".join(f"AVG({column}) AS {column}" for column in BALANCE_STATS)
    max_stats = ",".join(f"MAX({column}) AS {column}" for column in BALANCE_STATS)
    averages = db.execute(
        f"SELECT level,COUNT(*) cnt,{select_stats} FROM heroes WHERE snapshot_id=? AND visible=1 "
        "AND level IS NOT NULL GROUP BY level",
        (snapshot_id,),
    ).fetchall()
    maxima = db.execute(
        f"SELECT level,{max_stats} FROM heroes WHERE snapshot_id=? AND visible=1 "
        "AND level IS NOT NULL GROUP BY level",
        (snapshot_id,),
    ).fetchall()
    average_map = {int(row["level"]): dict(row) for row in averages}
    maximum_map = {int(row["level"]): dict(row) for row in maxima}
    result: dict[int, dict] = {}
    for level, current in average_map.items():
        below = average_map.get(level - 1)
        maximum = maximum_map.get(level)
        if not below or not maximum:
            result[level] = {"eligible": int(current["cnt"] or 0) >= 20, "count": int(current["cnt"] or 0), "stats": None}
            continue
        stats = {}
        for column in BALANCE_STATS:
            upper15 = int(round(float(below.get(column) or 0) * 1.15))
            cap75 = int(round(float(maximum.get(column) or 0) * 0.75))
            stats[column] = {"upper15": upper15, "cap75": cap75, "best": min(upper15, cap75)}
        result[level] = {"eligible": int(current["cnt"] or 0) >= 20, "count": int(current["cnt"] or 0), "stats": stats}
    return result


def query_level_players(snapshot_id: str, level: int, limit: int, offset: int) -> list[dict]:
    rows = get_db().execute(
        """
        SELECT pid,name,strength,defense,dexterity,mastery,vitality
        FROM heroes
        WHERE snapshot_id=? AND visible=1 AND level=?
        ORDER BY strength DESC,defense DESC,dexterity DESC,mastery DESC,vitality DESC,pid
        LIMIT ? OFFSET ?
        """,
        (snapshot_id, level, limit, offset),
    ).fetchall()
    return [dict(row) for row in rows]


def query_best_growth(param: str, level: int | None, limit: int, offset: int) -> tuple[list[tuple], int]:
    db = get_db()
    latest = db.execute("SELECT filename FROM snapshots ORDER BY ts DESC LIMIT 1").fetchone()
    if not latest:
        return [], 0
    level_sql = " AND level=?" if level is not None else ""
    level_args = [level] if level is not None else []
    count = int(
        db.execute(
            f"SELECT COUNT(*) FROM best30 WHERE best_for_snapshot_id=? AND param=?{level_sql}",
            [latest[0], param, *level_args],
        ).fetchone()[0]
    )
    rows = db.execute(
        f"""
        SELECT pid,name,level,diff,best_snapshot_id
        FROM best30
        WHERE best_for_snapshot_id=? AND param=?{level_sql}
        ORDER BY diff DESC,pid ASC
        LIMIT ? OFFSET ?
        """,
        [latest[0], param, *level_args, limit, offset],
    ).fetchall()
    return [
        (int(row["pid"]), row["name"], row["level"], int(row["diff"]), row["best_snapshot_id"])
        for row in rows
    ], count


def _pagination(page: int, total: int, base_args: dict[str, str]) -> dict:
    total_pages = max(1, math.ceil(total / PAGE_SIZE)) if total else 1
    page = min(max(page, 1), total_pages)
    def make_url(target: int) -> str:
        args = dict(base_args)
        args["page"] = str(target)
        return url_for("index") + "?" + urlencode(args)
    return {
        "page": page,
        "total_pages": total_pages,
        "total_rows": total,
        "prev_url": make_url(page - 1) if page > 1 else None,
        "next_url": make_url(page + 1) if page < total_pages else None,
    }


def _search_ranked_overall(snapshot: str, prev: str | None, param: str, level: int | None, query: str) -> list[sqlite3.Row]:
    current = _player_value_expr(param, "c")
    previous = _player_value_expr(param, "p")
    level_sql, level_args = _level_clause("c", level)
    delta = f"CASE WHEN p.pid IS NULL THEN NULL ELSE ({current}-{previous}) END" if prev else "NULL"
    join = "LEFT JOIN heroes p ON p.snapshot_id=? AND p.pid=c.pid" if prev else ""
    args = [prev] if prev else []
    args.extend([snapshot, *level_args, f"%{query.casefold()}%"])
    return get_db().execute(
        f"""
        WITH ranked AS (
            SELECT c.pid,c.name,c.name_norm,c.level,{current} AS value,{delta} AS extra,
                   ROW_NUMBER() OVER(ORDER BY {current} DESC,c.pid ASC) AS rank
            FROM heroes c {join}
            WHERE c.snapshot_id=? AND c.visible=1{level_sql}
        )
        SELECT * FROM ranked WHERE name_norm LIKE ?
        ORDER BY CASE WHEN name_norm=? THEN 0 ELSE 1 END,rank LIMIT 20
        """,
        [*args, query.casefold()],
    ).fetchall()


def _search_ranked_growth(snap_from: str, snap_to: str, param: str, level: int | None, query: str) -> list[sqlite3.Row]:
    current = _player_value_expr(param, "c")
    previous = _player_value_expr(param, "p")
    diff = f"({current}-{previous})"
    level_sql, level_args = _level_clause("c", level)
    return get_db().execute(
        f"""
        WITH ranked AS (
            SELECT c.pid,c.name,c.name_norm,c.level,{diff} AS value,NULL AS extra,
                   ROW_NUMBER() OVER(ORDER BY {diff} DESC,c.pid ASC) AS rank
            FROM heroes c JOIN heroes p ON p.pid=c.pid AND p.snapshot_id=?
            WHERE c.snapshot_id=? AND c.visible=1{level_sql}
              AND {current} IS NOT NULL AND {previous} IS NOT NULL
        )
        SELECT * FROM ranked WHERE name_norm LIKE ?
        ORDER BY CASE WHEN name_norm=? THEN 0 ELSE 1 END,rank LIMIT 20
        """,
        [snap_from, snap_to, *level_args, f"%{query.casefold()}%", query.casefold()],
    ).fetchall()


def _search_ranked_best(param: str, level: int | None, query: str) -> list[sqlite3.Row]:
    latest = get_db().execute("SELECT filename FROM snapshots ORDER BY ts DESC LIMIT 1").fetchone()
    if not latest:
        return []
    level_sql = " AND level=?" if level is not None else ""
    level_args = [level] if level is not None else []
    return get_db().execute(
        f"""
        WITH ranked AS (
            SELECT pid,name,name_norm,level,diff AS value,best_snapshot_id AS extra,
                   ROW_NUMBER() OVER(ORDER BY diff DESC,pid ASC) AS rank
            FROM best30
            WHERE best_for_snapshot_id=? AND param=?{level_sql}
        )
        SELECT * FROM ranked WHERE name_norm LIKE ?
        ORDER BY CASE WHEN name_norm=? THEN 0 ELSE 1 END,rank LIMIT 20
        """,
        [latest[0], param, *level_args, f"%{query.casefold()}%", query.casefold()],
    ).fetchall()


@app.route("/robots.txt")
def robots():
    return send_from_directory(app.static_folder, "robots.txt")


@app.route("/api/level_players")
def api_level_players():
    if not _db_available():
        return jsonify({"error": "db_not_available"}), 503
    snapshot_id = request.args.get("snapshot_id")
    level = request.args.get("level", type=int)
    page = request.args.get("page", default=1, type=int)
    page_size = max(10, min(500, request.args.get("page_size", default=LEVEL_PAGE_SIZE, type=int)))
    if not snapshot_id or level is None or page < 1:
        return jsonify({"error": "bad_request"}), 400
    offset = (page - 1) * page_size
    players = query_level_players(snapshot_id, level, page_size + 1, offset)
    has_more = len(players) > page_size
    html = render_template("level_players_rows.html", players=players[:page_size], start_index=offset)
    return jsonify({"rows_html": html, "next_page": page + 1, "has_more": has_more})


@app.route("/api/player_suggest")
def api_player_suggest():
    if not _db_available():
        return jsonify([])
    snapshot = request.args.get("snapshot")
    query = " ".join((request.args.get("q") or "").casefold().split())
    if not snapshot or len(query) < 2:
        return jsonify([])
    rows = get_db().execute(
        "SELECT DISTINCT name FROM heroes WHERE snapshot_id=? AND visible=1 "
        "AND name_norm LIKE ? ORDER BY CASE WHEN name_norm=? THEN 0 ELSE 1 END,name LIMIT 20",
        (snapshot, f"%{query}%", query),
    ).fetchall()
    return jsonify([row["name"] for row in rows])


@app.route("/api/player_search")
def api_player_search():
    if not _db_available():
        return jsonify({"error": "db_not_available"}), 503
    query = " ".join((request.args.get("q") or "").strip().split())
    if not query:
        return jsonify({"error": "empty_query"}), 400
    mode = request.args.get("mode", "Общий")
    param = request.args.get("param", "Слава")
    level_raw = request.args.get("level", "Все")
    level = int(level_raw) if level_raw not in {"", "Все", None} else None
    file = request.args.get("file")
    file1 = request.args.get("file1")
    file2 = request.args.get("file2")

    if mode == "Прирост" and file1 and file2:
        rows = _search_ranked_growth(file1, file2, param, level, query)
    elif mode == "Лучшие (приросты)":
        rows = _search_ranked_best(param, level, query)
    elif file:
        rows = _search_ranked_overall(file, prev_snapshot_id(file, list_snapshot_ids()), param, level, query)
    else:
        return jsonify({"error": "bad_request"}), 400

    results = []
    for row in rows:
        rank = int(row["rank"])
        args = {"mode": mode, "param": param, "level": level_raw, "page": str((rank - 1) // PAGE_SIZE + 1), "highlight_pid": str(row["pid"])}
        if file:
            args["file"] = file
        if file1:
            args["file1"] = file1
        if file2:
            args["file2"] = file2
        results.append(
            {
                "pid": int(row["pid"]), "name": row["name"], "level": row["level"],
                "value": row["value"], "rank": rank,
                "url": url_for("index") + "?" + urlencode(args),
            }
        )
    return jsonify({"results": results})


@app.route("/", methods=["GET", "POST"])
def index():
    values = request.values
    mode = values.get("mode", "Общий")
    selected_param = values.get("param", "Слава")
    selected_level = values.get("level", "Все")
    page = max(1, values.get("page", type=int) or 1)
    highlight_pid = values.get("highlight_pid", type=int)

    snapshots = list_snapshot_ids() if _db_available() else []
    selectable = params_for_mode(mode, param_options)
    if selected_param not in selectable:
        selected_param = selectable[0]

    empty_context = {
        "mode": mode, "param": selected_param, "selected_param": selected_param,
        "param_selectable": selectable, "param_options": selectable,
        "json_files": snapshots, "selected_file": None, "file": None, "file1": None, "file2": None,
        "selected_level": selected_level, "all_levels": [], "rating": [], "level_ratings": [],
        "totals": {"count": 0, "delta": 0}, "best_by_param": [], "column2_name": "Игрок",
        "column3_name": selected_param, "diff_hours": None, "pagination": None,
        "page_start": 0, "highlight_pid": highlight_pid, "page_size": PAGE_SIZE,
    }
    if not snapshots:
        return render_template("index.html", **empty_context)

    level = None
    if selected_level not in {None, "", "Все"}:
        try:
            level = int(selected_level)
        except ValueError:
            selected_level = "Все"

    file = values.get("file", snapshots[0])
    if file not in snapshots:
        file = snapshots[0]
    file1 = values.get("file1", snapshots[1] if len(snapshots) > 1 else snapshots[0])
    file2 = values.get("file2", snapshots[0])
    if file1 not in snapshots:
        file1 = snapshots[1] if len(snapshots) > 1 else snapshots[0]
    if file2 not in snapshots:
        file2 = snapshots[0]

    column2 = "Игрок"
    column3 = selected_param
    if selected_param == "По уровню":
        column2, column3 = "Уровень", "Сумма статов"
    elif selected_param.startswith("Кланы"):
        column2, column3 = "Клан", "Слава" if "славе" in selected_param else "Сумма статов"
    elif selected_param.startswith("Братства"):
        column2, column3 = "Братство", "Слава" if "славе" in selected_param else "Сумма статов"

    context = dict(empty_context)
    context.update(
        selected_file=file, file=file, file1=file1, file2=file2,
        column2_name=column2, column3_name=column3, selected_level=selected_level,
    )

    offset = (page - 1) * PAGE_SIZE
    base_args = {"mode": mode, "param": selected_param, "level": selected_level}

    if mode == "Общий":
        previous = prev_snapshot_id(file, snapshots)
        base_args["file"] = file
        if selected_param == "По уровню":
            groups, totals = query_level_summaries(file, previous)
            context.update(
                level_ratings=groups, totals=totals, balance_map=query_level_balance(file),
                all_levels=all_levels_for_snapshot(file),
            )
        elif selected_param.startswith("Кланы") or selected_param.startswith("Братства"):
            kind = "Клан" if selected_param.startswith("Кланы") else "Братство"
            score = "Слава" if "славе" in selected_param else "Сумма статов"
            context.update(
                rating=query_group_overall(file, previous, kind, score, level),
                all_levels=all_levels_for_snapshot(file),
            )
        else:
            rating, total = query_rating_overall(file, previous, selected_param, level, PAGE_SIZE, offset)
            pagination = _pagination(page, total, base_args)
            if pagination["page"] != page:
                page = pagination["page"]
                offset = (page - 1) * PAGE_SIZE
                rating, total = query_rating_overall(file, previous, selected_param, level, PAGE_SIZE, offset)
            context.update(rating=rating, all_levels=all_levels_for_snapshot(file), pagination=pagination, page_start=offset)
        current_ts, previous_ts = snapshot_ts(file), snapshot_ts(previous)
        if current_ts is not None and previous_ts is not None:
            context["diff_hours"] = round((current_ts - previous_ts) / 3600)

    elif mode == "Прирост":
        base_args.update(file1=file1, file2=file2)
        rating, total = query_growth_between(file1, file2, selected_param, level, PAGE_SIZE, offset)
        pagination = _pagination(page, total, base_args)
        if pagination["page"] != page:
            page = pagination["page"]
            offset = (page - 1) * PAGE_SIZE
            rating, total = query_growth_between(file1, file2, selected_param, level, PAGE_SIZE, offset)
        context.update(rating=rating, all_levels=all_levels_for_snapshot(file2), pagination=pagination, page_start=offset)
        start_ts, end_ts = snapshot_ts(file1), snapshot_ts(file2)
        if start_ts is not None and end_ts is not None:
            context["diff_hours"] = abs(round((end_ts - start_ts) / 3600))

    else:
        rating, total = query_best_growth(selected_param, level, PAGE_SIZE, offset)
        pagination = _pagination(page, total, base_args)
        if pagination["page"] != page:
            page = pagination["page"]
            offset = (page - 1) * PAGE_SIZE
            rating, total = query_best_growth(selected_param, level, PAGE_SIZE, offset)
        context.update(
            best_by_param=[{"param": selected_param, "rating": rating}],
            all_levels=all_levels_for_snapshot(snapshots[0]), pagination=pagination, page_start=offset,
        )

    return render_template("index.html", **context)


if __name__ == "__main__":
    app.config["TEMPLATES_AUTO_RELOAD"] = True
    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
    app.jinja_env.auto_reload = True
    app.run(debug=False, use_reloader=False)
