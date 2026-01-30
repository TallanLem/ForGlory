
import os, re, sqlite3, logging
from datetime import datetime
from flask import Flask, render_template, request, jsonify, g, send_from_directory
try:
	from flask_compress import Compress
except Exception:
	Compress = None

app = Flask(__name__)

if Compress:
	Compress(app)

@app.template_global()
def extract_datetime_from_filename(filename: str) -> datetime:
	m = re.search(r'(\d{4}-\d{2}-\d{2})(?:[_T](\d{2})-(\d{2})-(\d{2}))?', filename)
	if not m:
		return datetime.min

	day = m.group(1)
	hh, mm, ss = m.group(2), m.group(3), m.group(4)

	if hh and mm and ss:
		return datetime.strptime(f"{day} {hh}:{mm}:{ss}", "%Y-%m-%d %H:%M:%S")
	return datetime.strptime(day, "%Y-%m-%d")

# allow enumerate(...) in Jinja templates
app.jinja_env.globals.update(enumerate=enumerate)

logging.basicConfig(level=logging.INFO)

# -----------------------------
# Config
# -----------------------------
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DB_PATH = os.environ.get("DB_PATH", os.path.join(DATA_DIR, "db", "ratings.sqlite"))

MAX_BEST_PER_LIST = int(os.environ.get("MAX_BEST_PER_LIST", "1000"))
LEVEL_PAGE_SIZE = int(os.environ.get("LEVEL_PAGE_SIZE", "100"))

# Snapshot filename format: heroes_YYYY-MM-DD_HH-MM-SS.json(.gz)
DATETIME_RE = re.compile(r"heroes_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})")

stat_keys = ["Сила", "Защита", "Ловкость", "Мастерство", "Живучесть"]

param_options = [
	"Слава", "Побед", "Поражений", "Побед над Драконом", "Побед над Змеем", "Убито зверей",
	"По уровню", "Сила", "Защита", "Ловкость", "Мастерство", "Живучесть", "Сумма статов",
	"Награбил (серебро)", "Потерял (серебро)",
	"Награбил (кристаллы)", "Потерял (кристаллы)",
	"Братства по славе", "Братства по статам", "Кланы по славе", "Кланы по статам"
]

PARAM_EXCLUDE_BY_MODE = {
	"Прирост": {
		"По уровню",
		"Кланы по славе",
		"Кланы по статам",
		"Братства по славе",
		"Братства по статам",
	},
	"Лучшие (приросты)": {
		"По уровню",
		"Кланы по славе",
		"Кланы по статам",
		"Братства по славе",
		"Братства по статам",
	},
}

def params_for_mode(mode: str, all_params: list[str]) -> list[str]:
	banned = PARAM_EXCLUDE_BY_MODE.get(mode, set())
	return [p for p in all_params if p not in banned]

# -----------------------------
# DB helpers
# -----------------------------
def _db_available() -> bool:
	return os.path.exists(DB_PATH)

def get_db() -> sqlite3.Connection:
	if "db" not in g:
		conn = sqlite3.connect(DB_PATH, check_same_thread=False)
		conn.row_factory = sqlite3.Row
		g.db = conn
	return g.db

@app.teardown_appcontext
def close_db(_exc):
	conn = g.pop("db", None)
	if conn is not None:
		conn.close()

def _dt_from_filename(name: str):
	m = DATETIME_RE.search(name or "")
	if not m:
		return None
	date_s = m.group(1)
	time_s = m.group(2).replace("-", ":")
	try:
		return datetime.strptime(f"{date_s} {time_s}", "%Y-%m-%d %H:%M:%S")
	except ValueError:
		return None

#~ @app.template_filter("extract_datetime_from_filename")
#~ def extract_datetime_from_filename(name: str):
	#~ dt = _dt_from_filename(name)
	#~ return dt or datetime(1970, 1, 1)

def list_snapshot_ids():
	"""Return snapshot ids (filenames) sorted newest->oldest from DB."""
	db = get_db()
	rows = db.execute("SELECT id FROM snapshots ORDER BY ts DESC").fetchall()
	return [r["id"] for r in rows]

def prev_snapshot_id(current_id: str, ids_desc):
	"""Given current snapshot id and list sorted desc, return previous (older) id."""
	try:
		i = ids_desc.index(current_id)
		return ids_desc[i+1] if i+1 < len(ids_desc) else None
	except ValueError:
		return None

def all_levels_for_snapshot(snapshot_id: str):
	db = get_db()
	rows = db.execute(
		"SELECT DISTINCT level FROM heroes WHERE snapshot_id=? AND level IS NOT NULL ORDER BY level",
		(snapshot_id,)
	).fetchall()
	return [r["level"] for r in rows]

# -----------------------------
# Query builders
# -----------------------------
def _player_value_expr(param: str):
	# Returns SQL expression (string) for value.
	if param == "Сумма статов":
		return "(strength + defense + dexterity + mastery + vitality)"
	mapping = {
		"Слава": "glory",
		"Побед": "wins",
		"Поражений": "losses",
		"Побед над Драконом": "dragon_wins",
		"Побед над Змеем": "snake_wins",
		"Убито зверей": "beasts_killed",
		"Сила": "strength",
		"Защита": "defense",
		"Ловкость": "dexterity",
		"Мастерство": "mastery",
		"Живучесть": "vitality",
		"Награбил (серебро)": "rob_silver",
		"Потерял (серебро)": "lost_silver",
		"Награбил (кристаллы)": "rob_crystals",
		"Потерял (кристаллы)": "lost_crystals",
	}
	return mapping.get(param, "glory")

def query_rating_overall(snapshot_id: str, prev_id: str | None, param: str, level: int | None):
	db = get_db()
	val = _player_value_expr(param)

	where = "h.snapshot_id=?"
	args = [snapshot_id]
	if level is not None:
		where += " AND h.level=?"
		args.append(level)

	if prev_id:
		sql = f"""
		SELECT
			h.pid AS pid,
			h.name AS name,
			h.level AS level,
			{val} AS value,
			({val} - COALESCE(p.{val.split()[-1] if val.isidentifier() else '0'}, 0)) AS delta
		FROM heroes h
		LEFT JOIN heroes p
			ON p.snapshot_id=? AND p.pid=h.pid
		WHERE {where}
		ORDER BY value DESC
		LIMIT ?
		"""
		# delta expr above is tricky for computed values. We'll do a safer approach below in python
		"""
		"""
	# We will handle delta safely by selecting prev value separately
	if prev_id:
		sql = f"""
		SELECT
			h.pid AS pid,
			h.name AS name,
			h.level AS level,
			{val} AS value,
			({val} - COALESCE(pv.prev_value, 0)) AS delta
		FROM heroes h
		LEFT JOIN (
			SELECT pid, {val} AS prev_value
			FROM heroes
			WHERE snapshot_id=?
		) pv ON pv.pid=h.pid
		WHERE {where}
		ORDER BY value DESC
		LIMIT ?
		"""
		args2 = [prev_id] + args + [MAX_BEST_PER_LIST]
		rows = db.execute(sql, args2).fetchall()
	else:
		sql = f"""
		SELECT h.pid AS pid, h.name AS name, h.level AS level, {val} AS value, NULL AS delta
		FROM heroes h
		WHERE {where}
		ORDER BY value DESC
		LIMIT ?
		"""
		args2 = args + [MAX_BEST_PER_LIST]
		rows = db.execute(sql, args2).fetchall()

	rating = [(r["pid"], r["name"], r["level"], int(r["value"] or 0), (int(r["delta"]) if r["delta"] is not None else None)) for r in rows]
	return rating


def query_growth_between(snap_from: str, snap_to: str, param: str, level: int | None):
	"""
	Returns list of tuples: (pid, name, level, diff, extra)
	diff = value(to) - value(from)
	extra = per-fight average for robbed/lost params (same idea as old code)
	"""
	db = get_db()
	val = _player_value_expr(param)

	# Bring previous snapshot values + wins/losses (for "extra")
	sub_prev = f"""
		SELECT pid,
			   {val} AS prev_value,
			   wins AS prev_wins,
			   losses AS prev_losses
		FROM heroes
		WHERE snapshot_id=?
	"""

	diff_expr = f"({val} - COALESCE(pv.prev_value, 0))"

	extra_expr = "NULL"
	if param.startswith("Награбил"):
		extra_expr = f"CASE WHEN (c.wins - COALESCE(pv.prev_wins, 0)) > 0 THEN ROUND({diff_expr} * 1.0 / (c.wins - COALESCE(pv.prev_wins, 0))) ELSE NULL END"
	elif param.startswith("Потерял"):
		extra_expr = f"CASE WHEN (c.losses - COALESCE(pv.prev_losses, 0)) > 0 THEN ROUND({diff_expr} * 1.0 / (c.losses - COALESCE(pv.prev_losses, 0))) ELSE NULL END"

	where = "c.snapshot_id=?"
	args = [snap_to]
	if level is not None:
		where += " AND c.level=?"
		args.append(level)

	sql = f"""
	SELECT
		c.pid AS pid,
		c.name AS name,
		c.level AS level,
		{diff_expr} AS diff,
		{extra_expr} AS extra
	FROM heroes c
	LEFT JOIN ({sub_prev}) pv ON pv.pid=c.pid
	WHERE {where}
	ORDER BY diff DESC
	LIMIT ?
	"""

	rows = db.execute(sql, [snap_from] + args + [MAX_BEST_PER_LIST]).fetchall()
	rating = [
		(r["pid"], r["name"], r["level"], int(r["diff"] or 0), (int(r["extra"]) if r["extra"] is not None else None))
		for r in rows
	]
	return rating

def query_group_overall(snapshot_id: str, prev_id: str | None, group_kind: str, score_param: str, level: int | None):
	"""
	group_kind: "Клан" or "Братство"
	score_param: "Слава" or "Сумма статов"
	Returns list of dicts compatible with existing template:
	  {name, score, delta, count, count_delta, members:[{pid,name,level,value,delta,_rank}]}
	"""
	db = get_db()
	if group_kind == "Клан":
		gid_col, gname_col = "clan_id", "clan"
	else:
		gid_col, gname_col = "brotherhood_id", "brotherhood"

	val = _player_value_expr(score_param)

	where = f"snapshot_id=? AND {gid_col} IS NOT NULL AND {gid_col} != 0 AND TRIM(COALESCE({gname_col},'')) != ''"
	args = [snapshot_id]
	if level is not None:
		where += " AND level=?"
		args.append(level)

	cur_rows = db.execute(
		f"SELECT pid, name, level, {val} AS value, {gid_col} AS gid, {gname_col} AS gname FROM heroes WHERE {where}",
		args
	).fetchall()

	prev_groups = {}
	prev_value_by_pid = {}
	if prev_id:
		args_p = [prev_id]
		where_p = f"snapshot_id=? AND {gid_col} IS NOT NULL AND {gid_col} != 0 AND TRIM(COALESCE({gname_col},'')) != ''"
		if level is not None:
			where_p += " AND level=?"
			args_p.append(level)
		prev_rows = db.execute(
			f"SELECT pid, name, level, {val} AS value, {gid_col} AS gid, {gname_col} AS gname FROM heroes WHERE {where_p}",
			args_p
		).fetchall()
		for r in prev_rows:
			pid = r["pid"]
			prev_value_by_pid[pid] = int(r["value"] or 0)
			gid = r["gid"]
			gname = (r["gname"] or "").strip()
			if gid not in prev_groups:
				prev_groups[gid] = {"name": gname, "score": 0, "count": 0}
			prev_groups[gid]["score"] += int(r["value"] or 0)
			prev_groups[gid]["count"] += 1

	# build current groups + members
	groups = {}
	for r in cur_rows:
		gid = r["gid"]
		gname = (r["gname"] or "").strip()
		if gid not in groups:
			groups[gid] = {"name": gname, "score": 0, "members": []}
		v = int(r["value"] or 0)
		groups[gid]["score"] += v
		groups[gid]["members"].append({
			"pid": r["pid"],
			"name": r["name"],
			"level": r["level"],
			"value": v,
			"delta": (v - prev_value_by_pid[r["pid"]]) if (prev_id and r["pid"] in prev_value_by_pid) else None
		})

	# merge with previous groups for delta/count_delta
	out = []
	all_gids = set(groups.keys()) | set(prev_groups.keys())
	for gid in all_gids:
		cur = groups.get(gid, {"name": prev_groups.get(gid, {}).get("name", str(gid)), "score": 0, "members": []})
		prev = prev_groups.get(gid, {"score": 0, "count": 0})
		members = cur.get("members", [])
		members.sort(key=lambda h: h.get("value", 0), reverse=True)
		for i, h in enumerate(members, 1):
			h["_rank"] = i
		out.append({
			"name": cur.get("name") or prev.get("name") or str(gid),
			"score": int(cur.get("score", 0)),
			"delta": int(cur.get("score", 0)) - int(prev.get("score", 0)),
			"count": len(members),
			"count_delta": len(members) - int(prev.get("count", 0)),
			"members": members
		})

	out.sort(key=lambda g: g.get("score", 0), reverse=True)
	return out

def query_level_summaries(snapshot_id: str, prev_id: str | None):
	db = get_db()
	cur = db.execute(
		"SELECT level, COUNT(*) AS cnt FROM heroes WHERE snapshot_id=? AND level IS NOT NULL GROUP BY level ORDER BY level DESC",
		(snapshot_id,)
	).fetchall()
	prev_map = {}
	if prev_id:
		prev_rows = db.execute(
			"SELECT level, COUNT(*) AS cnt FROM heroes WHERE snapshot_id=? AND level IS NOT NULL GROUP BY level",
			(prev_id,)
		).fetchall()
		prev_map = {r["level"]: r["cnt"] for r in prev_rows}

	groups = []
	total = 0
	total_prev = 0
	for r in cur:
		lvl = r["level"]
		cnt = r["cnt"]
		total += cnt
		total_prev += prev_map.get(lvl, 0)
		groups.append({
			"level": lvl,
			"count": cnt,
			"count_delta": cnt - prev_map.get(lvl, 0),
		})
	totals = {"count": total, "prev_count": total_prev, "delta": total - total_prev}
	return groups, totals

BALANCE_STATS = ("strength", "defense", "dexterity", "mastery", "vitality")

def query_level_balance(snapshot_id: str):
	db = get_db()

	avg_rows = db.execute(
		"""
		SELECT
			level,
			COUNT(*) AS cnt,
			AVG(strength)  AS strength,
			AVG(defense)   AS defense,
			AVG(dexterity) AS dexterity,
			AVG(mastery)   AS mastery,
			AVG(vitality)  AS vitality
		FROM heroes
		WHERE snapshot_id=? AND level IS NOT NULL
		GROUP BY level
		""",
		(snapshot_id,)
	).fetchall()

	max_rows = db.execute(
		"""
		SELECT
			level,
			MAX(strength)  AS strength,
			MAX(defense)   AS defense,
			MAX(dexterity) AS dexterity,
			MAX(mastery)   AS mastery,
			MAX(vitality)  AS vitality
		FROM heroes
		WHERE snapshot_id=? AND level IS NOT NULL
		GROUP BY level
		""",
		(snapshot_id,)
	).fetchall()

	avg_map = {r["level"]: dict(r) for r in avg_rows}
	max_map = {r["level"]: dict(r) for r in max_rows}

	balance_map = {}
	for lvl, cur_avg in avg_map.items():
		cnt = int(cur_avg.get("cnt") or 0)
		below = lvl - 1

		below_avg = avg_map.get(below)
		cur_max = max_map.get(lvl)

		# Если нет уровня ниже или нет max на текущем — цифры не посчитать
		if not below_avg or not cur_max:
			balance_map[lvl] = {"eligible": cnt >= 20, "count": cnt, "stats": None}
			continue

		stats = {}
		for s in BALANCE_STATS:
			base = float(below_avg.get(s) or 0.0)
			upper15 = base * 1.15
			cap75 = float(cur_max.get(s) or 0.0) * 0.75

			# Сразу округляем до целых для вывода (как ты хочешь)
			upper15_i = int(round(upper15))
			cap75_i   = int(round(cap75))
			best_i    = min(upper15_i, cap75_i)

			stats[s] = {
				"upper15": upper15_i,
				"cap75": cap75_i,
				"best": best_i,
			}

		balance_map[lvl] = {"eligible": cnt >= 20, "count": cnt, "stats": stats}

	return balance_map

def query_level_players(snapshot_id: str, level: int, limit: int, offset: int):
	db = get_db()
	rows = db.execute(
		"""
		SELECT pid, name, strength, defense, dexterity, mastery, vitality
		FROM heroes
		WHERE snapshot_id=? AND level=?
		ORDER BY
			strength DESC,
			defense DESC,
			dexterity DESC,
			mastery DESC,
			vitality DESC,
			pid ASC
		LIMIT ? OFFSET ?
		""",
		(snapshot_id, level, limit, offset)
	).fetchall()
	return [dict(r) for r in rows]

def query_best30(param: str, level: int | None):
	db = get_db()
	latest = db.execute("SELECT id FROM snapshots ORDER BY ts DESC LIMIT 1").fetchone()
	if not latest:
		return []
	best_for = latest["id"]

	sql = """
		SELECT pid, name, level, diff, best_snapshot_id
		FROM best30
		WHERE best_for_snapshot_id=? AND param=?
	"""
	args = [best_for, param]

	if level is not None:
		sql += " AND level=?"
		args.append(level)

	sql += " ORDER BY diff DESC LIMIT ?"
	args.append(MAX_BEST_PER_LIST)

	rows = db.execute(sql, args).fetchall()
	return [(r["pid"], r["name"], r["level"], int(r["diff"] or 0), r["best_snapshot_id"]) for r in rows]


# -----------------------------
# Routes
# -----------------------------
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
	page_size = request.args.get("page_size", default=LEVEL_PAGE_SIZE, type=int)

	if not snapshot_id or level is None or page < 1:
		return jsonify({"error": "bad_request"}), 400

	offset = (page - 1) * page_size
	players = query_level_players(snapshot_id, level, limit=page_size + 1, offset=offset)
	has_more = len(players) > page_size
	players = players[:page_size]

	# render rows HTML via partial template
	rows_html = render_template("level_players_rows.html", players=players, start_index=offset)

	return jsonify({
		"rows_html": rows_html,
		"next_page": page + 1,
		"has_more": has_more,
	})

@app.route("/", methods=["GET", "POST"])
def index():
	mode = request.form.get("mode", "Общий")
	selected_param = request.form.get("param", "Слава")
	selected_level = request.form.get("level", "Все")

	json_files = list_snapshot_ids() if _db_available() else []
	file_selected = json_files[0] if json_files else None
	file1 = None
	file2 = None

	all_params = param_options[:]
	param_selectable = params_for_mode(mode, all_params)


	if not json_files:
		return render_template("index.html",
			mode=mode,
			param=selected_param,
			selected_param=selected_param,
			param_selectable=param_selectable,
			param_options=param_selectable,
			json_files=[],
			selected_file=None,
			file=None, file1=None, file2=None,
			selected_level=selected_level,
			all_levels=[],
			rating=[],
			level_ratings=[],
			totals={"count": 0, "delta": 0},
			best_by_param=[],
			column2_name="Игрок",
			column3_name=selected_param,
			diff_hours=None,
		)


	rating = []
	level_ratings = []
	totals = {"count": 0, "delta": 0}
	best_by_param = []

	if selected_param == "По уровню":
		if mode in ("Прирост", "Лучшие (приросты)"):
			selected_param = "Слава"

	if (selected_param.startswith("Кланы") or selected_param.startswith("Братства")) and mode == "Прирост":
		selected_param = "Слава"

	ctx = {
		# общие
		"mode": mode,
		"param": selected_param,
		"selected_param": selected_param,
		"param_selectable": param_selectable,
		"param_options": param_selectable,
		"json_files": json_files,

		# выбранные файлы/даты
		"selected_file": file_selected,
		"file": file_selected,
		"file1": file1,
		"file2": file2,

		# фильтры
		"selected_level": selected_level,
		"all_levels": [],

		# результаты
		"rating": [],
		"level_ratings": [],
		"totals": {"count": 0, "delta": 0},
		"best_by_param": [],

		# подписи колонок/прочее
		"column2_name": "Игрок",
		"column3_name": selected_param,
		"diff_hours": None,
	}

	level_int = None
	if selected_level and selected_level != "Все":
		try:
			level_int = int(selected_level)
		except Exception:
			level_int = None

	# Column titles (used in template)
	column2_name = "Игрок"
	column3_name = selected_param
	if selected_param == "По уровню":
		column2_name = "Уровень"
		column3_name = "Сумма статов"
	elif selected_param.startswith("Кланы"):
		column2_name = "Клан"
		column3_name = "Слава" if "славе" in selected_param else "Сумма статов"
	elif selected_param.startswith("Братства"):
		column2_name = "Братство"
		column3_name = "Слава" if "славе" in selected_param else "Сумма статов"

	ctx["column2_name"] = column2_name
	ctx["column3_name"] = column3_name

	if mode == "Общий":
		file_selected = request.form.get("file", file_selected)
		prev_id = prev_snapshot_id(file_selected, json_files)

		if selected_param == "По уровню":
			level_ratings, totals = query_level_summaries(file_selected, prev_id)
			all_levels = all_levels_for_snapshot(file_selected)

			ctx["balance_map"] = query_level_balance(file_selected)
			ctx["level_ratings"] = level_ratings
			ctx["totals"] = totals
			ctx["rating"] = []

		elif selected_param.startswith("Кланы") or selected_param.startswith("Братства"):
			if selected_param.startswith("Кланы"):
				group_kind = "Клан"
			else:
				group_kind = "Братство"
			score_param = "Слава" if "славе" in selected_param else "Сумма статов"
			rating = query_group_overall(file_selected, prev_id, group_kind=group_kind, score_param=score_param, level=level_int)
			all_levels = all_levels_for_snapshot(file_selected)
			ctx["rating"] = rating
		else:
			rating = query_rating_overall(file_selected, prev_id, selected_param, level_int)
			all_levels = all_levels_for_snapshot(file_selected)
			ctx["rating"] = rating

		ctx["all_levels"] = all_levels
		ctx["file"] = file_selected
		ctx["selected_file"] = file_selected


	if mode == "Прирост":
		# UI uses file1, file2
		file1 = request.form.get("file1", json_files[1] if len(json_files) > 1 else json_files[0])
		file2 = request.form.get("file2", json_files[0])

		# ensure file2 is newer than file1? keep as user selected
		rating = query_growth_between(file1, file2, selected_param, level_int)
		all_levels = all_levels_for_snapshot(file2)

		ctx["rating"] = rating
		ctx["all_levels"] = all_levels
		ctx["file1"] = file1
		ctx["file2"] = file2


	if mode == "Лучшие (приросты)":
		# build blocks for each param on demand (keep template-compatible)
		best_rating = query_best30(selected_param, level_int)
		best_by_param = [{"param": selected_param, "rating": best_rating}]
		all_levels = all_levels_for_snapshot(json_files[0])

		ctx["best_by_param"] = best_by_param
		ctx["all_levels"] = all_levels


	# fallback
	return render_template("index.html", **ctx)


if __name__ == "__main__":
	app.config["TEMPLATES_AUTO_RELOAD"] = True
	app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
	app.jinja_env.auto_reload = True
	app.run(debug=False, use_reloader=False)
