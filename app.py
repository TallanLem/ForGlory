import logging, os, sys, json, glob, re, gzip
import collections

from pprint import pprint
from flask import Flask, render_template, request
from datetime import datetime, timedelta
from operator import itemgetter
from collections import defaultdict
from functools import lru_cache


app = Flask(__name__)
BEST_CACHE = {}
ALL_LEVELS = set()

logging.basicConfig(level=logging.DEBUG)

JSON_DIR = "data"
BEST_WINDOW_DAYS = int(os.environ.get("BEST_WINDOW_DAYS", "30"))

stat_keys = ["Сила", "Защита", "Ловкость", "Мастерство", "Живучесть"]

param_options = [
	"Слава", "Побед", "Поражений", "Побед над Драконом", "Побед над Змеем", "Убито зверей",
	"По уровню", "Сила", "Защита", "Ловкость", "Мастерство", "Живучесть", "Сумма статов",
	"Награбил (серебро)", "Потерял (серебро)",
	"Награбил (кристаллы)", "Потерял (кристаллы)",
	"Братства по славе", "Братства по статам", "Кланы по славе", "Кланы по статам"
]

def load_json_any(path: str):
	if path.endswith(".gz"):
		with gzip.open(path, "rt", encoding="utf-8") as f:
			return json.load(f)
	with open(path, "r", encoding="utf-8") as f:
		return json.load(f)

@lru_cache(maxsize=4)
def snapshot(filename: str):
	full_path = os.path.join(JSON_DIR, filename)
	return load_json_any(full_path)

def list_json_files():
	files = [f for f in os.listdir(JSON_DIR) if f.startswith("heroes_") and f.endswith((".json", ".json.gz"))]
	return sorted(files, key=extract_datetime_from_filename, reverse=True)


def extract_datetime_from_filename(filename):
	match = re.search(r"heroes_(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})", filename)
	if match:
		dt = datetime.strptime("-".join(match.groups()), "%Y-%m-%d-%H-%M-%S")
		return dt
	return None


def build_rating(data, param, previous_data=None):
	if param == "Кланы по славе":
		return build_group_rating(data, "Клан", "Слава", previous_data)
	elif param == "Братства по славе":
		return build_group_rating(data, "Братство", "Слава", previous_data)
	elif param == "Кланы по статам":
		return build_group_rating(data, "Клан", stat_keys, previous_data)
	elif param == "Братства по статам":
		return build_group_rating(data, "Братство", stat_keys, previous_data)
	elif param == "Сумма статов":
		param = stat_keys

	rating = []
	for pid, hero in data.items():
		if hero.get("Имя", "").lower():
			if isinstance(param, str):
				value = hero.get(param, 0)
			elif isinstance(param, list):
				value = sum(hero.get(p, 0) for p in param)
			else:
				value = 0

			name = hero.get("Имя", "Безымянный")
			level = hero.get("Уровень", "?")
			delta = None
			if previous_data and pid in previous_data:
				if isinstance(param, str):
					value_old = previous_data[pid].get(param, 0)
				elif isinstance(param, list):
					value_old = sum(previous_data[pid].get(p, 0) for p in param)
				else:
					value_old = 0

				if value_old:
					delta = value - value_old
				else:
					delta = 0

			rating.append((pid, name, level, value, delta))

	rating.sort(key=lambda x: x[3], reverse=1)
	rating = rating[:1000]
	return rating


def build_growth_rating(data1, data2, param):
	rating = []
	for pid, hero2 in data2.items():
		hero1 = data1.get(pid, {})
		name = hero2.get("Имя", "Безымянный")
		level = hero2.get("Уровень", "?")
		if isinstance(param, str):
			v2 = hero2.get(param, 0)
			v1 = hero1.get(param, 0)
		elif isinstance(param, list):
			v2 = sum(hero2.get(p, 0) for p in param)
			v1 = sum(hero1.get(p, 0) for p in param)
		else:
			v2 = v1 = 0

		diff = v2 - v1
		extra = None
		if isinstance(param, str):
			if param.startswith("Награбил"):
				victories2 = hero2.get("Побед", 0)
				victories1 = hero1.get("Побед", 0)
				fights = victories2 - victories1
				if fights > 0:
					extra = round(diff / fights)
			elif param.startswith("Потерял"):
				defeats2 = hero2.get("Поражений", 0)
				defeats1 = hero1.get("Поражений", 0)
				fights = defeats2 - defeats1
				if fights > 0:
					extra = round(diff / fights)

		rating.append((pid, name, level, diff, extra))
	rating.sort(key=lambda x: x[3], reverse=1)
	rating = rating[:1000]
	return rating

def get_level_ratings(data):
	from collections import defaultdict
	grouped = defaultdict(list)
	for hero in data.values():
		if "Уровень" in hero and "Сила" in hero:
			grouped[hero["Уровень"]].append(hero)

	for level in grouped:
		grouped[level].sort(
			key=lambda h: (
				h.get("Сила", 0),
				h.get("Защита", 0),
				h.get("Ловкость", 0),
				h.get("Мастерство", 0),
				h.get("Живучесть", 0)
			),
			reverse=True
		)

	sorted_levels = sorted(grouped.keys(), reverse=True)

	result = []
	for level in sorted_levels:
		players = grouped[level]
		n = len(players) or 1
		avg_strength = sum(p.get("Сила", 0) for p in players) / n
		avg_defense  = sum(p.get("Защита", 0) for p in players) / n
		avg_dex      = sum(p.get("Ловкость", 0) for p in players) / n
		avg_mastery  = sum(p.get("Мастерство", 0) for p in players) / n
		avg_vit      = sum(p.get("Живучесть", 0) for p in players) / n

		result.append({
			"level": level,
			"avg": {
				"strength": avg_strength,
				"defense": avg_defense,
				"dexterity": avg_dex,
				"mastery": avg_mastery,
				"vitality": avg_vit,
			},
			"players": [
				{
					"ID": hero.get("ID") or hero.get("id"),
					"name": hero.get("Имя", "Безымянный"),
					"level": level,
					"strength": hero.get("Сила", 0),
					"defense": hero.get("Защита", 0),
					"dexterity": hero.get("Ловкость", 0),
					"mastery": hero.get("Мастерство", 0),
					"vitality": hero.get("Живучесть", 0)
				}
				for hero in players
			]
		})
	return result



def build_group_rating(data, group_key, param, previous_data=None):
	id_key = "clan_id" if group_key == "Клан" else (
		"brotherhood_id" if group_key == "Братство" else None
	)

	def is_id_mode(dataset):
		return any(id_key in h for h in dataset.values())

	use_id_mode = is_id_mode(data)

	def get_value(hero):
		if isinstance(param, str):
			return hero.get(param, 0)
		elif isinstance(param, list):
			return sum(hero.get(p, 0) for p in param)
		return 0

	def build_groups(dataset, id_mode):
		groups = {}
		for hero in dataset.values():
			# 1) определить ключ группы
			if id_mode:
				group_id = hero.get(id_key, 0)
				group_name = (hero.get(group_key, "") or "").strip()
				if not (group_id and group_name):
					continue
				key = group_id
				if key not in groups:
					groups[key] = {"name": group_name, "score": 0, "members": []}
			else:
				group_name = (hero.get(group_key, "") or "").strip()
				if not group_name or "не состоит" in group_name.lower():
					continue
				key = group_name
				if key not in groups:
					groups[key] = {"name": group_name, "score": 0, "members": []}

			# 2) текущее значение параметра
			value = get_value(hero)
			hero["value"] = value  # (как и раньше, не создаём копии)
			groups[key]["score"] += value
			groups[key]["members"].append(hero)

		# 3) сортировка участников и ранги
		for g in groups.values():
			g["members"].sort(key=lambda h: h["value"], reverse=True)
			for i, h in enumerate(g["members"], 1):
				h["_rank"] = i

		return groups

	current = build_groups(data, use_id_mode)
	previous = build_groups(previous_data, use_id_mode) if previous_data else {}

	# --- карта предыдущих значений по pid, чтобы посчитать дельты участников ---
	prev_value_by_pid = {}
	if previous:
		for g in previous.values():
			for h in g.get("members", []):
				pid = h.get("ID") or h.get("id") or h.get("pid")
				if pid is not None:
					# в previous уже посчитан h["value"] тем же get_value
					prev_value_by_pid[pid] = h.get("value", 0)

	# --- собрать итог по группам + проставить delta каждому участнику ---
	all_keys = set(current) | set(previous)
	result = []
	for key in all_keys:
		curr_group = current.get(key, {})
		prev_group = previous.get(key, {})

		name = curr_group.get("name") or prev_group.get("name") or str(key)
		score_now = curr_group.get("score", 0)
		score_prev = prev_group.get("score", 0)

		members_now = curr_group.get("members", [])
		# персональные дельты участников (None, если нет прошлого снэпшота или герой не найден)
		for h in members_now:
			pid = h.get("ID") or h.get("id") or h.get("pid")
			if pid is not None and pid in prev_value_by_pid:
				h["delta"] = h.get("value", 0) - prev_value_by_pid[pid]
			else:
				h["delta"] = None

		count_now = len(members_now)
		count_prev = len(prev_group.get("members", []))

		result.append({
			"name": name,
			"score": score_now,
			"delta": score_now - score_prev,
			"count": count_now,
			"count_delta": count_now - count_prev,
			"members": members_now,
		})

	result.sort(key=itemgetter("score"), reverse=True)
	return result


def _collect_all_levels(preloaded_data):
	levels = set()
	for snap in preloaded_data.values():
		for h in snap.values():
			lvl = h.get("Уровень")
			if isinstance(lvl, int):
				levels.add(lvl)
	return levels

def _filter_by_level(snapshot, level_int_or_none):
	if not level_int_or_none:
		return snapshot
	return {pid: h for pid, h in snapshot.items() if h.get("Уровень") == level_int_or_none}

def _param_key_for_best(selected_param, stat_keys):
	return stat_keys if selected_param == "Сумма статов" else selected_param

def precompute_best_cache(json_files, extract_datetime_from_filename, param_options, stat_keys):
    BEST_CACHE.clear()
    global ALL_LEVELS

    files_sorted = sorted(json_files, key=extract_datetime_from_filename)
    if len(files_sorted) < 2:
        ALL_LEVELS = set()
        return

    latest_dt = extract_datetime_from_filename(files_sorted[-1])
    cutoff_dt = latest_dt - timedelta(days=BEST_WINDOW_DAYS if 'BEST_WINDOW_DAYS' in globals() else 30)
    window_files = [f for f in files_sorted if extract_datetime_from_filename(f) >= cutoff_dt]
    if len(window_files) < 2:
        window_files = files_sorted

    levels = set()
    for f in window_files:
        snap = snapshot(f)
        for h in snap.values():
            lvl = h.get("Уровень")
            if isinstance(lvl, int):
                levels.add(lvl)
    ALL_LEVELS = levels

    def _param_key_for_best(p):
        return stat_keys if p == "Сумма статов" else p

    best_params = [
        p for p in param_options
        if p != "По уровню" and not p.startswith("Кланы") and not p.startswith("Братства")
    ]

    for param in best_params:
        pkey = _param_key_for_best(param)

        best_all = {}
        best_by_level = defaultdict(dict)

        for f_prev, f_curr in zip(window_files[:-1], window_files[1:]):
            data1 = snapshot(f_prev)
            data2 = snapshot(f_curr)

            if pkey == stat_keys:
                for pid, h2 in data2.items():
                    h1 = data1.get(pid)
                    if not h1:
                        continue
                    v2 = (h2.get("Сила", 0) + h2.get("Защита", 0) + h2.get("Ловкость", 0) +
                          h2.get("Мастерство", 0) + h2.get("Живучесть", 0))
                    v1 = (h1.get("Сила", 0) + h1.get("Защита", 0) + h1.get("Ловкость", 0) +
                          h1.get("Мастерство", 0) + h1.get("Живучесть", 0))
                    diff = v2 - v1
                    if diff <= 0:
                        continue
                    name = h2.get("Имя")
                    lvl = h2.get("Уровень")
                    tup = (pid, name, lvl, diff, None)

                    cur = best_all.get(pid)
                    if (cur is None) or (diff > cur[3]):
                        best_all[pid] = tup
                    if isinstance(lvl, int):
                        curL = best_by_level[lvl].get(pid)
                        if (curL is None) or (diff > curL[3]):
                            best_by_level[lvl][pid] = tup
            else:
                key = pkey
                for pid, h2 in data2.items():
                    h1 = data1.get(pid)
                    if not h1:
                        continue
                    v2 = h2.get(key, 0)
                    v1 = h1.get(key, 0)
                    diff = v2 - v1
                    if diff <= 0:
                        continue
                    name = h2.get("Имя")
                    lvl = h2.get("Уровень")
                    tup = (pid, name, lvl, diff, None)

                    cur = best_all.get(pid)
                    if (cur is None) or (diff > cur[3]):
                        best_all[pid] = tup
                    if isinstance(lvl, int):
                        curL = best_by_level[lvl].get(pid)
                        if (curL is None) or (diff > curL[3]):
                            best_by_level[lvl][pid] = tup

        merged_all = sorted(best_all.values(), key=lambda t: t[3], reverse=True)[:1000]
        BEST_CACHE[(param, None)] = merged_all

        for lvl in sorted(ALL_LEVELS):
            vals = best_by_level.get(lvl)
            if not vals:
                BEST_CACHE[(param, lvl)] = []
                continue
            BEST_CACHE[(param, lvl)] = sorted(vals.values(), key=lambda t: t[3], reverse=True)[:1000]

    # snapshot.cache_clear()  # опционально: можно очистить LRU после предрасчёта



json_files = list_json_files()
for f in json_files[:3]:
	try:
		snapshot(f)
	except Exception as e:
		app.logger.warning(f"Warmup failed for {f}: {e}")

print(3)
precompute_best_cache(json_files, extract_datetime_from_filename, param_options, stat_keys)
print(4)

@app.route("/", methods=["GET", "POST"])
def index():
	global json_files
	selected_param = "Слава"
	selected_file = json_files[0] if json_files else None
	file1 = file2 = None
	rating = []
	level_ratings = []
	best_by_param = []
	filename_display = ""
	diff_hours = None
	selected_level = request.form.get("level", "Все")
	files_display = [(f, extract_datetime_from_filename(f).strftime("%d.%m.%Y %H:%M")) for f in json_files]

	mode = request.form.get("mode") or "Общий"

	selected_param = request.form.get("param", selected_param)

	column2_name = "Игрок"
	column3_name = selected_param
	if selected_param == "Братства по славе":
		if mode == "Прирост":
			selected_param = 'Слава'
		column2_name = "Братство"
		column3_name = "Слава"
	elif selected_param == "Кланы по славе":
		if mode == "Прирост":
			selected_param = 'Слава'
		column2_name = "Клан"
		column3_name = "Слава"
	elif selected_param == "Кланы по статам":
		if mode == "Прирост":
			selected_param = 'Слава'
		column2_name = "Клан"
		column3_name = "Сумма статов"
	elif selected_param == "По уровню":
		if mode == "Прирост" or mode == "Лучшие (приросты)":
			selected_param = 'Слава'

	if mode == "Общий":
		selected_file = request.form.get("file", selected_file)

		data = snapshot(selected_file) if selected_file else {}

		file_index = json_files.index(selected_file)
		prev_name = json_files[file_index + 1] if file_index + 1 < len(json_files) else None
		prev_snap = snapshot(prev_name) if prev_name else None

		if selected_param == "По уровню":
			rating = []
			level_ratings = get_level_ratings(data)
			try:
				files_sorted = sorted(json_files, key=extract_datetime_from_filename)
				idx = files_sorted.index(selected_file)
			except Exception:
				files_sorted = json_files[:]  # fallback
				idx = 0

			prev_name = files_sorted[idx - 1] if idx - 1 >= 0 else None

			def _count_by_level(snapshot: dict) -> dict:
				counts = {}
				for hero in snapshot.values():
					lvl = hero.get("Уровень")
					if isinstance(lvl, int):
						counts[lvl] = counts.get(lvl, 0) + 1
				return counts

			curr_counts = _count_by_level(data)
			prev_counts = _count_by_level(snapshot(prev_name)) if prev_name else {}

			for g in level_ratings:
				lvl = g["level"] if isinstance(g, dict) else getattr(g, "level", None)
				delta = curr_counts.get(lvl, 0) - prev_counts.get(lvl, 0)
				if isinstance(g, dict):
					g["count_delta"] = delta
				else:
					setattr(g, "count_delta", delta)
		else:
			if selected_level and selected_level != "Все":
				selected_level = int(selected_level)
				data = {pid: h for pid, h in data.items() if h.get("Уровень") == selected_level}

			rating = build_rating(data, selected_param, prev_snap)

		dt = extract_datetime_from_filename(selected_file)
		diff_hours = None
		if dt and prev_name:
			prev_dt = extract_datetime_from_filename(json_files[file_index + 1])
			if prev_dt:
				diff_hours = round((dt - prev_dt).total_seconds()/3600, 1)
		filename_display = dt.strftime("%d.%m.%Y %H:%M") if dt else selected_file

	elif mode == "Прирост":
		file1 = request.form.get("file1")
		file2 = request.form.get("file2")
		if (not file1 or not file2) and len(json_files) >= 2:
			file2 = json_files[0]
			file1 = json_files[1]
		elif len(json_files) == 1:
			file1 = file2 = json_files[0]

		if file1 and file2:
			data1 = snapshot(file1)
			data2 = snapshot(file2)

			if selected_level and selected_level != "Все":
				selected_level = int(selected_level)
				data2 = {pid: h for pid, h in data2.items() if h.get("Уровень") == selected_level}

			if selected_param == "Сумма статов":
				param = stat_keys
			else:
				param = selected_param

			rating = build_growth_rating(data1, data2, param)
			dt1 = extract_datetime_from_filename(file1)
			dt2 = extract_datetime_from_filename(file2)
			if dt1 and dt2:
				diff_hours = round((dt2 - dt1).total_seconds()/3600, 1)
			filename_display = f"{dt1.strftime('%d.%m.%Y %H:%M')} → {dt2.strftime('%d.%m.%Y %H:%M')}" if dt1 and dt2 else ""
	elif mode == "Лучшие (приросты)":
		lvl_key = None if (not selected_level or selected_level == "Все") else int(selected_level)
		best_list = BEST_CACHE.get((selected_param, lvl_key), [])
		best_by_param = [{"param": selected_param, "rating": best_list}]
	else:
		selected_param = request.args.get("param", param_options[0])

	base_snap = snapshot(selected_file) if selected_file else {}
	all_levels = sorted({h.get("Уровень") for h in base_snap.values() if isinstance(h.get("Уровень"), int)}, reverse=True)

	param_selectable = [
		p for p in param_options
		if not (
			mode in ("Прирост", "Лучшие (приросты)")
			and (p.startswith("Кланы") or p.startswith("Братства") or p == "По уровню")
		)
	]


	return render_template(
		"index.html",
		rating=rating,
		level_ratings=level_ratings,
		best_by_param=best_by_param,
		param=selected_param,
		mode=mode,
		filename_display=filename_display,
		json_files=json_files,
		selected_file=selected_file,
		file1=file1,
		file2=file2,
		param_options=param_options,
		column2_name=column2_name,
		column3_name=column3_name,
		param_selectable=param_selectable,
		files_display=[(f, extract_datetime_from_filename(f).strftime("%d.%m.%Y %H:%M")) for f in json_files],
		extract_datetime_from_filename=extract_datetime_from_filename,
		enumerate=enumerate,
		diff_hours=(None if mode == "Лучшие (приросты)" else diff_hours),
		all_levels=all_levels,
		selected_level=selected_level,
	)


if __name__ == "__main__":
	app.run(debug=True, use_reloader=False, threaded=True)
